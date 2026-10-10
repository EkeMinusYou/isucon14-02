package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const maxWALRecordBytes = 1 << 20

// Records contain immutable after-images. History rows are never coalesced.
type stateRecord struct {
	Version     int              `json:"v"`
	Generation  string           `json:"g"`
	Sequence    uint64           `json:"s"`
	Location    *ChairLocation   `json:"l,omitempty"`
	Distance    *distanceTotal   `json:"d,omitempty"`
	Rides       []Ride           `json:"r,omitempty"`
	CreatedRide *createdRide     `json:"n,omitempty"`
	Statuses    []RideStatus     `json:"t,omitempty"`
	Chair       *Chair           `json:"c,omitempty"`
	Stats       *chairStatsTotal `json:"e,omitempty"`
	Ack         *notificationAck `json:"a,omitempty"`
}

type createdRide struct {
	Ride Ride
	Fare int
}

type notificationAck struct {
	StatusID string
	Kind     string
	At       time.Time
}

func openStateWAL(directory string) (*os.File, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(directory, "state.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	// A second process must not mutate the same owner-local state.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("state WAL already owned: %w", err)
	}
	return f, nil
}

func writeWALRecord(f *os.File, record stateRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(payload) > maxWALRecordBytes {
		return errors.New("state WAL record too large")
	}
	frame := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(frame, uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	n, err := f.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	return err
}

// Only an incomplete final write can be discarded. Corruption fails startup.
func readWALRecord(f *os.File) (stateRecord, error) {
	var record stateRecord
	var header [8]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return record, err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size == 0 || size > maxWALRecordBytes {
		return record, errors.New("invalid state WAL frame size")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(f, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return record, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(header[4:]) {
		return record, errors.New("state WAL checksum mismatch")
	}
	if err := json.Unmarshal(payload, &record); err != nil {
		return record, err
	}
	if record.Version != 1 {
		return record, fmt.Errorf("unsupported state WAL version %d", record.Version)
	}
	return record, nil
}

func (s *rideState) recoverWAL() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header, err := readWALRecord(s.wal)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (err == nil && header.Generation != s.generation) {
		if s.applied != 0 {
			return errors.New("missing WAL for an existing DB projection")
		}
		if err := s.wal.Truncate(0); err != nil {
			return err
		}
		if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := writeWALRecord(s.wal, stateRecord{Version: 1, Generation: s.generation}); err != nil {
			return err
		}
		return s.wal.Sync()
	}
	if err != nil {
		return err
	}
	if header.Sequence != 0 {
		return errors.New("invalid state WAL generation header")
	}
	for {
		offset, err := s.wal.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		record, err := readWALRecord(s.wal)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if err := s.wal.Truncate(offset); err != nil {
				return err
			}
			if _, err := s.wal.Seek(offset, io.SeekStart); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return fmt.Errorf("read state WAL at %d: %w", offset, err)
		}
		if record.Generation != s.generation || record.Sequence != s.sequence+1 {
			return errors.New("state WAL generation or sequence mismatch")
		}
		s.sequence = record.Sequence
		if record.Sequence > s.applied {
			s.apply(record)
			s.pending = append(s.pending, record)
		}
	}
	if s.sequence < s.applied {
		return errors.New("state WAL ends before DB projection")
	}
	return nil
}

// Called with s.mu held; no memory state is published before a complete write.
func (s *rideState) commit(record stateRecord) error {
	if !s.ready || s.failed != nil {
		return errors.New("state WAL is unavailable")
	}
	if len(s.pending) >= 32768 {
		return errors.New("state DB projection backlog is full")
	}
	record.Version, record.Generation, record.Sequence = 1, s.generation, s.sequence+1
	if err := writeWALRecord(s.wal, record); err != nil {
		s.failed = err
		return err
	}
	s.sequence = record.Sequence
	s.apply(record)
	s.pending = append(s.pending, record)
	return nil
}
