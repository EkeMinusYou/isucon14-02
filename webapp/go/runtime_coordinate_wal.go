package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

func (s *coordinateStripe) write(record coordinateRecord, timing *coordinateTiming) error {
	var started time.Time
	if timing != nil {
		started = time.Now()
	}
	payload, err := json.Marshal(record)
	if timing != nil {
		timing.elapsed[2], timing.visited[2], timing.ok[2] = time.Since(started), true, err == nil && len(payload) <= maxWALRecordBytes
	}
	if err != nil {
		return err
	}
	if len(payload) > maxWALRecordBytes {
		return errors.New("coordinate WAL record too large")
	}
	if timing != nil {
		started = time.Now()
	}
	s.frame = append(s.frame[:0], make([]byte, 8)...)
	s.frame = append(s.frame, payload...)
	binary.LittleEndian.PutUint32(s.frame, uint32(len(payload)))
	binary.LittleEndian.PutUint32(s.frame[4:], crc32.ChecksumIEEE(payload))
	if timing != nil {
		timing.elapsed[3], timing.visited[3], timing.ok[3] = time.Since(started), true, true
		started = time.Now()
	}
	n, err := s.wal.Write(s.frame)
	if err == nil && n != len(s.frame) {
		err = io.ErrShortWrite
	}
	if timing != nil {
		timing.elapsed[4], timing.visited[4], timing.ok[4], timing.bytes = time.Since(started), true, err == nil, n
	}
	return err
}

func readCoordinateRecord(f *os.File) (coordinateRecord, error) {
	var record coordinateRecord
	var header [8]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return record, err
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size == 0 || size > maxWALRecordBytes {
		return record, errors.New("invalid coordinate WAL frame size")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(f, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return record, err
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(header[4:]) {
		return record, errors.New("coordinate WAL checksum mismatch")
	}
	if err := json.Unmarshal(payload, &record); err != nil {
		return record, err
	}
	if record.Version != 1 {
		return record, fmt.Errorf("unsupported coordinate WAL version %d", record.Version)
	}
	return record, nil
}
