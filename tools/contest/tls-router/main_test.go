package main

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

type exhaustedListener struct {
	net.Listener
	err     error
	calls   int
	retried chan struct{}
	release chan struct{}
}

func (l *exhaustedListener) Accept() (net.Conn, error) {
	l.calls++
	if l.calls == 2 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: l.err}
	}
	if l.calls == 3 {
		close(l.retried)
		<-l.release
	}
	return l.Listener.Accept()
}

func TestServeRetriesFileDescriptorExhaustion(t *testing.T) {
	for _, resourceError := range []error{syscall.EMFILE, syscall.ENFILE} {
		t.Run(resourceError.Error(), func(t *testing.T) {
			upstream, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer upstream.Close()
			go func() {
				for {
					conn, err := upstream.Accept()
					if err != nil {
						return
					}
					go func() { defer conn.Close(); io.Copy(conn, conn) }()
				}
			}()
			frontend, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			listener := &exhaustedListener{Listener: frontend, err: resourceError,
				retried: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- serve(listener, []string{upstream.Addr().String()}) }()
			defer frontend.Close()
			defer func() {
				select {
				case <-listener.release:
				default:
					close(listener.release)
				}
			}()
			first, err := net.Dial("tcp", frontend.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			select {
			case err := <-done:
				t.Fatalf("resource pressure terminated the router: %v", err)
			case <-listener.retried:
			case <-time.After(3 * time.Second):
				t.Fatal("accept was not retried")
			}
			checkEcho := func(conn net.Conn) {
				t.Helper()
				conn.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := conn.Write([]byte("live stream")); err != nil {
					t.Fatal(err)
				}
				response := make([]byte, len("live stream"))
				if _, err := io.ReadFull(conn, response); err != nil || string(response) != "live stream" {
					t.Fatalf("existing stream: %q, %v", response, err)
				}
			}
			checkEcho(first)
			close(listener.release)
			second, err := net.Dial("tcp", frontend.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			checkEcho(second)
			checkEcho(first)
			frontend.Close()
			select {
			case err := <-done:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("listener close: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("router did not stop after listener close")
			}
		})
	}
}

func TestRelayPreservesResponseAfterClientHalfClose(t *testing.T) {
	frontend, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()
	upstream, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	done := make(chan struct{})
	go func() {
		client, err := frontend.AcceptTCP()
		if err == nil {
			relay(client, upstream.Addr().String())
		}
		close(done)
	}()
	client, err := net.DialTCP("tcp", nil, frontend.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	client.Write([]byte("complete request"))
	client.CloseWrite()
	upstream.SetDeadline(time.Now().Add(3 * time.Second))
	backend, err := upstream.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backend.SetDeadline(time.Now().Add(3 * time.Second))
	request, err := io.ReadAll(backend)
	if err != nil || string(request) != "complete request" {
		t.Fatalf("request after FIN: %q, %v", request, err)
	}
	if _, err := backend.Write([]byte("complete response")); err != nil {
		t.Fatal(err)
	}
	backend.CloseWrite()
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "complete response" {
		t.Fatalf("response after client FIN: %q, %v", response, err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("relay retained a completed connection")
	}
}
