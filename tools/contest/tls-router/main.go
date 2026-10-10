// tls-router balances encrypted TCP connections without interpreting TLS or HTTP.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", "", "private entry address:443")
	backends := flag.String("backends", "", "comma-separated TLS endpoint addresses")
	flag.Parse()
	if err := run(*listen, strings.Split(*backends, ",")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(address string, backends []string) error {
	if _, _, err := net.SplitHostPort(address); err != nil {
		return err
	}
	for _, backend := range backends {
		if _, _, err := net.SplitHostPort(backend); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return serve(listener, backends)
}

func serve(listener net.Listener, backends []string) error {
	defer listener.Close()
	next := 0
	var delay time.Duration
	for {
		client, err := listener.Accept()
		if err != nil {
			// Resource pressure must not terminate all established TLS/SSE streams.
			temporary, ok := err.(net.Error)
			if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || (ok && temporary.Temporary()) {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if delay > time.Second {
					delay = time.Second
				}
				time.Sleep(delay)
				continue
			}
			return err
		}
		delay = 0
		// Each accepted connection keeps one endpoint for its entire lifetime.
		backend := backends[next]
		next = (next + 1) % len(backends)
		go relay(client.(*net.TCPConn), backend)
	}
}

func relay(client *net.TCPConn, address string) {
	defer client.Close()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	upstream, err := dialer.Dial("tcp", address)
	if err != nil {
		return
	}
	defer upstream.Close()
	backend := upstream.(*net.TCPConn)
	done := make(chan struct{})
	go func() {
		_, err := io.Copy(backend, client)
		if err != nil {
			// Abort both directions on reset; preserve responses after a clean FIN.
			backend.Close()
			client.Close()
		} else {
			backend.CloseWrite()
		}
		close(done)
	}()
	_, err = io.Copy(client, backend)
	if err == nil {
		client.CloseWrite()
	} else {
		client.Close()
	}
	// A closed upstream cannot consume further request bytes.
	backend.Close()
	client.CloseRead()
	<-done
}
