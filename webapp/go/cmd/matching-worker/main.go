package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/isucon/isucon14/webapp/go/internal/matching"
)

func main() {
	addr := os.Getenv("ISURIDE_SOLVER_LISTEN")
	host, _, err := net.SplitHostPort(addr)
	ip, parseErr := netip.ParseAddr(host)
	if err != nil || parseErr != nil || !ip.IsPrivate() {
		log.Fatal("ISURIDE_SOLVER_LISTEN must bind a specific private IP:port")
	}
	allowed, err := netip.ParseAddr(os.Getenv("ISURIDE_SOLVER_ALLOWED_IP"))
	if err != nil || !allowed.IsPrivate() {
		log.Fatal("ISURIDE_SOLVER_ALLOWED_IP must be a private application IP")
	}
	server := &http.Server{Addr: addr, Handler: matching.NewHandler(allowed),
		ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second,
		IdleTimeout: 90 * time.Second, MaxHeaderBytes: 4096}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
