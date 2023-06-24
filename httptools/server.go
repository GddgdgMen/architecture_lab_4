package httptools

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type Server interface {
	Start()
	GetPort() *int
}

type server struct {
	httpServer *http.Server
	port       *int
}

func (s server) Start() {
	go func() {
		log.Println("Staring the HTTP server...")
		addr := s.httpServer.Addr
		if addr == "" {
			addr = ":http"
		}
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			address := ln.Addr().(*net.TCPAddr)
			*s.port = address.Port
			err = s.httpServer.Serve(ln)
		}
		log.Fatalf("HTTP server finished: %s. Finishing the process.", err)
	}()
}

func (s server) GetPort() *int {
	return s.port
}

func CreateServer(port int, handler http.Handler) Server {
	return server{
		httpServer: &http.Server{
			Addr:           fmt.Sprintf(":%d", port),
			Handler:        handler,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
		port: new(int),
	}
}
