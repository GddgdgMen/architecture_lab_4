package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	address string
	connCnt int
	mu      sync.Mutex
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []Server{
		{address: "server1:8080"},
		{address: "server2:8080"},
		{address: "server3:8080"},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()

	// TODO: Використовуйте дані про стан сервреа, щоб підтримувати список тих серверів, яким можна відправляти ззапит.
	for i := range serversPool {
		server := serversPool[i].address
		go func() {
			for range time.Tick(10 * time.Second) {
				log.Println(server, health(server))
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		serverIndex := min(serversPool, func(a *Server, b *Server) bool {
			a.mu.Lock()
			b.mu.Lock()
			defer a.mu.Unlock()
			defer b.mu.Unlock()
			return a.connCnt < b.connCnt
		})
		chosenServer := &serversPool[serverIndex]

		chosenServer.mu.Lock()
		chosenServer.connCnt++
		chosenServer.mu.Unlock()

		forward(chosenServer.address, rw, r)

		chosenServer.mu.Lock()
		chosenServer.connCnt--
		chosenServer.mu.Unlock()
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

func min(serversPool []Server, cmpFn func(*Server, *Server) bool) int {
	if len(serversPool) == 0 {
		log.Fatal("There are no servers in server pool!")
	}

	minimumIndex := 0

	for i := 1; i < len(serversPool); i++ {
		if cmpFn(&serversPool[i], &serversPool[minimumIndex]) {
			minimumIndex = i
		}
	}

	return minimumIndex
}
