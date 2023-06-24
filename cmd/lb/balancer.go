package main

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")

	ComplexityCount = 0
	IterationsCount = 0
)

type Server struct {
	address        string
	connCnt        int
	isServerOnline bool
}

type ServerHeap []*Server

func (h ServerHeap) Len() int {
	ComplexityCount++
	return len(h)
}

func (h ServerHeap) Less(i, j int) bool {
	return h[i].connCnt < h[j].connCnt
}

func (h ServerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ServerHeap) Push(x interface{}) {
	*h = append(*h, x.(*Server))
}

func (h *ServerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []*Server{
		{address: "server1:8080"},
		{address: "server2:8080"},
		{address: "server3:8080"},
	}
	onlineServers = make(ServerHeap, 0)
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
		//log.Println("fwd", resp.StatusCode, resp.Request.URL)
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

	healthCheckUp := func(server *Server) {
		isServerOnline := health(server.address)
		if isServerOnline && !server.isServerOnline {
			heap.Push(&onlineServers, server)
		} else if !isServerOnline && server.isServerOnline {
			serverIndex := -1
			for i, s := range onlineServers {
				if s == server {
					serverIndex = i
					break
				}
			}
			if serverIndex != -1 {
				heap.Remove(&onlineServers, serverIndex)
			}
		}

		log.Println(server.address, isServerOnline)
	}

	for i := range serversPool {
		server := serversPool[i]
		healthCheckUp(server)
	}

	go func() {
		for i := range serversPool {
			time.Sleep(100 * time.Millisecond)
			server := serversPool[i]
			go func() {
				for range time.Tick(10 * time.Second) {
					healthCheckUp(server)
				}
			}()
		}
	}()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if len(onlineServers) == 0 {
			rw.WriteHeader(http.StatusBadGateway)
			return
		}

		heap.Fix(&onlineServers, 0)
		IterationsCount++
		server := onlineServers[0]
		server.connCnt++

		forward(server.address, rw, r)

		server.connCnt--
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	port = frontend.GetPort()
	signal.WaitForTerminationSignal()
}
