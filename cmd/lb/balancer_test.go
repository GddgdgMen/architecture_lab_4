package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	isHealthy := health(server.Listener.Addr().String())
	assert.True(t, isHealthy, "Expected the server to be healthy")

	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	isHealthy = health(server.Listener.Addr().String())
	assert.False(t, isHealthy, "Expected the server to be unhealthy")
}

func TestForward(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	req, err := http.NewRequest("GET", "/test", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	recorder := httptest.NewRecorder()

	err = forward(server.Listener.Addr().String(), recorder, req)
	if err != nil {
		t.Fatalf("Failed to forward request: %v", err)
	}

	assert.Equal(t, http.StatusOK, recorder.Code, "Expected status code 200")

	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	recorder = httptest.NewRecorder()

	err = forward(server.Listener.Addr().String(), recorder, req)
	if err != nil {
		t.Fatalf("Failed to forward request: %v", err)
	}

	assert.Equal(t, http.StatusInternalServerError, recorder.Code, "Expected status code 503")
}

func TestBalancer(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1750 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(750 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server3.Close()

	serversPool = []Server{
		{address: server1.Listener.Addr().String()},
		{address: server2.Listener.Addr().String()},
		{address: server3.Listener.Addr().String()},
	}

	*traceEnabled = true

	go main()

	for {
		time.Sleep(500 * time.Millisecond)
		resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/test", *port))
		if resp != nil {
			break
		}
	}

	GetRequest := func(wg *sync.WaitGroup, expectedSrv string) {
		res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/some-data", *port))

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedSrv, res.Header.Get("lb-from"), "Expected index %s", expectedSrv)

		wg.Done()
	}

	wg := &sync.WaitGroup{}
	expectedSequence := []string{
		serversPool[0].address,
		serversPool[1].address,
		serversPool[2].address,
		serversPool[1].address,
		serversPool[0].address}

	for i := 0; i < len(expectedSequence); i++ {
		wg.Add(1)
		go GetRequest(wg, expectedSequence[i])
		time.Sleep(500 * time.Millisecond)
	}
	wg.Wait()
}

func BenchmarkBalancer(b *testing.B) {
	serversPool = []Server{}
	for i := 0; i < 3000; i++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		serversPool = append(serversPool, Server{address: server.Listener.Addr().String()})
	}

	time.Sleep(5000 * time.Millisecond)

	*port = 0

	go main()

	for {
		time.Sleep(500 * time.Millisecond)
		resp, _ := http.Get(fmt.Sprintf("http://127.0.0.1:%d/test", *port))
		if resp != nil {
			break
		}
	}

	GetRequest := func(wg *sync.WaitGroup) {
		_, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/some-data", *port))

		if err != nil {
			b.Fatal(err)
		}

		wg.Done()
	}

	wg := &sync.WaitGroup{}

	b.Run("parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			go GetRequest(wg)
		}
		wg.Wait()
	})

	average := float64(ComplexityCount / IterationsCount)
	log.Printf("Complexity: O(%f)\n", average)

	ComplexityCount = 0
	IterationsCount = 0

	b.Run("sync", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			wg.Add(1)
			GetRequest(wg)
		}
	})

	average = float64(ComplexityCount) / float64(IterationsCount)
	log.Printf("Complexity: O(%f)\n", average)

}
