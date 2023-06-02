package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
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

func TestMin(t *testing.T) {
	servers := []Server{
		{address: "server1:8080", connCnt: 3},
		{address: "server2:8080", connCnt: 2},
		{address: "server3:8080", connCnt: 1},
	}

	index := min(servers, func(a *Server, b *Server) bool {
		return a.connCnt < b.connCnt
	})
	assert.Equal(t, 2, index, "Expected index 2")

	servers[0].connCnt = 0

	index = min(servers, func(a *Server, b *Server) bool {
		return a.connCnt < b.connCnt
	})
	assert.Equal(t, 0, index, "Expected index 0")
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
	os.Args = append(os.Args, "-trace")
	go main()
	time.Sleep(2 * time.Second)

	GetRequest := func(wg *sync.WaitGroup, expectedSrv string) {
		res, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/some-data", *port))

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, expectedSrv, res.Header.Get("lb-from"), "Expected index %s", expectedSrv)

		wg.Done()
	}

	wg := &sync.WaitGroup{}
	expectedSequence := [5]string{
		serversPool[0].address,
		serversPool[1].address,
		serversPool[2].address,
		serversPool[1].address,
		serversPool[0].address}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go GetRequest(wg, expectedSequence[i])
		time.Sleep(500 * time.Millisecond)
	}
	wg.Wait()
}
