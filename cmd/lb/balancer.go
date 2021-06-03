package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gogaeva/balancer/httptools"
	"github.com/gogaeva/balancer/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)


type Server struct {
  Addr        string
  Connections int
  Alive       bool
}

type Balancer struct {
  *sync.Mutex
  servers []*Server
}

func (lb *Balancer) GetServer() (*Server, error) {
  lb.Lock()
  defer lb.Unlock()
  var available []*Server
  for _, server := range lb.servers {
    if server.Alive {
      available = append(available, server)
    }
  }
  if len(available) == 0 {
    return nil, errors.New("no server available")
  }
  min := available[0]
  for _, next := range available {
    if next.Connections < min.Connections {
      min = next
    }
  }
  return min, nil
}

func (lb *Balancer) SetServers(serverPool []string) {
  for _, serverAddr := range serversPool {
    server := &Server{serverAddr, 0, true}
    lb.servers = append(lb.servers, server)
  }
}

func NewBalancer(serverPool []string) *Balancer {
  lb := &Balancer{new(sync.Mutex), []*Server{}}
  lb.SetServers(serverPool)
  return lb
}

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
	fwdRequest.Header.Set("lb-author", r.RemoteAddr)

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

	lb := NewBalancer(serversPool)
	
	for _, server := range lb.servers {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				availability := health(server.Addr)
				log.Println(server.Addr, availability)
				server.Alive = availability
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server, _ := lb.GetServer()
		server.Connections++
		forward(server.Addr, rw, r)
		server.Connections--
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
