package integration

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

type Msg struct {
	Key   string
	Value string
}

type response struct {
	server string
	value  string
}

func Test(t *testing.T) {
	time.Sleep(10 * time.Second)
	TestingT(t)
}

type IntegrationSuite struct{}

var _ = Suite(&IntegrationSuite{})

func (s *IntegrationSuite) TestBalancer(c *C) {
	N := 3
	responses := make(chan response, N)
	for i := 0; i < N; i++ {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=bluemars", baseAddress))
			if err != nil {
				c.Error(err)
			}
			var val Msg
			err = json.NewDecoder(resp.Body).Decode(&val)
			if err != nil {
				log.Printf("%s", err)
			}
			respServer := resp.Header.Get("lb-from")
			responses <- response{respServer, val.Value}
			c.Logf("response from [%s]", resp.Header.Get("lb-from"))
		}()
	}
	prev := ""
	for i := 0; i < N; i++ {
		next := <-responses
		if prev != "" {
			c.Check(prev, Not(Equals), next.server)
		} else {
			prev = next.server
		}
		c.Check(next.value, Equals, time.Now().Format("January 1, 2001"))
	}
}

func (s *IntegrationSuite) BenchmarkBalancer(c *C) {
	for i := 0; i < c.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		c.Check(err, IsNil)
		c.Check(resp.StatusCode, Equals, http.StatusOK)
	}
}
