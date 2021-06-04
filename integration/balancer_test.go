package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func Test(t *testing.T) {
	time.Sleep(10 * time.Second)
	TestingT(t)
}

type IntegrationSuite struct{}

var _ = Suite(&IntegrationSuite{})

func (s *IntegrationSuite) TestBalancer(c *C) {
	N := 3
	servers := make(chan string, N)
	for i := 0; i < N; i++ {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
			if err != nil {
				c.Error(err)
			}
			servers <- resp.Header.Get("lb-from")
			c.Logf("response from [%s]", resp.Header.Get("lb-from"))
		}()
	}
	prev := ""
	for i := 0; i < N; i++ {
		next := <-servers
		if prev != "" {
			c.Check(prev, Not(Equals), next)
		} else {
			prev = next
		}
	}
}

func (s *IntegrationSuite) BenchmarkBalancer(c *C) {
	for i := 0; i < c.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		c.Check(err, IsNil)
		c.Check(resp.StatusCode, Equals, http.StatusOK)
	}
}
