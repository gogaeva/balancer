package main

import (
  "errors"
  "sync"
  "testing"

  . "gopkg.in/check.v1"
)

type testCase struct {
  lb     Balancer
  server *Server
  err    error
}

func Test(t *testing.T) {
  TestingT(t)
}

type BalancerSuite struct{}

var _ = Suite(&BalancerSuite{})

func (s *BalancerSuite) TestBalancer(c *C) {
  testCases := []testCase{
    {
      lb: Balancer{
        Mutex: new(sync.Mutex),
        servers: []*Server{
          {Addr: "server:8000", Connections: 2, Alive: false},
          {Addr: "server:8001", Connections: 6, Alive: true},
          {Addr: "server:8002", Connections: 5, Alive: true},
        },
      },
      server: &Server{Addr: "server:8002", Connections: 5, Alive: true},
      err:    nil,
    },
    {
      lb: Balancer{
        Mutex: new(sync.Mutex),
        servers: []*Server{
          {Addr: "server:8000", Connections: 0, Alive: false},
          {Addr: "server:8001", Connections: 2, Alive: true},
          {Addr: "server:8002", Connections: 2, Alive: true},
        },
      },
      server: &Server{Addr: "server:8001", Connections: 2, Alive: true},
      err:    nil,
    },
    {
      lb: Balancer{
        Mutex: new(sync.Mutex),
        servers: []*Server{
          {Addr: "server:8000", Connections: 0, Alive: true},
          {Addr: "server:8001", Connections: 0, Alive: true},
          {Addr: "server:8002", Connections: 0, Alive: true},
        },
      },
      server: &Server{Addr: "server:8000", Connections: 0, Alive: true},
      err:    nil,
    },
    {
      lb: Balancer{
        Mutex: new(sync.Mutex),
        servers: []*Server{
          {Addr: "server:8000", Connections: 2, Alive: false},
          {Addr: "server:8001", Connections: 6, Alive: false},
          {Addr: "server:8002", Connections: 5, Alive: false},
        },
      },
      server: nil,
      err:    errors.New("no server available"),
    },
    {
      lb: Balancer{
        Mutex: new(sync.Mutex),
        servers: []*Server{
          {Addr: "server:8000", Connections: 5, Alive: true},
          {Addr: "server:8001", Connections: 6, Alive: true},
          {Addr: "server:8002", Connections: 2, Alive: true},
        },
      },
      server: &Server{Addr: "server:8002", Connections: 2, Alive: true},
      err:    nil,
    },
  }

  for _, testCase := range testCases {
    server, err := testCase.lb.GetServer()
    c.Check(server, DeepEquals, testCase.server)
    if testCase.err == nil {
      c.Check(err, IsNil)
    } else {
      c.Check(err, ErrorMatches, testCase.err.Error())
    }
  }
}