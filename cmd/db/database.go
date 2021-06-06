package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/gogaeva/balancer/datastore"
	"github.com/gogaeva/balancer/httptools"
	"github.com/gogaeva/balancer/signal"
)

var dir = flag.String("dir", ".", "database directory")
var port = flag.Int("port", 18080, "database port")

func main() {
	flag.Parse()

	db, err := datastore.NewDb(*dir, datastore.DefaultSegmentSize)
	if err != nil {
		log.Fatalf("Database initialization failed: %s", err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "application/json")
		key := strings.Split(r.URL.Path, "/db/")[1]
		switch r.Method {
		case http.MethodGet:
			value, err := db.Get(key)
			if err != nil {
				switch err {
				case datastore.ErrNotFound:
					rw.WriteHeader(http.StatusNotFound)
				default:
					rw.WriteHeader(http.StatusInternalServerError)
				}
				return
			}
			rw.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(rw).Encode(struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				Key:   key,
				Value: value,
			})
		case http.MethodPost:
			val := &struct {
				Value string `json:"value"`
			}{}
			err := json.NewDecoder(r.Body).Decode(val)
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
			err = db.Put(key, val.Value)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusOK)
		default:
			rw.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
