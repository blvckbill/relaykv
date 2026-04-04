package main

import (
	"flag"
	"log"

	"github.com/blvckbill/redis-from-scratch/internal/server"
)

func main() {
	replicaOf := flag.String("replicaof", "", "primary address to replicate from")
	flag.Parse()

	s := server.NewServer()

	if *replicaOf != "" {
		if err := s.StartReplication(*replicaOf); err != nil {
			log.Fatalf("Could not connect to primary: %v", err)
		}
	}

	s.Start()
}
