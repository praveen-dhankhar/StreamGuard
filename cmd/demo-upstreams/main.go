package main

import (
	"log"
	"net/http"
	"sync"
	"time"

	"streamguard/internal/mockupstream"
)

func main() {
	servers := []struct {
		addr string
		opts mockupstream.Options
	}{
		{
			addr: ":9001",
			opts: mockupstream.Options{
				Provider:        "openai",
				Tokens:          []string{"Primary ", "provider ", "starts ", "well ", "then ", "fails. "},
				DelayMin:        55 * time.Millisecond,
				DelayMax:        110 * time.Millisecond,
				Failure:         mockupstream.FailureDeadSocket,
				FailAfterTokens: 4,
			},
		},
		{
			addr: ":9002",
			opts: mockupstream.Options{
				Provider: "anthropic",
				Tokens: []string{
					"Secondary ", "provider ", "replays ", "the ", "full ", "request ", "and ", "finishes ", "cleanly. ",
				},
				DelayMin: 45 * time.Millisecond,
				DelayMax: 90 * time.Millisecond,
			},
		},
	}

	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(addr string, opts mockupstream.Options) {
			defer wg.Done()
			server := &http.Server{
				Addr:    addr,
				Handler: mockupstream.NewHandler(opts),
			}
			log.Printf("demo_upstream_listen provider=%s addr=%s", opts.Provider, addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("demo_upstream_error provider=%s err=%v", opts.Provider, err)
			}
		}(srv.addr, srv.opts)
	}

	wg.Wait()
}
