package main

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/dodopizza/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, cancel)
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Name:       "jaeger-kusto",
		JSONFormat: true,
	})

	var configPath string

	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	kustoConfig := store.InitConfig(configPath, logger)

	kustoStore := store.NewStore(*kustoConfig, logger, ctx, &wg)
	grpc.Serve(kustoStore)
}

func gracefulShutdown(wg *sync.WaitGroup, cancel context.CancelFunc) {
	cancel()
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()
	select {
	case <-done:
		return
	case <-time.After(time.Second):
		return
	}
}
