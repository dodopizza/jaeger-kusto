package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	logger = hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Debug,
		Name:       "jaeger-kusto-tests",
		JSONFormat: true,
	})
)

const testConfigPath = ".././jaeger-kusto-config.json"

func TestKustoSpanReader_GetTrace(tester *testing.T) {

	testConfig := InitConfig(testConfigPath, logger)
	appCtx, appCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, appCancel)
	kustoStore := NewStore(*testConfig, logger, appCtx, &wg)
	trace, _ := model.TraceIDFromString("0232d7f26e2317b1")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fulltrace, err := kustoStore.SpanReader().GetTrace(ctx, trace)
	if err != nil {
		logger.Error("can't get trace", err.Error())
	}
	fmt.Printf("%+v\n", fulltrace)
}

func TestKustoSpanReader_GetServices(t *testing.T) {

	testConfig := InitConfig(testConfigPath, logger)
	appCtx, appCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, appCancel)
	kustoStore := NewStore(*testConfig, logger, appCtx, &wg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	services, err := kustoStore.SpanReader().GetServices(ctx)
	if err != nil {
		logger.Error("can't get services", err.Error())
	}
	fmt.Printf("%+v\n", services)
}

func TestKustoSpanReader_GetOperations(t *testing.T) {

	testConfig := InitConfig(testConfigPath, logger)
	appCtx, appCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, appCancel)
	kustoStore := NewStore(*testConfig, logger, appCtx, &wg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	operations, err := kustoStore.SpanReader().GetOperations(ctx, spanstore.OperationQueryParameters{
		ServiceName: "frontend",
		SpanKind:    "",
	})
	if err != nil {
		logger.Error("can't get operations", err.Error())
	}
	fmt.Printf("%+v\n", operations)
}

func TestFindTraces(tester *testing.T) {
	query := spanstore.TraceQueryParameters{
		ServiceName:   "frontend",
		OperationName: "",
		StartTimeMin:  time.Date(2020, time.June, 10, 13, 0, 0, 0, time.UTC),
		StartTimeMax:  time.Date(2020, time.June, 10, 14, 0, 0, 0, time.UTC),
		NumTraces:     20,
		Tags: map[string]string{
			"http_method": "GET",
		},
	}

	testConfig := InitConfig(testConfigPath, logger)
	appCtx, appCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, appCancel)
	kustoStore := NewStore(*testConfig, logger, appCtx, &wg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	traces, err := kustoStore.SpanReader().FindTraces(ctx, &query)
	if err != nil {
		logger.Error("can't find traces", err.Error())
	}
	fmt.Printf("%+v\n", traces)

}

func TestStore_DependencyReader(t *testing.T) {
	testConfig := InitConfig(testConfigPath, logger)
	appCtx, appCancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, appCancel)
	kustoStore := NewStore(*testConfig, logger, appCtx, &wg)
	dependencyLinks, err := kustoStore.DependencyReader().GetDependencies(time.Now(), 168*time.Hour)
	if err != nil {
		logger.Error("can't find dependencyLinks", err.Error())
	}
	fmt.Printf("%+v\n", dependencyLinks)
}
