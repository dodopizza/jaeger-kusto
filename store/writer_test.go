package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jaegertracing/jaeger/model"
)

const (
	testOperation = "testOperation"
	testService   = "testService"
)

func TestWriteSpan(tester *testing.T) {

	date, _ := time.Parse(time.RFC3339, "1990-12-02T16:50:41+00:00")
	var span = &model.Span{
		TraceID:       model.NewTraceID(555, 5),
		SpanID:        model.NewSpanID(5676767557),
		OperationName: testOperation,
		Process: &model.Process{
			ServiceName: testService,
		},
		StartTime: date,
		Duration:  34523 * time.Millisecond,
		Tags: []model.KeyValue{{
			Key:  "abc",
			VStr: "sdf",
		}},
	}
	span2 := &model.Span{
		TraceID:       model.NewTraceID(5355, 51),
		SpanID:        model.NewSpanID(3434343434),
		OperationName: testOperation,
		Process: &model.Process{
			ServiceName: testService,
		},
		StartTime: date,
		Duration:  34242 * time.Millisecond,
		Tags: []model.KeyValue{{
			Key:  "rty",
			VStr: "fgh",
		}},
	}

	span3 := &model.Span{
		TraceID:       model.NewTraceID(5555, 55),
		SpanID:        model.NewSpanID(567676755756767),
		OperationName: testOperation,
		Process: &model.Process{
			ServiceName: testService,
		},
		StartTime: date,
		Duration:  12121 * time.Millisecond,
		Tags: []model.KeyValue{{
			Key:  "qwe",
			VStr: "zxc",
		}},
	}

	testConfig := InitConfig(testConfigPath, logger)
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	defer gracefulShutdown(&wg, cancel)
	kustoStore := NewStore(*testConfig, logger, ctx, &wg)
	assert.NoError(tester, kustoStore.SpanWriter().WriteSpan(span))
	assert.NoError(tester, kustoStore.SpanWriter().WriteSpan(span2))
	assert.NoError(tester, kustoStore.SpanWriter().WriteSpan(span3))

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
