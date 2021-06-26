package store

import (
	"context"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"sync"
)

type store struct {
	dependencyStoreReader dependencystore.Reader
	reader                spanstore.Reader
	writer                spanstore.Writer
}

type kustoFactory struct {
	*kusto.Client
}

func (f *kustoFactory) Reader() kustoReaderClient {
	return f.Client
}

func (f *kustoFactory) Ingest(database string) (in kustoIngest, err error) {
	in, err = ingest.New(f.Client, database, "Spans")
	return
}

// NewStore creates new Kusto store for Jaeger span storage
func NewStore(config KustoConfig, logger hclog.Logger, appCtx context.Context, shutdownWg *sync.WaitGroup) shared.StoragePlugin {

	authorizer := kusto.Authorization{
		Config: auth.NewClientCredentialsConfig(config.ClientID, config.ClientSecret, config.TenantID),
	}

	client, err := kusto.New(config.Endpoint, authorizer)
	if err != nil {
		logger.Error("Error creating Kusto client", err.Error())
		panic("cant create Kusto client")
	}
	factory := kustoFactory{client}

	reader := newKustoSpanReader(&factory, config.Database)

	writer := newKustoSpanWriter(&factory, logger, config.Database, appCtx, shutdownWg)
	store := &store{
		dependencyStoreReader: reader,
		reader:                reader,
		writer:                writer,
	}

	return store
}

func (store *store) DependencyReader() dependencystore.Reader {
	return store.dependencyStoreReader
}

func (store *store) SpanReader() spanstore.Reader {
	return store.reader
}

func (store *store) SpanWriter() spanstore.Writer {
	return store.writer
}
