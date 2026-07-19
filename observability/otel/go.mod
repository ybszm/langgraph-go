module github.com/wahanbo/langgraph-go/observability/otel

go 1.25.0

require (
	github.com/wahanbo/langgraph-go v0.0.0-20260719141329-15b5bb400872
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

replace github.com/wahanbo/langgraph-go => ../..
