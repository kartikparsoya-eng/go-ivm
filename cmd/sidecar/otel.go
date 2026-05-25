package main

// OTLP/HTTP trace exporter (REVIEW-final MED-CROSS-4). One server span per
// RPC handler, parented via the TS-forwarded `traceparent`. Tracing is a
// no-op when no OTEL_EXPORTER_OTLP_ENDPOINT is set so callers can call
// startHandlerSpan unconditionally without paying for spans nobody collects.

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/kartikparsoya-eng/go-ivm/sidecar"

// tracer is reassigned by otelInit once a real provider is registered;
// before that it's the noop tracer so handlers can start spans regardless.
var tracer trace.Tracer = otel.GetTracerProvider().Tracer(tracerName)

var w3cPropagator = propagation.TraceContext{}

// otelInit registers an OTLP/HTTP trace exporter from the standard
// OTEL_EXPORTER_OTLP_* env vars and returns a flush-and-shutdown func. When
// no endpoint is configured, returns a no-op shutdown and leaves the global
// provider at its noop default. Exporter-construction errors return a no-op
// shutdown plus the err — callers should log and continue rather than
// taking the sidecar down for a broken telemetry pipe.
func otelInit(ctx context.Context) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		fmt.Fprintln(os.Stderr, "[GO-IVM] OTel disabled (no OTEL_EXPORTER_OTLP_ENDPOINT)")
		return noop, nil
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "go-ivm-sidecar"
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return noop, fmt.Errorf("otel: build resource: %w", err)
	}

	// otlptracehttp picks endpoint/headers/insecure straight from env. Passing
	// options here would override env, which is confusing for ops.
	exporter, err := otlptrace.New(ctx, otlptracehttp.NewClient())
	if err != nil {
		return noop, fmt.Errorf("otel: build exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(w3cPropagator)
	tracer = tp.Tracer(tracerName)

	fmt.Fprintf(os.Stderr, "[GO-IVM] OTel enabled service=%s endpoint=%s\n",
		serviceName, endpoint)

	return func(ctx context.Context) error {
		// 5s drain — long enough to flush pending batches, short enough that
		// a stuck collector doesn't block sidecar shutdown.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("otel: shutdown: %w", err)
		}
		return nil
	}, nil
}

func extractTraceparent(parent context.Context, tp string) context.Context {
	if tp == "" {
		return parent
	}
	return w3cPropagator.Extract(parent, propagation.MapCarrier{"traceparent": tp})
}

// Returns the span context and a closer the caller must invoke (success or
// error) before sending the RPC response.
func startHandlerSpan(ctx context.Context, method string) (context.Context, func(err *RPCError)) {
	ctx, span := tracer.Start(ctx, "go-ivm."+method,
		trace.WithSpanKind(trace.SpanKindServer),
	)
	return ctx, func(rpcErr *RPCError) {
		if rpcErr != nil {
			span.RecordError(fmt.Errorf("rpc error %d: %s", rpcErr.Code, rpcErr.Message))
			span.SetStatus(codes.Error, rpcErr.Message)
		}
		span.End()
	}
}
