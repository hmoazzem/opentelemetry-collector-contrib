```sh
go install go.opentelemetry.io/collector/cmd/builder@latest

cat <<EOF > builder-config-nats.yaml
dist:
  name: otelcol-nats
  description: OTel Collector with NATS receiver and exporter
  output_path: ./bin
  otelcol_version: "0.153.0"

receivers:
- gomod: github.com/open-telemetry/opentelemetry-collector-contrib/receiver/natsreceiver v0.0.0
  path: ./receiver/natsreceiver
- gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.153.0

exporters:
- gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/natsexporter v0.0.0
  path: ./exporter/natsexporter
- gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.153.0
EOF

builder --config builder-config-nats.yaml

cat <<EOF > otelcol-nats.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
  nats:
    servers:
    - nats://localhost:4222
    auth:
      username: admin
      password: password
    traces:
      consumer_type: core
      subjects:
      - otel.traces
      encoding: otlp_proto
      workers: 4

exporters:
  nats:
    servers:
    - nats://localhost:4222
    auth:
      username: admin
      password: password
    sending_queue:
      enabled: false
    traces:
      subject: otel.traces
      encoding: otlp_proto
  debug:
    verbosity: detailed

service:
  pipelines:
    traces/ingest:
      receivers: [otlp]
      exporters: [nats]
    traces/consume:
      receivers: [nats]
      exporters: [debug]
EOF

./bin/otelcol-nats --config otelcol-nats.yaml

# test using telemetrygen
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

telemetrygen traces --otlp-insecure --rate 5 --duration 5s
```
