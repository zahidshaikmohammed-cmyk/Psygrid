# PSYGRID intelligence

A separate service that turns PSYGRID's market data into measured, evidenced
observations: features for every instrument, contextual anomalies,
relationship breaks, events with a searchable history, and historical
similarity. It never gives trading advice, never calls Dhan, needs no paid
service or GPU, and cannot change PSYGRID's behaviour: it runs as its own
capped process and only reads what PSYGRID already produces.

| Document | What it covers |
| --- | --- |
| [architecture.md](architecture.md) | Components, data flow, process isolation, design rules |
| [features.md](features.md) | Feature catalogue, anomaly, relationship and similarity methods |
| [event-schema.md](event-schema.md) | Event schema `event/1`, catalogue, rules, storage |
| [api.md](api.md) | `/v2` routes, authentication, rate limits, errors, the WebSocket stream |
| [replay.md](replay.md) | Replay, the no-look-ahead guarantees, the CLI |
| [deployment.md](deployment.md) | The systemd service, the deploy workflow, first-time setup, API keys |
| [configuration.md](configuration.md) | Every environment variable and its default |
| [operations.md](operations.md) | Monitoring, troubleshooting, backups and restore, recovery |
| [testing.md](testing.md) | The test suites and what each one proves |
| [security.md](security.md) | Security review: threats, controls, residual risks |
| [performance.md](performance.md) | Benchmarks at production scale and resource requirements |
| [data-rights.md](data-rights.md) | Data sources and the right to use each one |
