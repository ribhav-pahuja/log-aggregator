# Stream runtime

The only supported stream runtime is **Quix Streams**.

- **Dedup:** Quix keyed state (`group_by(fingerprint)` + stateful apply) — `dedup/quix_state.py`
- **Persist / dispatch:** `AlertProcessor.emit_alert`
- **Redis:** UI read cache only
- **Ingress:** Kafka `logs` topic is the pipeline input. Optional bridges under
  `sources/` (Grafana Loki poll, Grafana Alerting webhook) publish *into* that topic.

```bash
pip install 'alert-pipeline[pipeline]'
alert-pipeline

# Optional: Grafana → Kafka bridge
export GRAFANA_LOKI_URL=http://loki:3100
grafana-source
```

## Quix repartition topics

`group_by` creates internal `repartition__*` / changelog topics via the Kafka Admin API.
The Quix app enables `auto_create_topics=True` for those; broker-level auto-create can stay off.

## DLQ

Unparseable messages go to `KAFKA_DLQ_TOPIC` when enabled.
