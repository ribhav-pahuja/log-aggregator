# Stream runtime

The only supported stream runtime is **Quix Streams**.

- **Dedup:** Quix keyed state (`group_by(fingerprint)` + stateful apply) — `dedup/quix_state.py`
- **Persist / dispatch:** `AlertProcessor.emit_alert`
- **Redis:** UI read cache only

```bash
pip install 'alert-pipeline[pipeline]'
alert-pipeline
```

## Quix repartition topics

`group_by` creates internal `repartition__*` / changelog topics via the Kafka Admin API.
The Quix app enables `auto_create_topics=True` for those; broker-level auto-create can stay off.

## DLQ

Unparseable messages go to `KAFKA_DLQ_TOPIC` when enabled.
