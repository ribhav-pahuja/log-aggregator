# Stream runtimes

Business logic lives in `alert_pipeline.processing.AlertProcessor`.
Runtimes only move bytes from Kafka into that processor.

| Runtime | Module | Env |
| --- | --- | --- |
| Quix Streams (default) | `quix_runtime.py` | `PIPELINE_RUNTIME=quix` |
| Apache Flink (PyFlink) | `flink_runtime.py` | `PIPELINE_RUNTIME=flink` |

```bash
alert-pipeline

pip install 'alert-pipeline[flink]'
PIPELINE_RUNTIME=flink FLINK_PARALLELISM=1 alert-pipeline
```

## Adding another engine

1. Class with `name` and `run(settings)` (`StreamRuntime` protocol).
2. `AlertProcessor(settings)` once per task/worker.
3. Each message → `processor.handle_payload(raw)`.
4. Register in `factory._RUNTIMES`.
