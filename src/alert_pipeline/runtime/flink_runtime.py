"""Apache Flink (PyFlink) runtime adapter — same AlertProcessor as Quix.

The processor is constructed inside the operator ``open()`` method so Flink
does not need to pickle SQLAlchemy engines / weakrefs across the JVM boundary.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from alert_pipeline.config import Settings, get_settings

logger = logging.getLogger(__name__)


def _kafka_connector_jar() -> str | None:
    env_path = os.environ.get("FLINK_KAFKA_CONNECTOR_JAR")
    if env_path and Path(env_path).is_file():
        return Path(env_path).resolve().as_uri()
    lib = Path("/opt/flink/lib")
    if lib.is_dir():
        for candidate in lib.glob("flink-sql-connector-kafka*.jar"):
            return candidate.resolve().as_uri()
    return None


class FlinkStreamRuntime:
    """Kafka → PyFlink DataStream → AlertProcessor."""

    name = "flink"

    def run(self, settings: Settings) -> None:
        try:
            from pyflink.common import Configuration, SimpleStringSchema, Types, WatermarkStrategy
            from pyflink.datastream import StreamExecutionEnvironment
            from pyflink.datastream.connectors.kafka import (
                KafkaOffsetsInitializer,
                KafkaSource,
            )
            from pyflink.datastream.functions import MapFunction
        except ImportError as exc:  # pragma: no cover
            raise SystemExit(
                "PyFlink is not installed. Use Dockerfile.flink or "
                "pip install 'alert-pipeline[flink]' on Python 3.11. "
                f"Original error: {exc}"
            ) from exc

        # Capture only plain settings fields for the operator (picklable).
        settings_dump = settings.model_dump()

        class ProcessLogMap(MapFunction):
            def open(self, runtime_context):  # noqa: ANN001
                from alert_pipeline.config import Settings as S
                from alert_pipeline.processing.handler import AlertProcessor

                # Fresh settings + processor in the TaskManager Python worker
                get_settings.cache_clear()
                self._processor = AlertProcessor(S(**settings_dump), reload_yaml=True)

            def map(self, raw: str) -> str:
                try:
                    payload: Any = json.loads(raw) if raw else None
                except json.JSONDecodeError:
                    payload = {"message": raw, "level": "ERROR", "service": "unknown"}
                result = self._processor.handle_payload(payload)
                out = result.to_dict()
                return json.dumps(out) if out else ""

        parallelism = max(1, int(settings.flink_parallelism))
        conf = Configuration()
        env = StreamExecutionEnvironment.get_execution_environment(conf)
        env.set_parallelism(parallelism)

        jar_uri = _kafka_connector_jar()
        if jar_uri:
            env.add_jars(jar_uri)
            logger.info("Added Flink Kafka connector jar %s", jar_uri)
        else:
            logger.warning("No flink-sql-connector-kafka jar; set FLINK_KAFKA_CONNECTOR_JAR")

        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(settings.kafka_bootstrap_servers)
            .set_topics(settings.kafka_input_topic)
            .set_group_id(settings.kafka_consumer_group + "-flink")
            .set_starting_offsets(
                KafkaOffsetsInitializer.earliest()
                if settings.kafka_auto_offset_reset == "earliest"
                else KafkaOffsetsInitializer.latest()
            )
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )

        stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-logs")
        mapped = stream.map(ProcessLogMap(), output_type=Types.STRING()).filter(lambda s: bool(s))

        if settings.flink_print_results:
            mapped.print()
        else:
            mapped.map(lambda s: s, output_type=Types.STRING())

        logger.info(
            "Flink runtime topic=%s group=%s-flink parallelism=%s",
            settings.kafka_input_topic,
            settings.kafka_consumer_group,
            parallelism,
        )
        env.execute("alert-pipeline-flink")
