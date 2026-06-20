"""Shared SparkSession for the transform unit tests. Skips the whole module cleanly when
PySpark or a JVM (Java 17) isn't available, so the rest of the suite still runs."""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("transform-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 — no JVM/Java on this machine
        pytest.skip(f"Spark could not start (is Java 17 on PATH?): {exc}")

    yield session
    session.stop()
