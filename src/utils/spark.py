"""SparkSession creation helpers."""

from __future__ import annotations

import os
import sys


def create_spark_session(app_name: str, master: str, aqe_enabled: bool, use_pyspark_package: bool = True):
    """Create a local or cluster SparkSession from configuration values."""
    if use_pyspark_package:
        os.environ.pop("SPARK_HOME", None)
        os.environ.pop("SPARK_DIST_CLASSPATH", None)
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["PYSPARK_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.adaptive.enabled", str(aqe_enabled).lower())
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
