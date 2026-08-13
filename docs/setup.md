# Local Setup

TowerIQ is local-first, but it should avoid accidental dependency and Spark
runtime mismatches.

## Create The Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Verify The Environment

```bash
python -c "import pyspark; print(pyspark.__version__)"
python -m pytest tests/unit
```

Expected PySpark version:

```text
3.5.x
```

## Run Bronze Ingestion

```bash
source .venv/bin/activate
python -m src.jobs.run_bronze_ingestion --config configs/local.yaml --profile tiny
```

## Why This Matters

The local machine may have a global Spark installation configured through
environment variables such as:

```text
SPARK_HOME
SPARK_DIST_CLASSPATH
PYSPARK_PYTHON
PYSPARK_DRIVER_PYTHON
```

During Bronze ingestion, TowerIQ detected a mismatch where the Python PySpark
package was version `3.5.5`, but the JVM Spark runtime came from global
`SPARK_HOME` and reported Spark `4.1.1`.

For local development, `configs/local.yaml` uses:

```yaml
spark:
  use_pyspark_package: true
```

This makes the TowerIQ Spark helper ignore global `SPARK_HOME` and use the
PySpark runtime installed in the virtual environment.

