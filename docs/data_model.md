# Data Model

TowerIQ deals with telecom network reliability data.

The data represents activity from a telecom network: customers use devices,
devices connect to towers, towers belong to regions, and calls/data sessions
generate events. Some events succeed, some fail, and some arrive late or contain
bad values.

## Type of Data

We will work with two main types of data:

- Master/reference data: relatively stable information that describes the
  business entities.
- Event/activity data: high-volume records that describe what happened on the
  network.

Examples of master/reference data:

- regions
- towers
- subscribers
- devices
- service plans

Examples of event/activity data:

- network events
- calls
- data sessions
- tower alarms

## Why Star Schema

TowerIQ uses a star-schema-inspired model because the project is analytics
focused.

In telecom analytics, we often ask questions like:

```text
How many dropped calls happened by tower, region, plan type, device type, and hour?
```

Star schema helps because it separates:

- what happened
- the business context around what happened

This makes joins, aggregations, KPI calculation, data quality checks, and Spark
performance experiments easier to reason about.

## Fact Tables

Fact tables store events or activities.

They answer:

```text
What happened?
When did it happen?
How much happened?
Did it succeed or fail?
```

Fact tables are usually large, fast-growing, and timestamp-based.

TowerIQ fact tables:

- `network_events`: general network activity, failures, handovers, signal events
- `calls`: voice call activity and dropped-call analysis
- `data_sessions`: mobile internet usage, latency, and session failures
- `tower_alarms`: tower infrastructure alarms such as power or backhaul issues

## Dimension Tables

Dimension tables describe the entities involved in the events.

They answer:

```text
Who?
Where?
What type?
Which category?
```

Dimension tables are usually smaller and slower-changing.

TowerIQ dimension tables:

- `regions`: geographical or business areas
- `towers`: cell towers and tower attributes
- `subscribers`: telecom customers
- `devices`: phones/devices used by subscribers
- `service_plans`: prepaid, postpaid, enterprise, and 5G plans

## Simple Relationship

```text
Subscriber
  uses Device
    connects to Tower
      belongs to Region
        generates Calls, Data Sessions, and Network Events

Tower
  also generates Tower Alarms
```

In simple words:

```text
Fact tables store what happened.
Dimension tables explain who, where, and what was involved.
```

## First Version Tables

The first version of the dataset will focus on these tables:

| Table | Type | Purpose |
| --- | --- | --- |
| `regions` | Dimension | Defines geographical/business areas. |
| `towers` | Dimension | Defines towers, capacity, location, and network support. |
| `service_plans` | Dimension | Defines customer plan types and service priority. |
| `subscribers` | Dimension | Defines customers and their home region/plan. |
| `devices` | Dimension | Defines subscriber devices and 5G support. |
| `network_events` | Fact | Captures general network activity and failures. |
| `calls` | Fact | Captures voice calls and dropped-call behavior. |
| `data_sessions` | Fact | Captures mobile data usage, latency, and failures. |
| `tower_alarms` | Fact | Captures infrastructure problems reported by towers. |

## Tiny Dataset Location

The first generated dataset is stored locally under:

```text
data/raw/tiny/
```

Raw source data is stored as CSV files. Later pipeline layers will write Parquet:

```text
data/bronze/tiny/
data/silver/tiny/
data/gold/tiny/
data/quarantine/tiny/
```

## First Version Columns

### `regions`

Purpose: geographical/business areas.

```text
region_id
region_name
country
state
city
zone
region_type
timezone
is_active
```

### `towers`

Purpose: cell towers that handle calls, sessions, and network events.

```text
tower_id
region_id
tower_name
city
latitude
longitude
tower_type
supported_networks
capacity_score
activation_date
tower_status
```

### `service_plans`

Purpose: subscriber plans and service priority.

```text
plan_id
plan_name
plan_type
monthly_data_limit_gb
voice_limit_minutes
priority_level
is_5g_enabled
monthly_price
```

### `subscribers`

Purpose: telecom customers.

```text
subscriber_id
home_region_id
plan_id
activation_date
subscriber_status
customer_segment
age_band
```

### `devices`

Purpose: subscriber phones/devices.

```text
device_id
subscriber_id
manufacturer
model
os_type
supports_5g
release_year
device_status
```

### `network_events`

Purpose: general network activity such as connection attempts, handovers,
signal samples, failures, and retries.

```text
event_id
subscriber_id
device_id
tower_id
event_timestamp
ingestion_timestamp
event_type
network_type
signal_strength_dbm
latency_ms
status
error_code
batch_id
source_file
schema_version
```

### `calls`

Purpose: voice call behavior and dropped-call analysis.

```text
call_id
subscriber_id
device_id
tower_id
call_start_timestamp
call_end_timestamp
duration_seconds
call_type
network_type
call_status
drop_reason
ingestion_timestamp
batch_id
source_file
schema_version
```

### `data_sessions`

Purpose: mobile internet usage, latency, and failed-session analysis.

```text
session_id
subscriber_id
device_id
tower_id
session_start_timestamp
session_end_timestamp
duration_seconds
network_type
bytes_uploaded
bytes_downloaded
latency_ms
session_status
failure_reason
ingestion_timestamp
batch_id
source_file
schema_version
```

### `tower_alarms`

Purpose: infrastructure alarms reported by towers.

```text
alarm_id
tower_id
alarm_timestamp
alarm_type
severity
alarm_status
resolved_timestamp
description
ingestion_timestamp
batch_id
source_file
schema_version
```

## Initial Tiny Dataset Size

| Table | Rows |
| --- | ---: |
| `regions` | 8 |
| `towers` | 80 |
| `service_plans` | 6 |
| `subscribers` | 5,000 |
| `devices` | 5,500 |
| `network_events` | 50,000 |
| `calls` | 20,000 |
| `data_sessions` | 30,000 |
| `tower_alarms` | 800 |
