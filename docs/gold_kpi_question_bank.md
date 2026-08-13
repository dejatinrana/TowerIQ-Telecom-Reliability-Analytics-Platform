# Gold KPI Question Bank

The current Gold KPI layer can answer roughly 25-35 business questions about
telecom reliability, network performance, and customer experience.

These questions are structurally supported by the current tiny dataset. They
will become more realistic after larger datasets and messy scenarios are added.

## Tower-Level Questions

Answered mainly by `tower_daily_kpis`.

- Which towers have the lowest health scores?
- Which towers have the highest network failure rate?
- Which towers have the highest dropped-call rate?
- Which towers have the highest failed-session rate?
- Which towers generated the most critical alarms?
- Which towers carried the most network events?
- Which towers carried the most data usage?
- Which towers have high traffic but low health score?
- Which tower types perform worse: urban, rural, stadium, airport, or business district?
- Do high-capacity towers perform better than lower-capacity towers?
- Which towers have poor average signal strength?
- Which towers have high average latency?
- Which towers should be prioritized for maintenance?

## Region-Level Questions

Answered mainly by `region_daily_kpis`.

- Which regions have the worst average tower health?
- Which regions have the highest dropped-call rate?
- Which regions have the highest failed-session rate?
- Which regions have the most critical alarms?
- Which regions have the highest network event volume?
- Which regions consume the most mobile data?
- Which zones are performing worse: North, South, West, or Central?
- Are some regions consistently worse across multiple days?

## Network-Type Questions

Answered mainly by `network_type_daily_kpis`.

- Is 5G performing better or worse than 4G?
- Which network type has the highest failure rate?
- Which network type has the highest latency?
- Which network type carries the most data sessions?
- Which network type carries the most data usage?
- Does LTE have weaker signal strength than 4G or 5G?
- Does one network type have more failed sessions?

## Subscriber And Plan Questions

Answered mainly by `subscriber_segment_daily_kpis`.

- Which customer segment has the highest dropped-call rate?
- Which plan type has the highest failed-session rate?
- Are premium or postpaid users receiving better reliability?
- Which subscriber segment consumes the most data?
- Are enterprise users experiencing failures?
- Which segment-plan combination has the worst experience?
- Are IoT users behaving differently from consumer users?

## Current Limitation

The current tiny dataset is clean and synthetic, so these questions are useful
for validating the Gold table structure but not yet strong enough for final
resume metrics.

After future phases add larger data and messy scenarios, the same Gold tables
should support stronger questions such as:

- Which towers were affected by a simulated regional outage?
- Did a stadium traffic burst reduce tower health?
- Did late-arriving events change previous Gold KPIs?
- Did duplicate batches affect reliability metrics before idempotency fixes?
- Did skewed tower traffic affect Spark performance?

