# LogForge

LogForge is a PostgreSQL-backed log ingestion, query, aggregation, and retention service.

## Setup

Requirements:

- Docker Desktop with Compose
- .NET 10 SDK for local development
- k6 for load testing

Start the complete service:

```powershell
docker compose up --build
```

The API listens on `http://localhost:8080`. PostgreSQL is exposed on port `5432` for local inspection. The application waits for PostgreSQL, applies EF Core migrations at startup, and exposes `GET /health` only after startup completes.

To stop the service:

```powershell
docker compose down
```

The `postgres_data` volume persists database data. Use `docker compose down -v` only when the database can be discarded.

## API

### `GET /health`

Returns HTTP `200` when the application is running, the database is available, and migrations have completed.

### `POST /logs`

Accepts one or more logs in a batch.

```json
{
  "logs": [
    {
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": {
        "user_id": 42,
        "region": "eu-west",
        "retries": 3
      }
    }
  ]
}
```

Each entry must have a valid ISO 8601 timestamp, a level of `debug`, `info`, `warn`, or `error`, a non-empty service, and a non-empty message. Timestamps more than five minutes in the future are rejected. Attributes must be a flat object containing only strings, numbers, or booleans.

Invalid entries do not reject valid entries in the same batch. The response identifies each rejected entry by array index:

```json
{
  "accepted": 1,
  "rejected": [
    {
      "index": 1,
      "reason": "invalid level: 'critical'"
    }
  ]
}
```

The endpoint returns `200` when at least one entry is accepted and `400` when the complete batch is rejected or the request body is invalid.

Ingestion is asynchronous: a successful response means the validated batch was handed off for publishing to RabbitMQ, not that RabbitMQ has confirmed receipt. Publishing happens fire-and-forget in the background so the HTTP response isn't held up by a broker round trip; the background consumer then persists accepted messages to PostgreSQL and the rollup table. If a publish fails after the response has already been returned, it is logged rather than surfaced to the caller. A bounded in-flight limit on background publishes protects the application from unbounded memory growth if RabbitMQ falls behind; batches beyond that limit are dropped and logged instead of queued indefinitely.

### `GET /logs`

Supported query parameters:

| Parameter | Meaning |
| --- | --- |
| `service` | Exact service match |
| `level` | Exact level match |
| `since` | Inclusive timestamp lower bound |
| `until` | Exclusive timestamp upper bound |
| `attr.<key>` | Attribute equality, compared as strings |
| `q` | Case-insensitive message substring |
| `limit` | Result count, default `100`, maximum `1000` |
| `cursor` | Opaque cursor from the previous response |

Example:

```text
GET /logs?service=checkout&level=error&since=2026-07-20T14:00:00Z&limit=100
```

Results are ordered by timestamp descending and then ID descending. Pagination uses a stable keyset cursor based on the timestamp and ID ordering pair.

Response:

```json
{
  "logs": [
    {
      "id": "123",
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": {
        "user_id": "42"
      }
    }
  ],
  "next_cursor": null
}
```

Invalid timestamps, levels, limits, ranges, or cursors return HTTP `400` with an `error` response.

### `GET /logs/aggregate`

Required parameters:

| Parameter | Meaning |
| --- | --- |
| `since` | Inclusive aggregation range start |
| `until` | Exclusive aggregation range end |
| `bucket` | `1m`, `5m`, `1h`, or `1d` |

Optional parameters are `service`, `level`, `q`, `attr.<key>`, and `group_by`. `group_by` supports `service` or `level`.

Example:

```text
GET /logs/aggregate?since=2026-07-20T14:00:00Z&until=2026-07-20T15:00:00Z&bucket=1m&group_by=service
```

Response buckets are ordered by start time ascending. Empty buckets are omitted and `group` is `null` when no grouping is requested.

## Schema And Index Design

The `log` table is range-partitioned by timestamp. The partitioning migration creates daily partitions covering 5 days in the past and 30 days in the future, plus a default partition for out-of-range timestamps.

The primary key is `("Timestamp", "Id")`. PostgreSQL requires the partition key to be included in unique constraints on a partitioned table.

The partitioned log table has these indexes:

- `("Service", "Timestamp" DESC, "Id" DESC)` for service/time queries and deterministic cursor pagination within a service
- BRIN on `"Timestamp"` for efficient range scans across partitions

The `log_minute_rollup` table has a key of `("BucketStart", "Service", "Level")` and an index on `"BucketStart"`.

Ingestion writes the raw log and updates the minute rollup in one PostgreSQL transaction. The rollup uses `date_bin('1 minute', ...)` to assign records to fixed minute boundaries. `ON CONFLICT` increments an existing rollup count instead of inserting a duplicate row.

## Attribute Storage

Attributes are stored as JSONB, not as an EAV table. During validation, supported scalar values are normalized to strings. For example, `42`, `true`, and `"eu-west"` are stored as `"42"`, `"true"`, and `"eu-west"`.

Attribute filters use JSONB containment:

```sql
"Attributes" @> jsonb_build_object(@key, @value)
```

There is currently no GIN index on `"Attributes"`, so containment filters scan and evaluate the JSONB predicate against each row within whatever partitions/indexes narrow the query first (e.g. service or timestamp range). Nested objects and arrays are rejected by the ingestion validator.

## Aggregation Strategy

Unfiltered time, service, and level aggregations use the minute rollup table to avoid scanning raw logs. The rollup is grouped into the requested bucket size by `date_bin()`.

Aggregations with message or attribute filters use the partitioned raw log table because those predicates cannot be answered by the service/level rollup.

Aggregation results are cached in memory for five seconds with a maximum of 256 entries. Cache keys include all filter and range values. Randomized benchmark ranges generally produce cache misses by design.

## Retention

Retention defaults to 30 days and runs at `02:00 UTC`. Fully expired daily log partitions are dropped instead of deleting their rows individually. Any remaining partial/default-partition data is deleted in batches.

Configuration is available in `LogForge.API/appsettings.json`:

```json
{
  "Retention": {
    "RetentionDays": 30,
    "RunAtUtc": "02:00:00",
    "DeleteBatchSize": 10000
  }
}
```

## Resource Configuration

The Compose file applies the required benchmark limits:

- Application: `0.5 CPU`, `256 MB`
- PostgreSQL: `1 CPU`, `1024 MB`
- RabbitMQ: `1 CPU`, `512 MB`

## Measured Performance

The measured report used Compose resource limits and a machine-speed factor of `0.622x` the reference machine.

| Scenario | Throughput | Error rate | P95 latency | Result |
| --- | ---: | ---: | ---: | --- |
| Load | 14,999 logs/sec | 0% | 1.52 ms | Completed |
| Stress | 20,999 logs/sec | 0% | 1.60 ms | Completed |
| Spike | 15,374 logs/sec | 0% | 1.38 ms | Completed |
| Breakpoint | 24,374 logs/sec | 0% | 3.63 ms | Completed |

All four scenarios were offered at their target rate (up to 24,375 logs/sec in the breakpoint scenario) with zero dropped iterations and zero errors. None of the scenarios were generator-limited or service-limited: throughput matched the offered rate throughout.

Benchmark score:

| Category | Score |
| --- | ---: |
| Correctness | 15 / 15 |
| Performance | 47.499 / 50 |
| Queries | 14.982 / 15 |
| Reliability | 20 / 20 |
| **Total** | **97.481 / 100** |

All 15 correctness checks passed, including ingestion, filtering, pagination, aggregation, and invalid-parameter handling. All four scenarios completed without application errors or crashes. The remaining score reduction is a small margin against the performance and query-latency scoring curves rather than a service limitation; read-after-write success is near zero by design (see Known Limitations) but is scored separately from eventual consistency, which passed in all four scenarios.

## Known Limitations

- Ingestion is eventually consistent: HTTP acceptance occurs before the message is confirmed by RabbitMQ and before the background PostgreSQL write commits.
- Publishing to RabbitMQ is fire-and-forget: a `200` response means the batch was accepted for publishing, not that RabbitMQ has acknowledged it. If the publish itself fails after the response is sent, the batch is lost and only logged, not retried or surfaced to the caller.
- Background publishes are capped by an in-flight limit to bound memory; if RabbitMQ falls behind that limit, further batches are dropped and logged rather than queued indefinitely.
- A failed batch write to PostgreSQL is retried three times and then logged; the batch is not requeued after all retries fail.
- The in-memory aggregation cache has no cross-process sharing or in-flight single-flight deduplication.
- Randomized aggregate ranges have low cache reuse.
- Partition creation currently covers a finite migration-time window; a long-running deployment should proactively create future partitions.
- The default partition can reduce partition-pruning benefits for timestamps outside the pre-created range.
- Message substring queries use `ILIKE`; no trigram index is currently enabled in the migration.
- `level`-only filters (without a `service` filter) have no dedicated index; they rely on partition pruning by timestamp and a sequential scan within each partition.
- Attribute (`attr.<key>`) containment filters have no GIN index; they are evaluated row-by-row within whatever partitions/indexes narrow the query first.

## Bottlenecks encountered

- The initial ingestion path used MassTransit over RabbitMQ with publisher confirms awaited inline on the request path. Under concurrent load this added 100-300ms of publish latency per request, which starved the background consumer of CPU on the application's constrained core budget and left PostgreSQL idle during load while a growing backlog was worked off only after load stopped. Publishing was changed to fire-and-forget so the HTTP response no longer waits on a broker round trip.
- MassTransit's batch-consumer pipeline appeared to process batches close to sequentially even with multiple configured consumers, capping consumption throughput well below what PostgreSQL could sustain. Ingestion was rebuilt on the `RabbitMQ.Client` library directly, with a hand-rolled batching consumer, which removed the framework's dispatch overhead and let consumption reach the same throughput PostgreSQL could already sustain.
- Fire-and-forget publishing removes natural backpressure from the request path: every accepted request spawns a background publish task regardless of how many are already pending. Without a limit, a sustained burst beyond RabbitMQ's ability to keep up could accumulate pending publish tasks and exhaust the application's memory limit. A bounded in-flight counter caps concurrent background publishes; batches beyond the cap are dropped and logged rather than queued unboundedly.
- The initial aggregation design used only the raw log table, which caused slow queries for unfiltered aggregations. The aggregation design was changed to use a minute rollup table for unfiltered aggregations, which significantly improved performance.
- The initial retention design used a single DELETE statement to remove expired logs, which caused long-running transactions and table bloat. The retention design was changed to drop fully expired partitions and delete remaining rows in batches, which improved performance and reduced bloat.
- The initial log table design used a single primary key on the ID column, which caused slow queries for time-range queries. The log table design was changed to use a composite primary key on (Timestamp, Id), which improved query performance and allowed for partitioning by timestamp.

## Optimizations Applied

Implemented optimization features including:

- Time-range partitioning
- Minute rollups for unfiltered aggregation
- Fire-and-forget, bounded asynchronous ingestion via a direct RabbitMQ client
- In-memory aggregate-result caching
- Partition-aware retention
