# Event-driven operational metrics

These Prometheus metrics observe existing lifecycle operations without querying
historical task/event tables. Enable the existing Prometheus endpoint and scrape
each Engine replica independently. They follow the existing tenant metrics gate;
the default self-hosted gate does not query the database.

| Metric | Hook | Meaning |
| --- | --- | --- |
| `hatchet_task_initial_assignment_delay_seconds` | Scheduler receives committed assignment results | Task creation to assignment handling on retry zero. Covers normal, optimistic, and committed batch assignments. Labels: tenant, queue. |
| `hatchet_task_dispatch_attempts_total` | Single-task dispatcher send outcome | Labels: tenant, queue, worker, retry bucket `0/1/2/3/4+`, outcome `sent/failed`. One observation per attempted task dispatch, across local optimistic and queued dispatch. |
| `hatchet_task_worker_completions_total` | Completion transaction returns released runtimes | Current-attempt completions with an actual worker, excluding DAG orchestrators. Labels: tenant, queue, worker. |

Creation/success/final-failure counts remain available as the existing
`hatchet_created_tasks_total`, `hatchet_succeeded_tasks_total`, and
`hatchet_failed_tasks_total` counters. Dispatch failure reasons remain available
as `hatchet_dispatch_to_worker_failures_total`.

## Semantics

- These are operational observations, not an exactly-once audit ledger. A process
  crash between commit and observation or before a scrape can lose an increment.
  Prometheus counter reset handling cannot recover such increments.
- The completion hook runs after commit. A completion redelivery whose runtime
  was already deleted has no worker and is skipped; stale retry completions are
  skipped. Early slot release removes slot rows while preserving runtime identity,
  so a later completion is still counted.
- Dispatch counts represent attempts, including redelivery. `sent` means the
  existing send path returned success, **not** a worker acknowledgement. Existing
  deadline/uncertain-delivery behavior is unchanged. The separate `START_BATCH`
  RPC does not feed this counter; ordinary bulk dispatch's per-task sends do.
- Assignment duration starts at task `inserted_at`, not the historical `QUEUED`
  event timestamp, and can include concurrency/dependency wait. Use an explicit
  creation-to-assignment panel title. Retries are excluded; durable reinvocations
  with retry zero can produce additional observations.
- Rate windows use observation time. They do not reproduce a historical cohort's
  eventual state. In particular, successful retry attempts divided by all retry
  attempts is not the recovery percentage of initially failed tasks.
- Do not use task IDs, run IDs, raw errors, prompts, or metadata as labels. Worker
  IDs are the registered Worker identities; monitor cardinality if registration
  churn is high.
- Histograms estimate quantiles. No historical samples are backfilled.

## PromQL examples

Select only the intended Engine scrape job/environment. Apply `rate` or
`increase` before aggregating replicas so resets are handled per series.

```promql
sum(increase(hatchet_succeeded_tasks_total[5m]))

histogram_quantile(0.95,
  sum by (le) (rate(hatchet_task_initial_assignment_delay_seconds_bucket[5m]))
)

sum by (queue, worker_id) (rate(hatchet_task_worker_completions_total[1m]))

sum(increase(hatchet_task_dispatch_attempts_total{retry_bucket="0",outcome="failed"}[5m]))
/
sum(increase(hatchet_task_dispatch_attempts_total{retry_bucket="0"}[5m]))
```

Both dispatch outcome series are initialized on observation, so failure-free
observed traffic has a zero failure numerator. An empty observation window has no
defined percentage; do not hide missing scraping with `or vector(0)`.

Queue inventory gauges still need authoritative snapshots or an independently
validated in-memory scheduler view. Do not reconstruct current queue size solely
by subtracting process-local create/complete counters across restarts.
