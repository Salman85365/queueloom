# SDK transport microbenchmark

This benchmark measures calls to `HttpTransport.send()` using prebuilt events. It
separately accounts for delivery and loss, so quick queue submissions cannot be
mistaken for successful event delivery.

It uses a local HTTP acknowledgement stub, **not the QueueLoom ingestion server**.
It does not measure Celery task overhead, API/database throughput, or production
capacity. The no-op case provides context for Python loop/method-call cost; its
ratio to transport calls is not an application slowdown estimate.

## Reproduce

From the repository root, with Python 3.11 or newer:

```bash
python3 -m venv .venv-benchmark
source .venv-benchmark/bin/activate
python -m pip install -c benchmarks/constraints.txt .
python benchmarks/transport.py \
  --events 2000 --warmup 200 --repeats 5 \
  --output benchmark-result.json
```

`benchmarks/constraints.txt` pins the SDK environment used for the recorded sample.
The benchmark itself adds no dependencies beyond the SDK. Use Python 3.12 to
match that sample's interpreter family; timings will still vary by machine and
load. The script starts its own loopback HTTP stub and needs no Docker, Redis,
credentials, or running QueueLoom server. It typically finishes within a minute.

For a short correctness check, including an incomplete final batch:

```bash
python benchmarks/transport.py --events 137 --warmup 25 --repeats 1
```

No timing threshold is used. The script exits unsuccessfully if a flush times
out, event accounting does not balance, the healthy stub misses or reorders an
event, or the unreachable endpoint unexpectedly delivers an event.

## Method

Each of five repetitions runs three cases, rotating their order:

1. **No-op baseline:** a Python method that does nothing.
2. **Healthy HTTP stub:** the real transport serializes event batches and sends
   them over loopback HTTP. A standard-library server decodes the JSON, records
   event IDs, and responds with HTTP 200. The exact received ID sequence must
   match the submitted workload.
3. **Unreachable HTTP endpoint:** a loopback socket reserves a port without
   listening. An actual HTTP probe records whether this host refuses the
   connection or times out. The same transport submits the workload and counts
   every event eventually abandoned after its failed HTTP attempt.

Every case submits 200 warmup events first. HTTP warmups fully drain before
measurement, excluding connection/client/thread startup. The 2,000 timed events
have fixed IDs, timestamps, and representative small success-event fields; the
sample event is 403 bytes in Pydantic's compact JSON representation. Event
construction, argument capture, framework signals, and redaction are outside the
timed region. Serialization and networking run on the transport's background
thread and can still compete with the submitting thread for execution time.

Configuration is deliberately explicit:

| Setting | Value |
| --- | --- |
| Maximum batch size | 100 events |
| Flush interval | 0.01 seconds |
| Queue capacity | 2,100 events |
| HTTP timeout | 0.25 seconds |
| Retries | 0 |
| Maximum wait for drain | 30 seconds |
| SDK logging | Critical only |
| HTTP environment proxies | Disabled |

These are benchmark settings, **not the transport defaults**. Retries are disabled
to keep the failure experiment bounded; this does not evaluate retry/backoff or
recovery after an outage. On each run the initially empty queue can hold the whole
workload. Thus queue acceptance is inferred from capacity, queue rejection is
zero by construction, and all observed drops in the failure case occur after
HTTP attempts. This is not a queue-saturation experiment.

Two clocks answer different questions:

- `submission_us_per_event`: elapsed time around the submission loop divided by
  the event count. Each sample is an average across that run, not a measurement
  of individual-call tail latency.
- `submission_through_drain_seconds`: time from the first submission until the
  transport has either sent or abandoned every event. `flush()` completing does
  **not** mean delivery succeeded.

Every HTTP result includes submitted, queue-accepted, sent, dropped, failed-batch,
collector-received, and pending counts. Its delivered-events-per-second value
uses **sent events**, yielding zero for the unreachable collector. Do not use
submission rate as delivery throughput.

## Recorded sample

Results and environment details are available in the
[raw JSON](../benchmarks/results/2026-09-15-macos-python312.json). This was an
ordinary local run without CPU isolation, on 2026-09-15, with QueueLoom `0.1.0a1`,
CPython 3.12.14, Darwin 22.6.0, an x86_64 interpreter, and four logical CPUs.
The JSON records exact installed dependency versions and SHA-256 hashes of the
transport, event schema, and benchmark source. It contains no hostname or local
filesystem paths.

Each timing below is the median of five run averages. The range shows the
smallest and largest run averages, not a per-call latency percentile.

| Case | Median submission, µs/event | Range, µs/event | Median submission through drain | Sent / submitted across 5 runs | Dropped |
| --- | ---: | ---: | ---: | ---: | ---: |
| No-op baseline | 0.196 | 0.115–0.917 | Not applicable | Not applicable | Not applicable |
| Healthy HTTP stub | 10.984 | 5.297–24.631 | 0.187 s | 10,000 / 10,000 | 0 |
| Unreachable HTTP endpoint | 9.028 | 7.209–19.057 | 5.473 s | 0 / 10,000 | 10,000 |

The HTTP stub independently recorded all 10,000 measured events in order. All
HTTP runs ended with zero pending events and no queue rejections. The unreachable
endpoint's probe raised `httpx.ConnectTimeout` after approximately 0.252 seconds
on this host. Its fast submissions did not deliver any events: the transport
later abandoned all 10,000 after the configured HTTP attempts.

The difference from the baseline median was 10.788 µs/event for the healthy case
and 8.832 µs/event for the unreachable case. These describe warmed transport
submission in this experiment only. The spread between runs is substantial;
repeat locally and inspect the raw samples before drawing performance conclusions.

## Interpretation and limits

The healthy case checks complete HTTP receipt for this small workload. Receipt
by the stub is not validation, persistence, or processing by QueueLoom's real
server. The unreachable case checks that submission can proceed during this
specific connection failure while loss remains visible. Every unreachable-case
event is lost in this experiment; no durable buffering or eventual-delivery
guarantee is implied.

The benchmark covers one submitting thread, small prebuilt success events, a
warmed transport, a generously sized queue, and a local collector. It does not
cover queue saturation, multiple workers/processes, network latency and TLS,
large tracebacks, default retries, memory growth, application task runtime, or
long-running stability. A single machine's numbers are a reproducible reference
sample, not a cross-machine performance promise or a release gate.
