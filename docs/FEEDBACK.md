# Try QueueLoom and tell us where it falls short

QueueLoom is an alpha for investigating Python background jobs. Feedback from developers
who run Celery is especially useful. A failed installation or a confusing screen is useful
feedback too; there is no need to finish the whole walkthrough.

## A short evaluation

Allow about 10 minutes after installing dependencies. Use the
[v0.1.0a2 technical walkthrough](WALKTHROUGH.md) and its synthetic local workload:

1. Start the demo and open the dashboard.
2. Find a failed task and identify its exception from the timeline.
3. Find a retry and follow its eventual outcome. The workload is random, so a retry may take
   time to appear; record that if it makes the demo hard to evaluate.
4. Compare a report's queue latency with its execution duration.
5. Open Diagnose and check whether its error clusters help explain the failures.

No Redis server, Docker, account, AI key or paid service is needed. The in-memory broker
exercises a real local Celery worker, but it does not test a distributed deployment.

## Send feedback

[**Open the evaluation form**](https://github.com/Salman85365/queueloom/issues/new?template=evaluation.yml)

The most useful response tells us:

- Which source revision or release, Python version and operating system you tried.
- How far you got, and roughly how long it took to reach useful task data.
- One question you could or could not answer from the dashboard.
- The most confusing step or missing information.
- Whether it would help with a real task you debug today, and what you currently use instead.

For a reproducible failure, use the
[bug report form](https://github.com/Salman85365/queueloom/issues/new?template=bug_report.yml).
Include the smallest example and expected versus actual behavior. GitHub issues are public;
use synthetic task data and remove API keys, private URLs and sensitive exception text from
logs or screenshots.

## What the current evidence shows

- The [release and install checks](INSTALL.md) establish that the packaged alpha can be
  installed and exercised.
- The [v0.1.0a1 benchmark](BENCHMARK.md) measures transport behavior against a local collector,
  with raw results and explicit limits; it is historical evidence, not a new v0.1.0a2 measurement.
- The demo shows task events reaching the dashboard with a real in-process Celery worker.

These checks do not establish production reliability or replace feedback from outside
developers. External evaluations are recorded as issues when reviewers choose to submit them.

## Contributing a fix

If you find an existing report, add your reproduction there. For a code fix, describe the
observed problem, add a regression test where appropriate, and run the
[development checks](../README.md#development). Small fixes and clearer documentation are
welcome; a star or fork is not required to participate.
