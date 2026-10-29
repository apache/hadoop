<!-- SPDX-License-Identifier: Apache-2.0 -->

# KMS HTTP throughput benchmark

This opt-in benchmark compares original and fixed delegation-token secret-manager code through real KMS HTTP requests. Each case runs one KMS server JVM, one standalone ZooKeeper JVM, and a separate load-generator JVM. It injects no storage delay and fixes the sequence-number batch size at the trunk default of 100.

## Workloads

A verify operation is `GET /kms/v1/key/benchmark-key/_metadata` with an existing delegation token in the `X-Hadoop-Delegation-Token` header and no authentication cookie. A create operation is `GET /kms/v1/?op=GETDELEGATIONTOKEN&renewer=benchmark&user.name=benchmark` with primary simple authentication and no delegation-token header.

The verify request traverses KMS authentication, delegation-token password verification, authorization, the cached key provider, JSON serialization, and HTTP response processing. The create request traverses KMS primary authentication, token preparation, synchronous ZooKeeper persistence, serialization, and HTTP response processing. A precreated local JCEKS key avoids changing key material during measurement.

The benchmark uses simple authentication for issuance so that only KMS and ZooKeeper servers are needed. It does not measure Kerberos handshakes, TLS, a ZooKeeper quorum, encryption throughput, or production storage/network capacity.

```mermaid
flowchart LR
  C[Load generator JVM] -->|Token authenticated metadata GET| K[One KMS JVM]
  C -->|GETDELEGATIONTOKEN| K
  K -->|Real token writes and range reservation| Z[One ZooKeeper JVM]
  K --> P[Local JCEKS provider]
```

## Fixed comparison matrix

| Dimension | Values |
| --- | --- |
| Code | Original and fixed common jar |
| Verify to create ratio | 100:0, 99:1, 95:5, 90:10 |
| Concurrent HTTP requests | 32 |
| Sequence batch size | 100 |
| Added latency | None |
| Repetitions | Three |
| Warmup per case | Five seconds |
| Measurement per case | Fifteen seconds |

There are eight configurations and 24 cases. The full concurrency sweep is excluded. Warmup and measurement alone take eight minutes; startup, preflight checks, Maven, and draining add overhead. Each case has fresh server processes, ZooKeeper data, and a key store. Each original/fixed pair uses the same seeded request schedule, and pair order alternates across repetitions.

## Authentication and artifact checks

Before loading the server, the harness configures the same test-only diagnostic subclass of KMS's normal pseudo delegation-token handler for both versions. All authentication and token-management behavior delegates to the real implementation. During preflight it counts entry to delegation-token authentication and token-acquisition management operations, checks the actual secret-manager class is `DelegationTokenManager$ZKSecretManager`, checks successful authenticated metadata access, and checks rejection of a token with an invalid password and a request with no credentials. Diagnostic counters are disabled before warmup.

The HTTP client disables cookie management, redirects, and automatic retries. A primary-authentication cookie therefore cannot bypass delegation-token verification, and retry traffic cannot silently change the requested mix. Each successful acquisition response is decoded and its token kind and nonempty identifier/password are checked. Issued tokens are not renewed or cancelled during load.

The child KMS process records its loaded common-jar location and SHA-256 hashes of the actual abstract and ZooKeeper secret-manager class bytes. The comparison runner checks those hashes against the requested jars for every case. The same KMS and client code are used in both versions; only the child server classpaths change. Neither sources nor installed Maven artifacts are modified by the comparison runner.

## Scheduling and measurement boundaries

A shared dispatcher provides a seeded shuffled block of 100 operations with the exact selected ratio. Workers acquire the next operation as soon as their current HTTP request completes. Ratios are based on requests, not counts of dedicated reader/writer threads.

At the time cutoff, the dispatcher finishes submitting the current 100-operation block, then stops and drains. This makes the full submitted ratio exact. Post-cutoff submissions and successful late completions are reported separately. Main goodput is successful completions before the fixed cutoff divided by the configured measurement duration; late completions never inflate it. Actual elapsed time including the block tail and drain is also reported.

Latencies cover requests started before cutoff, including those that finish during drain, so a slow request is not silently removed from tail-latency statistics. Requests started after cutoff do not enter the latency histogram. Latency includes client request construction, HTTP, body consumption, and response validation. Histograms use approximately one-percent logarithmic buckets; percentile values are bucket upper bounds, while maximum latency is exact.

This is a closed-loop capacity benchmark. It does not compensate for coordinated omission and must not be presented as latency under a fixed external arrival rate.

## Metrics and validation

Each case saves total and per-operation goodput; submitted, late-submitted, successful, late-completed, and failed request counts; error categories; per-operation p50/p95/p99/max latency; and measurement/drain duration. Failures are not counted as successful throughput.

KMS, ZooKeeper, and client process snapshots include CPU time, heap usage, live threads, and GC counts/time. ZooKeeper `mntr` snapshots record server latency and outstanding-request counters outside the load window. Those server metrics are cumulative, not per-window percentiles. ZooKeeper data-file sizes describe allocated file size, not physical disk bandwidth. Server logs remain available per case with WARN-level logging configured identically in both versions.

Node counts before and after measurement must increase by exactly the number of completed create operations, including the drained block tail. Runs must have no HTTP/validation errors and the full submitted ratio must match the target. The independent 100:0 control helps identify effects unrelated to creation lock contention. There is no performance pass/fail threshold: a gain or regression is reported as measured.

## Running the benchmark

Use JDK 17 and Maven 3.9.15 or newer, with Hadoop dependencies already built/installed. These tests do not require native libraries. Normal tests run only the scheduler and latency-accounting checks; the HTTP benchmark requires explicit opt-in.

```sh
mvn -o -pl hadoop-common-project/hadoop-kms \
  -Dtest=TestKMSHttpBenchmark -Dkms.benchmark=true \
  -Dmaven.test.failure.ignore=false -Dcheckstyle.skip -Dspotbugs.skip test
```

The defaults are exactly the matrix above. Optional properties are `kms.benchmark.concurrency`, `kms.benchmark.create.percent`, `kms.benchmark.repetitions`, `kms.benchmark.warmup.seconds`, `kms.benchmark.seconds`, `kms.benchmark.output`, `kms.benchmark.label`, and `kms.benchmark.common.jar`. The jar override is prepended only to the child server classpaths, allowing controlled original/fixed comparison without reinstalling dependencies.

Build one original common jar from the baseline checkout and one from the fixed checkout. Keep all other source and dependency versions identical. Then run:

```sh
python3 dev-support/benchmarks/kms-http-benchmark.py \
  --baseline-common /absolute/path/to/original/hadoop-common.jar \
  --fixed-common /absolute/path/to/fixed/hadoop-common.jar \
  --maven mvn \
  --output /absolute/path/to/fresh/results-directory
```

The runner alternates pair order, checks all four mixes and loaded class hashes, and saves incremental `comparison.json` containing raw cases and median/range summaries. Per-case startup logs, configuration, commands, snapshots, and result JSON remain in the output tree. Output directories must be fresh to avoid combining unrelated runs.

## Results

All 24 cases completed with zero HTTP or response-validation errors. Every case had the exact submitted mix, correct token-node growth, and the expected loaded class hashes. The following values are medians of three independent runs at concurrency 32, with five seconds of warmup and fifteen seconds of measurement per run.

| Verify:create | Original requests/s | Fixed requests/s | Ratio | Original verify p99 ms | Fixed verify p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100:0 | 30,811 | 30,150 | 0.98x | 4.14 | 4.48 |
| 99:1 | 17,672 | 26,885 | 1.52x | 5.31 | 4.18 |
| 95:5 | 5,349 | 23,949 | 4.48x | 14.22 | 4.31 |
| 90:10 | 3,103 | 23,174 | 7.47x | 21.81 | 3.39 |

| Verify:create | Original throughput range | Fixed throughput range | Original create p99 ms | Fixed create p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 100:0 | 30,317–32,111 | 23,908–30,815 | — | — |
| 99:1 | 16,099–17,731 | 24,820–27,852 | 7.08 | 18.97 |
| 95:5 | 5,120–5,643 | 22,415–28,690 | 17.18 | 21.38 |
| 90:10 | 3,086–3,140 | 22,945–25,814 | 22.47 | 19.35 |

The 100:0 median was 2.1 percent lower for the fixed build. Two fixed control runs measured 30,150 and 30,815 requests/s; the third measured 23,908. Keep that lower observation: the short runs show substantial variability, and three repetitions do not establish statistical significance for small changes.

Mixed-load median throughput increased by 1.52x, 4.48x, and 7.47x as creation increased from one to ten percent. Token-creation p99 increased at 99:1 and 95:5 even while verification improved. These are closed-loop saturation comparisons: fixed-build creation arrival rates were also higher, so the latency results are not matched-arrival-rate comparisons. No claim of uniformly improved creation latency is supported.

ZooKeeper metrics recorded real fsync activity. No artificial delays, renewals, cancellations, concurrency sweep, or batch-size variation were used. These results concern authenticated metadata requests against a local single-server setup, not encryption requests, TLS/Kerberos costs, or a production quorum. They replace secret-manager-only speedups as the end-to-end evidence for this change.

Charts and the public benchmark report are available in [HADOOP-20045](https://issues.apache.org/jira/browse/HADOOP-20045).
