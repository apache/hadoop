<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Statistic collection with the IOStatistics API

```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
```

The `IOStatistics` API is intended to provide statistics on individual IO
classes -such as input and output streams, *in a standard way which
applications can query*

Many filesystem-related classes have implemented statistics gathering
and provided private/unstable ways to query this, but as they were
not common across implementations it was unsafe for applications
to reference these values. Example: `S3AInputStream` and its statistics
API. This is used in internal tests, but cannot be used downstream in
applications such as Apache Hive or Apache HBase.

The IOStatistics API is intended to

1. Be instance specific:, rather than shared across multiple instances
   of a class, or thread local.
1. Be public and stable enough to be used by applications.
1. Be easy to use in applications written in Java, Scala, and, via libhdfs, C/C++
1. Have foundational interfaces and classes in the `hadoop-common` JAR.

## Core Model

Any class *may* implement `IOStatisticsSource` in order to
provide statistics.

Wrapper I/O Classes such as `FSDataInputStream` anc `FSDataOutputStream` *should*
implement the interface and forward it to the wrapped class, if they also
implement it -and return `null` if they do not.

`IOStatisticsSource` implementations `getIOStatistics()` return an
instance of `IOStatistics` enumerating the statistics of that specific
instance.

The `IOStatistics` Interface exports five kinds of statistic:


| Category | Type | Description |
|------|------|-------------|
| `counter`        | `long`          | a counter which may increase in value; SHOULD BE >= 0 |
| `gauge`          | `long`          | an arbitrary value which can down as well as up; SHOULD BE >= 0 |
| `minimum`        | `long`          | an minimum value; MAY BE negative |
| `maximum`        | `long`          | a maximum value;  MAY BE negative |
| `meanStatistic`  | `MeanStatistic` | an arithmetic mean and sample size; mean MAY BE negative |

Four are simple `long` values, with the variations how they are likely to
change and how they are aggregated.


#### Aggregation of Statistic Values

For the different statistic category, the result of `aggregate(x, y)` is

| Category         | Aggregation |
|------------------|-------------|
| `counter`        | `max(0, x) + max(0, y)`  |
| `gauge`          | `max(0, x) + max(0, y)` |
| `minimum`        | `min(x, y)` |
| `maximum`        | `max(x, y)` |
| `meanStatistic` | calculation of the mean of `x` and `y` ) |


#### Class `MeanStatistic`

## package `org.apache.hadoop.fs.statistics`

This package contains the public statistics APIs intended
for use by applications.

<!--  ============================================================= -->
<!--  Class: MeanStatistic -->
<!--  ============================================================= -->

`MeanStatistic` is a tuple of `(mean, samples)` to support aggregation.

A `MeanStatistic`  with a sample of `0` is considered an empty statistic.

All `MeanStatistic` instances where `sample = 0` are considered equal,
irrespective of the `mean` value.

Algorithm to calculate the mean :

```python
if x.samples = 0:
    y
else if y.samples = 0 :
    x
else:
    samples' = x.samples + y.samples
    mean' = (x.mean * x.samples) + (y.mean * y.samples) / samples'
    (samples', mean')
```

Implicitly, this means that if both samples are empty, then the aggregate value is also empty.

```java
public final class MeanStatistic implements Serializable, Cloneable {
  /**
   * Arithmetic mean.
   */
  private double mean;

  /**
   * Number of samples used to calculate
   * the mean.
   */
  private long samples;

  /**
   * Get the mean value.
   * @return the mean
   */
  public double getMean() {
    return mean;
  }

  /**
   * Get the sample count.
   * @return the sample count; 0 means empty
   */
  public long getSamples() {
    return samples;
  }

  /**
   * Is a statistic empty?
   * @return true if the sample count is 0
   */
  public boolean isEmpty() {
    return samples == 0;
  }
   /**
   * Add another mean statistic to create a new statistic.
   * When adding two statistics, if either is empty then
   * a copy of the non-empty statistic is returned.
   * If both are empty then a new empty statistic is returned.
   *
   * @param other other value
   * @return the aggregate mean
   */
  public MeanStatistic add(final MeanStatistic other) {
    /* Implementation elided. */
  }
  @Override
  public int hashCode() {
    return Objects.hash(mean, samples);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    MeanStatistic that = (MeanStatistic) o;
    if (this.isEmpty()) {
      return that.isEmpty();
    }
    return Double.compare(that.mean, mean) == 0 &&
        samples == that.samples;
  }

  @Override
  public MeanStatistic clone() {
    return new MeanStatistic(this);
  }

  public MeanStatistic copy() {
    return new MeanStatistic(this);
  }

}
```

<!--  ============================================================= -->
<!--  Interface: IOStatisticsSource -->
<!--  ============================================================= -->

### class `org.apache.hadoop.fs.statistics.IOStatisticsSource`

```java

/**
 * A source of IO statistics.
 * These statistics MUST be instance specific, not thread local.
 */
@InterfaceStability.Unstable
public interface IOStatisticsSource {

  /**
   * Return a statistics instance.
   * It is not a requirement that the same instance is returned every time.
   * {@link IOStatisticsSource}.
   * If the object implementing this is Closeable, this method
   * may return null if invoked on a closed object, even if
   * it returns a valid instance when called earlier.
   * @return an IOStatistics instance or null
   */
  IOStatistics getIOStatistics();
}
```

This is the interface which an object instance MUST implement if they are a source of
IOStatistics information.

#### Invariants

The result of `getIOStatistics()` must be one of

* `null`
* an immutable `IOStatistics` for which each map of entries is
an empty map.
* an instance of an `IOStatistics` whose statistics MUST BE unique to that
instance of the class implementing `IOStatisticsSource`.

Less formally: if the statistics maps returned are non-empty, all the statistics
must be collected from the current instance, and not from any other instances, the way
some of the `FileSystem` statistics are collected.


The result of `getIOStatistics()`, if non-null, MAY be a different instance
on every invocation.


<!--  ============================================================= -->
<!--  Interface: IOStatistics -->
<!--  ============================================================= -->

### class `org.apache.hadoop.fs.statistics.IOStatistics`

These are per-instance statistics provided by an object which
implements `IOStatisticsSource`.

```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface IOStatistics {

  /**
   * Map of counters.
   * @return the current map of counters.
   */
  Map<String, Long> counters();

  /**
   * Map of gauges.
   * @return the current map of gauges.
   */
  Map<String, Long> gauges();

  /**
   * Map of minumums.
   * @return the current map of minumums.
   */
  Map<String, Long> minumums();

  /**
   * Map of maximums.
   * @return the current map of maximums.
   */
  Map<String, Long> maximums();

  /**
   * Map of meanStatistics.
   * @return the current map of MeanStatistic statistics.
   */
  Map<String, MeanStatistic> meanStatistics();

}
```

### Statistic Naming

The naming policy of statistics is designed to be readable, shareable
and ideally consistent across `IOStatisticSource` implementations.

* Characters in key names MUST match the regular expression
  `[a-z|0-9|_]` with the exception of the first character, which
  MUST be in the range `[a-z]`. Thus the full regular expression
  for a valid statistic name is:

        [a-z][a-z|0-9|_]+

* Where possible, the names of statistics SHOULD be those defined
  with common names.

        org.apache.hadoop.fs.statistics.StreamStatisticNames
        org.apache.hadoop.fs.statistics.StoreStatisticNames

   Note 1.: these are evolving; for clients to safely reference their
   statistics by name they SHOULD be copied to the application.
   (i.e. for an application compiled hadoop 3.4.2 to link against hadoop 3.4.1,
   copy the strings).

   Note 2: keys defined in these classes SHALL NOT be removed
   from subsequent Hadoop releases.

* A common statistic name MUST NOT be used to report any other statistic and
  MUST use the pre-defined unit of measurement.

* A statistic name in one of the maps SHOULD NOT be re-used in another map.
  This aids diagnostics of logged statistics.

### Statistic Maps

For each map of statistics returned:

* The operations to add/remove entries are unsupported: the map returned
  MAY be mutable by the source of statistics.

* The map MAY be empty.

* The map keys each represent a measured statistic.

* The set of keys in a map SHOULD remain unchanged, and MUST NOT remove keys.

* The statistics SHOULD be dynamic: every lookup of an entry SHOULD
  return the latest value.

* The values MAY change across invocations of `Map.values()` and `Map.entries()`

* The update MAY be in the `iterable()` calls of the iterators returned,
  or MAY be in the actual `iterable.next()` operation. That is: there is
  no guarantee as to when the evaluation takes place.

* The returned `Map.Entry` instances MUST return the same value on
 repeated `getValue()` calls. (i.e once you have the entry, it is immutable).

* Queries of statistics SHOULD be fast and non-blocking to the extent
 that if invoked during a long operation, they will prioritize
 returning fast over most timely values.

* The statistics MAY lag; especially for statistics collected in separate
 operations (e.g stream IO statistics as provided by a filesystem
 instance).

* Statistics which represent time SHOULD use milliseconds as their unit.

* Statistics which represent time and use a different unit MUST document
  the unit used.

### Thread Model

1. An instance of `IOStatistics` can be shared across threads;

1. Read access to the supplied statistics maps MUST be thread safe.

1. Iterators returned from the maps MUST NOT be shared across threads.

1. The statistics collected MUST include all operations which took
   place across all threads performing work for the monitored object.

1. The statistics reported MUST NOT be local to the active thread.

This is different from the `FileSystem.Statistics` behavior where per-thread statistics
are collected and reported.

That mechanism supports collecting limited read/write statistics for different
worker threads sharing the same FS instance, but as the collection is thread local,
it invariably under-reports IO performed in other threads on behalf of a worker thread.


## Statisic Snapshot

A snapshot of the current statistic values MAY be obtained by calling
`IOStatisticsSupport.snapshotIOStatistics()`

```java
  public static <X extends IOStatistics & Serializable> X
      snapshotIOStatistics(IOStatistics statistics)
```

This snapshot is serializable through Java serialization and through
Jackson to/from JSON.

## Helper Classes


### class `org.apache.hadoop.fs.statistics.IOStatisticsSupport`

This provides helper methods to work with IOStatistics sources and instances.

Consult the javadocs for its operations.

### class `org.apache.hadoop.fs.statistics.IOStatisticsLogging`

Support for efficiently logging `IOStatistics`/`IOStatisticsSource`
instances.

These are intended for assisting logging, including only enumerating the
state of an `IOStatistics` instance when the log level needs it.

```java
LOG.info("IOStatistics after upload: {}", demandStringify(iostats));

// or even better, as it results in only a single object creations
Object latest = demandStringify(iostats);
LOG.info("IOStatistics : {}", latest);
/* do some work. */
LOG.info("IOStatistics : {}", latest);

```

## Package `org.apache.hadoop.fs.statistics.impl`

This contains implementation classes to support providing statistics to applications.

These MUST NOT BE used by applications. If a feature is needed from this package then
the provisioning of a public implementation MAY BE raised via the Hadoop development
channels.

These MAY be used by those implementations of the Hadoop `FileSystem`, `AbstractFileSystem`
and related classes which are not in the hadoop source tree. Implementors MUST BE
aware that the implementation this code is unstable and may change across
minor point releases of Hadoop.
