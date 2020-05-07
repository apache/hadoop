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

# Introduction to the IOStatistics API

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
applications such as 

The new IOStatistics API is intended to 

1. Be instance specific:, rather than shared across multiple instances
   of a class, or thread local.
1. Be public and stable enough to be used by applications.
1. Be easy to use in applications written in Java, Scala, and, via libhdfs, C/C++
1. Have foundational interfaces and classes in the `hadoop-common` JAR.

## Core model

Any Hadoop I/O class *may* implement `IOStatisticsSource` in order to
provide statistics.

Wrapper I/O Classes (e.g `FSDataInputStream`, `FSDataOutputStream` *should*
implement the interface and forward it to the wrapped class, if they also
implement it -or return `null` if they do not.

`IOStatisticsSource` implementations `getIOStatistics()` return an
instance of `IOStatistics` enumerating the statistics of that specific
instance.

The `IOStatistics` implementations provide

* A way to enumerate all keys/statistics monitored.
* An iterator over all such keys and their latest values.
* A way to explitly request the value of specific statistic.

## package `org.apache.hadoop.fs.statistics`

This package contains the public statistics APIs intended
for use by applications.

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

<!--  ============================================================= -->
<!--  Interface: IOStatistics -->
<!--  ============================================================= -->

### class `org.apache.hadoop.fs.statistics.IOStatistics`

These are low-cost per-instance statistics provided by any Hadoop I/O class instance.

```java
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface IOStatistics extends Iterable<Map.Entry<String, Long>> {

  /**
   * Get the value of a statistic.
   *
   * @return The value of the statistic, or null if not tracked.
   */
  Long getStatistic(String key);

  /**
   * Return true if a statistic is being tracked.
   *
   * @return True only if the statistic is being tracked.
   */
  boolean isTracked(String key);

  /**
   * Get the set of keys.
   * No guarantees are made about the mutability/immutability
   * of this set.
   * @return the set of keys.
   */
  Set<String> keys();

}
```


The statistics MUST BE for the specific instance of the source;
possibly including aggregate statistics from other objects
created by that stores.
For example, the statistics of a filesystem instance must be unique
to that instant and not shared with any other.
However, those statistics may also collect and aggregate statistics
generated in the use of input and output streams created by that
file system instance.

The iterator is a possibly empty iterator over all monitored statistics.

* The set of statistic keys SHOULD remain unchanged, and MUST NOT
remove keys.

*  The statistics SHOULD be dynamic: every call to `iterator()`
 MAY return a current/recent set of statistics.

* The values MAY change across invocations of `iterator()`.

* The update MAY be in the `iterable()` call, or MAY be in the actual
 `iterable.next()` operation.

* The returned Map.Entry instances MUST return the same value on
 repeated `getValue()` calls.

* Queries of statistics SHOULD Be fast and Nonblocking to the extent
 that if invoked during a long operation, they will prioritize
 returning fast over most timely values.

* The statistics MAY lag; especially for statistics collected in separate
 operations (e.g stream IO statistics as provided by a filesystem
 instance).


### Thread Model

1. An instance of IOStatistics can be shared across threads;
 a call to `iterator()` is thread safe.

1. The actual `Iterable` returned MUST NOT be shared across threads.

1. The statistics collected MUST include all operations which took place across all threads performing work for the monitored object.

1. The statistics reported MUST NOT be local to the active thread.

This is different from the `FileSystem.Statistics` behavior where per-thread statistics
are collected and reported.
That mechanism supports collecting limited read/write statistics for different
worker threads sharing the same FS instance, but as the collection is thread local,
it invariably under-reports IO performed in other threads on behalf of a worker thread.




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

These MUST NOT be used by applications. If a feature is needed from this package then
the provisioning of a public implementation should be raised via the Hadoop development
channels.

These MAY be used by those implementations of the Hadoop `FileSystem`, `AbstractFileSystem`
and related classes which are not in the hadoop source tree. Implementors MUST BR
that all this code is unstable and may change across minor point releases of Hadoop.
