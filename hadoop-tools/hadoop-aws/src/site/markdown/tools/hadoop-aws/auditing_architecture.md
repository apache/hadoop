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

# Object Store Auditing: Architecture

This the architecture document of the S3A [Auditing](auditing.html) component.


The S3A auditing subsystem is defined in the package `org.apache.hadoop.fs.s3a.audit`.

This package is declared `LimitedPrivate`; some classes inside are explicitly
declared `@Public` (e.g `AuditConstants`) while others `@Private`. If declared
`@Private`, external auditing modules MUST NOT use them.

```java
@InterfaceAudience.LimitedPrivate("auditing extensions")
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.audit;
```

The auditing implementation classes are all in the package
package `org.apache.hadoop.fs.s3a.audit.impl`.
These MUST NOT be subclassed or invoked directly by external code.

Audit classes/interfaces which are intended to be used across object store
clients and manipulated from other parts of hadoop are in `hadoop-common` JAR in
the package `org.apache.hadoop.fs.store`.

### Interface `org.apache.hadoop.fs.store.audit.AuditSpan`

An AuditSpan audits a single Hadoop FileSystem API operation such as
`open(Path)`, `rename(Path, Path)` or `listFiles(Path, Boolean)`.

```java
public interface AuditSpan extends Closeable {

  String getSpanId();

  String getOperationName();

  long getTimestamp();

  AuditSpan activate();

  void deactivate();

  default void close() {
    deactivate();
  }

  boolean isValidSpan();

  void set(String key, String value);
}
```

Audit Spans are intended for use _within_ FileSystem clients; that is
not visible to applications invoking them.

1. One `AuditSpan` is created per Hadoop FS API call.
1. Each span has the name of the operation and optionally source and destination paths.
1. A span may be `activate()`d or `deactivate()`d. Between these two operations a span is _active_.
1. Activation is on a per-thread basis. A single span can be active in multiple threads
   simultaneously; other spans may be active in other threads.
1. A single filesystem can have only one active span per thread, but different filesystem
   instances MAY have different active spans.
1. All store operations performed on a thread are considered _within_
   the active span.
1. Spans do not explicitly terminate; they just stop being invoked; eventually
   Garbage Collection should dispose of them.
1. Every `AuditSpan` has an ID, which *must* be unique. A UUID and a counter is
   the base implementation.
1. The `AuditSpan` class does extend `Closeable`; calling `close()` simply deactivates
   the span _for that thread_.
1. All FS API calls which return objects which go on to perform FS operations
   (`create()`, `open()`, incremental list calls which return `RemoteIterator`  etc) pass
   the span into the objects which they return.
1. As a result, any store IO performed by the returned streams and iterators MUST activate
   the span before that IO and deactivate it afterwards.
1. There is also the "Unbonded Span" which is the effective span of an FS when there
   is no active span.
1. Calling a store within the unbonded span is generally considered an bug. In the S3A codebase
   this should never happen outside copy/rename operations, and will be logged at
   warning level in the Logging Auditor.

### interface `org.apache.hadoop.fs.store.audit.AuditSpanSource`

This interface is implemented by sources of audit spans.

```java
public interface AuditSpanSource<T extends AuditSpan> {

  T createSpan(String operation,
      @Nullable String path1,
      @Nullable String path2)
      throws IOException;
}
```

All S3 Auditors implement this interface, as does the `AuditManagerS3A`.
(Implementation note: so do `S3AFileSystem` and `WriteOperationHelper`)

When a Hadoop FS API call is made of an `S3AFileSystem` instance, it
calls `startOperation` on its audit manager; this will relay it to
the auditor is bound to.

The auditor then creates and returns a span for the specific operation.
The AuditManagerS3A will automatically activate the span returned by the auditor
(i.e. assign it the thread local variable tracking the active span in each thread).

### Memory Leakage through `ThreadLocal` misuse

The original implementation of the integration with the S3AFileSystem class
contained a critical defect,
[HADOOP-18091](https://issues.apache.org/jira/browse/HADOOP-18091) _S3A auditing leaks memory through ThreadLocal references_.

The original code was written with the assumption that when the `ActiveAuditManagerS3A` service was
garbage collected, references in its `ThreadLocal` field would be freed.
In fact, they are retained until all threads with references are terminated.
If any long-lived thread had performed an s3 operation which created a span,
a reference back to the audit manager instance was created
*whose lifetime was that of the thread*

In short-lived processes, and long-lived processes where a limited set of
`S3AFileSystem` instances were reused, this had no adverse effect.
Indeed, if the filesystem instances were retained in the cache until
the process was shut down, there would be strong references to the owning
`S3AFileSystem` instance anyway.

Where it did have problems was when the following conditions were met
1. Process was long-lived.
2. Long-lived threads in the process invoked filesystem operations on `s3a://` URLs.
3. Either `S3AFileSystem` instances were created repeatedly, rather than retrieved
   from the cache of active instances.
4. Or, after a query for a specific user was completed,
   `FileSystem.closeAllForUGI(UserGroupInformation)` was invoked to remove all
   cached FS instances of that user.

Conditions 1, 2 and 4 are exactly those which long-lived Hive services can
encounter.

_Auditing was disabled by default until a fix was implemented._

The memory leak has been fixed using a new class `org.apache.hadoop.util.WeakReferenceMap`
to store a map of thread IDs to active spans. When the S3A filesystem is closed,
its audit manager service is stopped and all references to spans removed from the
map of thread ID to span.

Weak References are used for the map so that span references do not consume memory even if
threads are terminated without resetting the span reference of that thread.
There is therefore a theoretical risk that if a garbage collection takes place during
execution of a spanned operation, the reference will be lost.

This is not considered an issue as all bounded entry points into the S3A filesystem
retain a strong reference to their audit span.

All entry points which return an object which can invoke s3 operations (input and output
streams, list iterators, etc.) also retain a strong reference to their span, a reference
they activate before performing S3 operations.
A factory method is provided to demand-generate a new span if, somehow, these conditions
are not met. The "unbounded span" is used here.
Except in deployments where `fs.s3a.audit.reject.out.of.span.operations` is true,
invoking S3 operations within the unbounded span are permitted.
That option is set to `true` within S3A test suites.
Therefore it is unlikely that any operations are invoked in unbounded spans except
for the special case of copy operations invoked by the transfer manager threads.
Those are already ignored in the logging auditor, whose unbounded span ignores
requests which `AWSRequestAnalyzer.isRequestNotAlwaysInSpan()` indicates
may happen outside of a span.
This is restricted to bucket location probes possibly performed by the SDK
on instantiation, and copy part/complete calls.


```java
  public static boolean
      isRequestNotAlwaysInSpan(final Object request) {
    return request instanceof CopyPartRequest
        || request instanceof CompleteMultipartUploadRequest
        || request instanceof GetBucketLocationRequest;
  }
```


### Class `org.apache.hadoop.fs.audit.CommonAuditContext`

This is a class in `hadoop-common` which provides a context to auditing operations
across all instrumented filesystems.

It's Global Context values are a map of string keys and values, which are
constant across all threads. This is where global values such as a process
UUID and the class executed by `ToolRunner` are noted.

The `CommonAuditContext.currentAuditContext()` call returns a thread local
`CommonAuditContext` which is a thread-local map of keys to string values.
It also supports a map of _evaluated entries_.
This is a map of type `Map&lt;String, Supplier&lt;String>>`.
supplier methods/lambda expressions set here are dynamically evaluated when
auditors retrieve the values.
Spans may be used on different thread from that which they were created.
Spans MUST always use the values from the `currentAuditContext()` in the creation
thread.

#### Memory Usage of `CommonAuditContext`

The `CommonAuditContext` map has a `ThreadLocal` field to store global
information which is intended to span multiple operations across multiple
filesystems, for example the MapReduce or Spark job ID, which is set
in the S3A committers.

Applications and Hadoop code MUST NOT attach context entries
which directly or indirectly consumes lots of memory, as the life
of that memory use will become that of the thread.

Applications and Hadoop code SHOULD remove context entries when
no-longer needed.

If memory leakage is suspected here, set the log
`org.apache.hadoop.fs.audit.CommonAuditContext` to `TRACE`
to log the origin of operations which add log entries.

This will produce a log entry whose stack trace will show the caller chain.
```
2022-01-26 16:10:28,384 TRACE audit.CommonAuditContext (CommonAuditContext.java:put(149)) - Adding context entry t1
java.lang.Exception: t1
    at org.apache.hadoop.fs.audit.CommonAuditContext.put(CommonAuditContext.java:149)
    at org.apache.hadoop.fs.audit.CommonAuditContext.init(CommonAuditContext.java:192)
    at org.apache.hadoop.fs.audit.CommonAuditContext.createInstance(CommonAuditContext.java:210)
    at org.apache.hadoop.fs.audit.CommonAuditContext.lambda$static$0(CommonAuditContext.java:119)
    at java.lang.ThreadLocal$SuppliedThreadLocal.initialValue(ThreadLocal.java:284)
    at java.lang.ThreadLocal.setInitialValue(ThreadLocal.java:180)
    at java.lang.ThreadLocal.get(ThreadLocal.java:170)
    at org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext(CommonAuditContext.java:219)
    at org.apache.hadoop.fs.audit.TestCommonAuditContext.<init>(TestCommonAuditContext.java:54)
```


### class `NoopAuditor`

This auditor creates spans which doesn't do anything with the events.

```xml
<property>
  <name>fs.s3a.audit.service.classname</name>
  <value>org.apache.hadoop.fs.s3a.audit.impl.NoopAuditor</value>
</property>
```

This is *not* the same as disabling auditing, as it still uses the `ActiveAuditManagerS3A` class
which is the source of memory leaks.

Avoid using it except in tests as there is no benefit -simply significant cost.

### class `LoggingAuditor`

The logging auditor logs operations to the console at DEBUG level (to keep the noise down),
and attaches the operation details in the HTTP "referer" header.

It can be configured to raise an exception whenever an S3 API call is made
from within the unbonded span.
This option primarily for development, as it is how we can verify that all
calls are audited/identify where this is not possible.


### class `ActiveAuditManager` interface `ActiveAuditManager`

The class `ActiveAuditManager` provides all the support needed for
`S3AFileSystem` to support spans, including
* Loading and starting the auditor declared in a Hadoop configuration.
* Maintaining a per-thread record of the active audit span
* Switching spans on `AuditSpan.activate()` and reverting to the
  unbonded span in `deactivate()` and `close()`.
* Providing binding classes to be passed into the AWS SDK so as to
  invoke audit operations prior to requests being issued. This is essential to
  guarantee that all AWS S3 operations will be audited.

It's a YARN composite service which follows the standard lifecycle.
The actual auditor is instantiated initialized and started in its service
start phase; closed when the Audit Manager is stopped.

```java
public interface AuditManagerS3A extends Service,
    AuditSpanSource<AuditSpanS3A>,
    AWSAuditEventCallbacks,
    ActiveThreadSpanSource<AuditSpanS3A> {

  /**
   * Get the auditor; valid once initialized.
   * @return the auditor.
   */
  OperationAuditor getAuditor();

  /**
   * Create the request handler(s) for this audit service.
   * The list returned is mutable; new handlers may be added.
   * @return list of handlers for the SDK.
   * @throws IOException failure.
   */
  List<RequestHandler2> createRequestHandlers() throws IOException;

  /**
   * Return a transfer state change callback which
   * fixes the active span context to be that in which
   * the state change listener was created.
   * This can be used to audit the creation of the multipart
   * upload initiation request which the transfer manager
   * makes when a file to be copied is split up.
   * This must be invoked/used within the active span.
   * @return a state change listener.
   */
  TransferStateChangeListener createStateChangeListener();

  /**
   * Check for permission to access a path.
   * The path is fully qualified and the status is the
   * status of the path.
   * This is called from the {@code FileSystem.access()} command
   * and is a soft permission check used by Hive.
   * @param path path to check
   * @param status status of the path.
   * @param mode access mode.
   * @return true if access is allowed.
   * @throws IOException failure
   */
  boolean checkAccess(Path path, S3AFileStatus status, FsAction mode)
      throws IOException;
}
```

## Using Audit Spans within the S3A Connector

1. All public FS API calls must be marked as `@AuditEntryPoint` and initiate  a span.
1. All interfaces which provided a subset of the store API to another class
   (e.g. listing) MUST pick up the current span, store it, and activate/deactivate
   the span when invoked. This ensures use across threads.
1. Methods/classes which operate across threads must store the audit span which
   was active on their creation/invocation, and activate it in all threads which
   interact with the FS. This should be automatic if callback interfaces
   do this.
1. All S3 SDK request objects MUST be created in the request factory. Add new
   methods if need be.

## Implementing a custom `OperationAuditor`


_This extension point is `@Unstable`_

```java
@InterfaceAudience.LimitedPrivate("S3A auditing extensions")
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.audit;
```

A custom `OperationAuditor` auditor is a class which implements the interface
`org.apache.hadoop.fs.s3a.audit.OperationAuditor`. This SHOULD be done by
subclassing
`org.apache.hadoop.fs.s3a.audit.AbstractOperationAuditor`.

It is a YARN service and follows the lifecycle:
configured in `serviceInit()`; start any worker threads/perform startup
operations in `serviceStart()` and shutdown in `serviceStop()`.

In use, it will be instantiated in `S3AFileSystem.initialize()`
and shutdown when the FS instance is closed.

It will be instantiated before the AWS S3 Client is built -it may provide a
request handler to be part of the handler chain of the S3 request pipeline.

It will be closed in the `FileSystem.close()` operation, after the S3 Client is
itself closed.


### Design Decisions/Review questions

### Why use https://audit.example.org/ as referrer host?

IETF requires *.example.org to be unresolvable through DNS, so with a well configured DNS there's never any host to probe.

It guarantees that there will never be real HTTP requests coming in from that host.


## And why `hadoop/1/` in the referrer path?

Provenance and versioning.


### Why no explicit end to an AuditSpan?

While most API calls have a bounded duration, e.g. `getFileStatus()`,
some calls have a very long lifespan (input and output streams).
List iterators are never formally terminated, they just "fall out of scope",
Thus, they'd never end.

Having a uniform "Audit Spans are never explicitly terminated" design
means that it is consistent everywhere.

### Can you activate an already active audit span?

It's a no-op.

It does mean that if you deactivate the span the first time, then the thread
reverts immediately to the unbonded span.

### Why does `AuditSpan.deactivate()` switches to the unbound span, rather than the span which was active before

Again, it gets complicated fast, especially when audit spans our shared across threads.

Because of the sharing you cannot store the previous span in a field within the AuditSpan itself.

Instead you need to have a thread local stack per FileSystem instance of active audit spans.

And you had better be confident that audit spans are correctly activated and deactivated,
with no span deactivated more than once -else the stack will become confused.

Having a simple "In Span" or "Out of Span" model avoids this problem.
However, it does prevent the S3A FileSystem implementation methods from
calling other methods which create new spans.
Hence the annotation of all span entry points as `@AuditEntryPoint` and
a need for rigorous review of the invocations.
As with the need to make sure that we never call retry() around a method tagged `@Retry`,
making sure that an audit entry point doesn't invoke another audit entry point
is going to become another piece of maintenance overhead.



### History


* 2021-02 Creation. [HADOOP-17511](https://issues.apache.org/jira/browse/HADOOP-17511) _Add an Audit plugin point for S3A auditing/context_.

