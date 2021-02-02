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

# S3A Auditing


The S3A connector provides an extension point for auditing requests to S3.
The auditing can take place at the entry point to every FS operation,
and inside the AWS S3 SDK, immediately before the request is executed.


## Auditing workflow

1. An audit service can be instantiated for each S3A FileSystem instance,
   created during FS initialization and closed when the FS instance is closed.
1. The S3A FS will request an `AuditSpan` for each Hadoop FileSystem API call.
1. The audit span will have its callbacks invoked during each of the S3 operations
   invoked during the execution of the API call.
1. This allows the audit service to log requests made and associate with users and operations.
1. And/or reject operations.

Thus: auditing mechanisms can be plugged in to provide (best-effort) auditing as well
as hinted allow/deny security.

* Why best effort: coverage is not complete. See limitations below.
* Why "hinted" security? Any custom code running in the JVM could retrieve the AWS
  credential chain and so bypass this auditing mechanism.
  
## Architecture

The auditing subsystem is implemented in the package `org.apache.hadoop.fs.s3a.audit`.

This package is declared `LimitedPrivate`; some classes inside are explicitly
declared `@Public` (e.g `AuditConstants`) while others `@Private`. If declared
`@Private`, external auditing modules MUST NOT use them.

```java
@InterfaceAudience.LimitedPrivate("auditing extensions")
@InterfaceStability.Unstable
package org.apache.hadoop.fs.s3a.audit;
```

![image](audit.png)


### Interface `AuditSpan`

This is the central class as far as actual FS operations are concerned.

1. One `AuditSpan` is created per Hadoop FS API call; the S3A FileSystem's `AuditManager` provides this.
1. Each span has the name of the operation and optionally source and destination paths.
1. A span may be _activated_ or _deactivated_. Between these two operations a span is _active_.
1. Activation is on a per-thread basis. A single span can be active in multiple threads
   simultaneously; other spans may be active in other threads.
1. A single filesystem can have only one active span per thread, but different filesystem
   instances MAY have different active spans. 
1. All S3 operations performed on a thread are considered _within_
   the active span.
1. Spans do not explicitly terminate; they just stop being invoked; eventually
   Garbage Collection should dispose of them.
1. The `AuditSpan` class does extend `Closeable`; calling `close()` simply deactivates
   the span _for that thread_.
1. FS API calls which return objects which go on to perform FS operations
   (`create()`, `open()`, list calls which return Remote Iterators etc.) pass
   the span into those objects.
1. And Any S3 IO performed by the objects (GET, POST, PUT, LIST,...) activate
   the span before that IO.
1. There is also the "Unbonded Span" which is the effective span of an FS when there
   is no active span.
1. Calling S3 within the unbonded span is considered an error. In the S3A codebase
   this should never happen outside copy/rename operations, and will be logged at
   warning level in the Logging Auditor.

### interface `AuditSpanSource`

This interface is implemented by sources of audit spans.

```
public interface AuditSpanSource {

  AuditSpan startOperation(String name,
      @Nullable String path1,
      @Nullable String path2)
      throws IOException;
}
```

All Auditors implement this interface, as does the `AuditManager`.

Implementation note: so do `S3AFileSystem` and `WriteOperationHelper`.

When a Hadoop FS API call is made of an S3AFileSystem instance, it
calls `startOperation` on its audit manager; this will relay it to
the auditor is bound to.

The auditor then creates and returns a span for the specific operation.
The AuditManager will automatically activate the span returned by the auditor
(i.e. assign it the thread local variable tracking the active span in each thread)

### class `NoopAuditor`

This auditor creates spans which perform no auditing.
It is very efficient and reliable.

### class `LoggingAuditor`

The logging auditor logs operations to the console at DEBUG level (to keep the noise down),
and attaches the operation details in the HTTP "referer" header.

It can be configured to raise an exception whenever an S3 API call is made
from within the unbonded span.
This is primarily for development, as it is how we can verify that all
calls are audited/identify where this is not possible.


### class `ActiveAuditManager` interface `ActiveAuditManager`

The class `ActiveAuditManager` provides all the support needed for the
S3AFileSystem to support spans, including
* Loading and starting the auditor declared in a Hadoop configuration.
* Maintaining a per-thread record of the active audit span
* Switching spans on `AuditSpan.activate()` and reverting to the unbonded span in `deactivate()` and `close().
* Providing binding classes to be passed into the AWS SDK so as to
  invoke audit operations prior to requests being issued. This is essential to
  guarantee that all AWS S3 operations will be audited.

It's a YARN composite service which follows the standard lifecycle.
The actual auditor is instantiated initialized and started in its service
start phase; closed when the Audit Manager is stopped.

```java
public interface AuditManager extends Service, AuditSpanSource,
    AuditSpanCallbacks {

  /**
   * Get the wrapped active span.
   * @return the currently active span.
   */
  AuditSpan getActiveThreadSpan();

  /**
   * Create the request handler(s) for this audit service.
   * The list returned is mutable; new handlers may be added.
   * @return list of handlers for the SDK.
   */
  List<RequestHandler2> createRequestHandlers();

  /**
   * Return a transfer state change callback which
   * fixes the active span context to be that in which
   * the state change listener was created.
   * This ensures that copy operations get bounded
   * to the correct span.
   * @return a state change listener.
   */
  TransferStateChangeListener createStateChangeListener();
}
```

### Integration with S3A Committers

Work submitted through the S3A committer will have the job (query) ID associated
with S3 operations taking place against all S3A filesystems in that thread.

For this to be useful, the work performed in a task MUST be in the same thread
which called `jobSetup()` or `taskSetup()` on the committer.

## Using Auditing

### Using the Logging Auditor


```xml
<property>
  <name>fs.s3a.audit.service.classname</name>
  <value>org.apache.hadoop.fs.s3a.audit.LoggingAuditororg.apache.hadoop.fs.s3a.audit.LoggingAuditor</value>
</property>
```


```
# Auditing
log4j.logger.org.apache.hadoop.fs.s3a.audit=DEBUG
```

### Integration with S3 Logging

In the logging auditor the HTTP `referer` field of every AWS S3 request is built up into a URL
which provides context and span information.
As this field is saved in the S3 logs, if S3 bucket logging is enabled, the
logs will be able to correlate access by S3 clients to the actual operations
taking place.


## Debugging

The `org.apache.hadoop.fs.s3a.audit` log context contains logs for the
different components implementing auditing.

Logging of requests audited with the `LoggingAuditService` can be enabled
by setting that log to debug.

```
# Log before a request is made to S3
log4j.logger.org.apache.hadoop.fs.s3a.audit.LoggingAuditor=DEBUG
```

This adds one log line per request -and does provide some insight into communications
between the S3A client and AWS S3.

For low-level debugging of the Auditing system, set the log to `TRACE`:
```
# log request creation, span lifecycle and other low-level details
log4j.logger.org.apache.hadoop.fs.s3a.audit=TRACE
```

This is very noisy and not recommended in normal operation.

## Implementing a custom Auditor

A custom auditor is a class which implements the interface `org.apache.hadoop.fs.s3a.audit.OperationAuditor`.
This SHOULD be done by subclassing `org.apache.hadoop.fs.s3a.audit.AbstractOperationAuditor`.

* It is a YARN service and follows the lifecycle: configured in `serviceInit()`, SHALL start
any worker threads/perform startup operations in `serviceInit()` and shutdown in `serviceStop()`.

* In use, it will be instantiated in `S3AFileSystem.initialize()` and shutdown when the FS instance
is closed.

## Limitations

This is not a means of controlling access to S3 resources. It is a best-effort
attempt at supporting logging of FileSystem operations API calls, and,
in particular, correlating  S3 object requests with those FS API calls, and ideally the jobs.

* Low-level code using public S3A methods intended only for internal use may not create spans.
* Code which asks for the AWS S3 client may bypass span creation
* Application code can also create a new S3 client (reusing any existing credentials)
  and so have unaudited access to S3.
* There's (currently) not tie-up with OpenTelemetry.
* Uploads and copy operations through the TransferManager do not pick up an active
  span because work is executed in threads which the S3A code cannot update.
* There's a limit to how long an http referer header can be; operations on long paths may be incompletely logged.
* There's no guarantee that audit spans will be closed/deactivated

## Outstanding TODO items

* Statistics for auditing
* thread ID to go into span from common context (and so travel into helper spans)
* Maybe: parse AWS S3 log lines for use in queries, with some test data. LineRecordReader would be the start
* log auditor to log AWS Request ID in responses, especially error reporting. +
  verify
* javadocs for RequestFactory
* Opportunities to simplify

Tests for

* RequestFactoryImpl
* callback from AWS SDK, including handling of no request handler
