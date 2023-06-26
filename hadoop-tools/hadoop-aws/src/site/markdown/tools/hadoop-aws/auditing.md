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

# Object Store Auditing


The S3A connector provides an extension point for auditing requests to S3.
The auditing can take place at the entry point to every FS operation,
and inside the AWS S3 SDK, immediately before the request is executed.

The full architecture is covered in [Auditing Architecture](auditing_architecture.html);
this document covers its use.

## Important: Auditing is disabled by default

Due to a memory leak from the use of `ThreadLocal` fields, this auditing feature
leaked memory as S3A filesystem instances were created and deleted.
This caused problems in long-lived processes which either do not re-use filesystem
instances, or attempt to delete all instances belonging to specific users.
See [HADOOP-18091](https://issues.apache.org/jira/browse/HADOOP-18091) _S3A auditing leaks memory through ThreadLocal references_.

To avoid these memory leaks, auditing was disabled by default in the hadoop 3.3.2 release.

As these memory leaks have now been fixed, auditing has been re-enabled.

To disable it, set `fs.s3a.audit.enabled` to `false`.

## Auditing workflow

1. An _Auditor Service_ can be instantiated for each S3A FileSystem instance,
   created during FS initialization and closed when the FS instance is closed.
1. The S3A FS will request from the Auditor Service an _Audit Span_ for each Hadoop FileSystem API call.
1. The audit span will have callbacks invoked during each of the S3 operations
   invoked during the execution of the API call, *from within the AWS SDK*
1. This allows the Auditor Service to log requests made and associate with users and operations.
1. And/or reject operations.
1. The bundled "Logging Auditor" logs operations and attaches information about calls to the HTTP Referrer header.
1. So aiding debugging of issues related to performance, bucket load, S3 costs...etc.

Thus: an Auditor Service can be plugged in to provide (best-effort) auditing as well
as hinted allow/deny security.

* Why best effort: coverage is not complete. See limitations below.
* Why "hinted" security? Any custom code running in the JVM could retrieve the AWS
  credential chain and so bypass this auditing mechanism.

## Limitations

This is not a means of controlling access to S3 resources. It is a best-effort
attempt at supporting logging of FileSystem operations API calls, and, in
particular, correlating S3 multiple object requests with a single FS API call,
ideally even identifying the process/job generating load.

* Low-level code using public S3A methods intended only for internal use may not
  create spans.
* Code which asks for the AWS S3 client may bypass span creation.
* Application code can also create a new S3 client (reusing any existing
  credentials)
  and so have unaudited access to S3.
* There's no tie-up with OpenTelemetry.
* Uploads and copy operations through the TransferManager do not pick up an
  active span because work is executed in threads which the S3A code cannot
  update.
* There's a limit to how long an http referer header can be; operations on long
  paths may be incompletely logged.

## Using Auditing

Auditing is disabled by default.
When auditing enabled, a Logging Auditor will annotate the S3 logs through a custom
HTTP Referrer header in requests made to S3.
Other auditor classes may be used instead.

### Auditor Options

| Option | Meaning | Default Value |
|--------|---------|---------------|
| `fs.s3a.audit.enabled` | Is auditing enabled? | `true` |
| `fs.s3a.audit.service.classname` | Auditor classname | `org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor` |
| `fs.s3a.audit.request.handlers` | List of extra subclasses of AWS SDK RequestHandler2 to include in handler chain | `""` |
| `fs.s3a.audit.referrer.enabled` | Logging auditor to publish the audit information in the HTTP Referrer header | `true` |
| `fs.s3a.audit.referrer.filter` | List of audit fields to filter | `""` |
| `fs.s3a.audit.reject.out.of.span.operations` | Auditor to reject operations "outside of a span" | `false` |


### Disabling Auditing.

In this release of Hadoop, auditing is disabled.

This can be explicitly set globally or for specific buckets

```xml
<property>
  <name>fs.s3a.audit.enabled</name>
  <value>false</value>
</property>
```

Specific buckets can have auditing disabled, even when it is enabled globally.

```xml
<property>
  <name>fs.s3a.bucket.landsat-pds.audit.enabled</name>
  <value>false</value>
  <description>Do not audit landsat bucket operations</description>
</property>
```

### Auditing with the Logging Auditor

The "Logging Auditor" is the default auditor.
It provides two forms of logging

1. Logging of operations in the client via the active SLF4J imolementation.
1. Dynamic generation of the HTTP Referrer header for S3 requests.

The Logging Auditor is enabled by providing its classname in the option
`fs.s3a.audit.service.classname`.

```xml
<property>
  <name>fs.s3a.audit.enabled</name>
  <value>true</value>
</property>

<property>
  <name>fs.s3a.audit.service.classname</name>
  <value>org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor</value>
</property>
```

To print auditing events in the local client logs, set the associated Log4J log
to log at debug:

```
# Auditing
log4j.logger.org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor=DEBUG
```


### Integration with S3 Server Access Logging

An AWS S3 bucket can be configured to store logs of all HTTP requests made of a bucket
into a different S3 bucket,
[S3 Server Access Logging](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html)
In the logging auditor the HTTP `referer` field of every AWS S3 request is built
up into a URL which provides context and span information. As this field is
saved in the S3 logs, if S3 bucket logging is enabled, the logs will be able to
correlate access by S3 clients to the actual operations taking place.

Note: this logging is described as "Best Effort". There's no guarantee as to
when logs arrive.

### Rejecting out-of-span operations

The logging auditor can be configured to raise an exception whenever
a request is made to S3 outside an audited span -that is: the thread
interacting with S3 through the `S3AFileSystem` instance which created
the auditor does not have any span activated.

This is primarily for development, as it can be used to guarantee
spans are being entered through the public API calls.

```xml
<property>
  <name>fs.s3a.audit.reject.out.of.span.operations</name>
  <value>true</value>
</property>
```

This rejection process is disabled for some AWS S3 Request classes,
which are created within the AWS SDK as part of larger operations
and for which spans cannot be attached.

| AWS Request Always allowed | Reason |
|----------------------------|--------|
| `GetBucketLocationRequest` | Used in AWS SDK to determine S3 endpoint |
| `CopyPartRequest` | Used in AWS SDK during copy operations |
| `CompleteMultipartUploadRequest` | Used in AWS SDK to complete copy operations |

The request to initiate a copy/multipart upload is always audited,
therefore the auditing process does have coverage of rename and multipart
IO. However, the AWS S3 logs will not include full trace information
in the referrer header of the associated copy/complete calls.


## Auditing and the HTTP Referrer header

The HTTP referrer header is attached by the logging auditor.
If the S3 Bucket is configured to log requests to another bucket, then these logs
entries will include the audit information _as the referrer_.

This can be parsed (consult AWS documentation for a regular expression)
and the http referrer header extracted.

```
https://audit.example.org/hadoop/1/op_rename/3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3/
    ?op=op_rename
    &p1=s3a://alice-london/path1
    &pr=alice
    &p2=s3a://alice-london/path2
    &ps=235865a0-d399-4696-9978-64568db1b51c
    &ks=5
    &id=3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3
    &t0=12
    &fs=af5943a9-b6f6-4eec-9c58-008982fc492a
    &t1=12
    &ts=1617116985923
```

Here are the fields which may be found in a request.
If any of the field values were `null`, the field is omitted.

| Name | Meaning | Example |
|------|---------|---------|
| `cm` | Command | `S3GuardTool$BucketInfo` |
| `fs` | FileSystem ID | `af5943a9-b6f6-4eec-9c58-008982fc492a` |
| `id` | Span ID | `3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3` |
| `ji` | Job ID  (S3A committer)| `(Generated by query engine)` |
| `op` | Filesystem API call | `op_rename` |
| `p1` | Path 1 of operation | `s3a://alice-london/path1` |
| `p2` | Path 2 of operation | `s3a://alice-london/path2` |
| `pr` | Principal | `alice` |
| `ps` | Unique process UUID | `235865a0-d399-4696-9978-64568db1b51c` |
| `rg` | GET request range | `100-200` |
| `ta` | Task Attempt ID (S3A committer) | |
| `t0` | Thread 0: thread span was created in | `100` |
| `t1` | Thread 1: thread this operation was executed in | `200` |
| `ts` | Timestamp (UTC epoch millis) | `1617116985923` |
| `ks` | Key size (num of files) to delete as part of the given request (applicable to delete and rename ops) | `5` |

_Notes_

* Thread IDs are from the current thread in the JVM, so can be compared to those in`````````
  Log4J logs. They are never unique.
* Task Attempt/Job IDs are only ever set during operations involving the S3A committers, specifically
  all operations excecuted by the committer.
  Operations executed in the same thread as the committer's instantiation _may_ also report the
  IDs, even if they are unrelated to the actual task. Consider them "best effort".

```java
Long.toString(Thread.currentThread().getId())
```

When `t0` and `t1` are different it means that the span
has been handed off to another thread for work on
behalf of the original operation.
This can be correlated with log entries on the client
to isolate work to specific threads.

### Limitations of the HTTP Referrer header

There is a size limit on the length of the header;
operations on long paths may exceed it.
In such situations the audit log is incomplete.

This is why the span ID is always passed in as part of the URL,
rather than just an HTTP query parameter: even if
the header is chopped, the span ID will always be present.

## Privacy Implications of HTTP Referrer auditing

When the S3A client makes requests of an S3 bucket, the auditor
adds span information to the header, which is then
stored in the logs

If the S3 bucket is owned by the same organization as the client,
this span information is internal to the organization.

If the S3 bucket is owned/managed by a different entity,
then the span information is visible in any S3 bucket logs
collected by that entity. This includes the principal name
and the command executed if the application is launched via the `Tools` or
service launcher APIs.

Sharing this information can be disabled by either filtering specific
headers, or by explicitly disabling referrer header generation entirely.

Note: even when the HTTP Referrer is disabled by or the principal filtered,
AWS S3 logs include ARN of the user or IAM role making the request.

### Filtering Referrer headers

Specific fields can be filtered from the referrer header, and so are not
included in the S3A logs.

```xml
<property>
  <name>fs.s3a.audit.referrer.filter</name>
  <value>pr, cm</value>
  <description>Strip out principal and command from referrer headers</description>
</property>
```

### Disabling Referrer headers

The logging auditor can be configured to not add the referrer header
by setting the option `fs.s3a.audit.referrer.enabled` to `false`,
either globally or for specific buckets:

```xml

<property>
  <name>fs.s3a.audit.referrer.enabled</name>
  <value>false</value>
  <description>Disable referrer for all buckets</description>
</property>

<property>
  <name>fs.s3a.bucket.landsat-pds.audit.referrer.enabled</name>
  <value>false</value>
  <description>Do not add the referrer header to landsat operations</description>
</property>
```

## Collecting AWS S3 Logs for Analysis

The S3 Bucket(s) must be set up for
[Server Access Logging](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html).

This will tell AWS S3 to collect access logs of all HTTP requests
and store them in a different bucket in the same region.
The logs arrive as files containing a few seconds worth
of log data, stored under the configured path.

### Enabling logging: Source bucket

1. Create a separate bucket for logs in the same region, if you do not already have one.
1. In the S3 console, locate the bucket you wish to act as a source for logs,
   and go to the "properties".
1. Scroll down to "Server access logging"
1. Select "edit" and then enable logging, entering a path in a nearby bucket for logs.
   (Tip: for ease of logging multiple buckets to the same log bucket, use a prefix like
   `logs/$BUCKET/log-` to isolate different bucket's logs.
   For example, the path log data from `dev data london` could be
   `s3://london-log-bucket/logs/dev-data-lon/log-`
1. Save this.

There's a lag of about an hour between S3 requests being made and the logs
appearing; don't worry during setup if things do not appear to be working.
Enable the log, work with the bucket through the "hadoop fs" command line, wait
an hour, then go and look in the log bucket for the entries.
The log filename includes the time at which these logs
began

### Keeping costs down by deleting old logs.

As logs are stored in an S3 bucket, they too run up charges.
Keep costs down by deleting logs after a period of time, and/or
set up a workflow to load and coalesce log entries into a compressed
format and larger files.

It is straightforward to set up a rule to automatically delete old log files.

1. In the S3 console, bring up the bucket which is the destination for the logs,
   e.g. `london-log-bucket`.
1. Go to the "Management" tab.
1. Add a lifecycle rule (alongside the "abort pending uploads" rule you should already have).
1. Add rule name "Delete old log files".
1. Select "Limit the scope".
1. Add the prefix `logs/` to have it delete all logs of all buckets. 
   Important: you _must not_ have any leading "/", such as `/logs/` -there will be no
   match and the rule will not work.
1. In "Lifecycle rule actions", select "Expire current versions"
   This will delete log entries.
1. In "Expire current versions of objects", set the number of days to keep
   log entries.
1. Finish by pressing the "Create Rule" button 

Keep an eye on the bucket to make sure the deletion is working; it's easy to
make an error in the prefix, and as logs will be created without limit,
costs will ramp up.

## Parsing AWS S3 Logs to extract the referrer header

The [AWS S3 Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html)
covers the log format and includes a hive external table declaration to work with it.

The Java pattern regular expression used in the `hadoop-aws` test suites to
extract headers is defined as:

```
(?<owner>[^ ]*) (?<bucket>[^ ]*) (?<timestamp>\[(.*?)\]) (?<remoteip>[^ ]*) (?<requester>[^ ]*) (?<requestid>[^ ]*) (?<operation>[^ ]*) (?<key>[^ ]*) (?<requesturi>(-|"[^"]*")) (?<http>(-|[0-9]*)) (?<awserrorcode>[^ ]*) (?<bytessent>[^ ]*) (?<objectsize>[^ ]*) (?<totaltime>[^ ]*) (?<turnaroundtime>[^ ]*) (?<referrer>(-|"[^"]*")) (?<useragent>(-|"[^"]*")) (?<version>[^ ]*) (?<hostid>[^ ]*) (?<sigv>[^ ]*) (?<cypher>[^ ]*) (?<auth>[^ ]*) (?<endpoint>[^ ]*) (?<tls>[^ ]*)*$
```

The class `org.apache.hadoop.fs.s3a.audit.S3LogParser` provides this pattern
as well as constants for each group. It is declared as `Public/Unstable`.

## Debugging

The `org.apache.hadoop.fs.s3a.audit` log context contains logs for the different
components implementing auditing.

Logging of requests audited with the `LoggingAuditService` can be enabled by
setting that log to debug.

```
# Log before a request is made to S3
log4j.logger.org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor=DEBUG
```

This adds one log line per request -and does provide some insight into
communications between the S3A client and AWS S3.

For low-level debugging of the Auditing system, such as when spans are
entered and exited, set the log to `TRACE`:

```
# log request creation, span lifecycle and other low-level details
log4j.logger.org.apache.hadoop.fs.s3a.audit=TRACE
```

This is very noisy and not recommended in normal operation.

## Integration with S3A Committers

Work submitted through the S3A committer will have the job (query) ID associated
with S3 operations taking place against all S3A filesystems in that thread.

For this to be useful, the work performed in a task MUST be in the same thread
which called `jobSetup()` or `taskSetup()` on the committer.

