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

# S3 Select

**Experimental Feature**

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

S3 Select is a feature for Amazon S3 introduced in April 2018. It allows for
SQL-like SELECT expressions to be applied to files in some structured
formats, including CSV and JSON.

By performing the SELECT operation in the S3 storage infrastructure, the
bandwidth requirements between S3 and the hosts making the request can be reduced.
Along with latency, this bandwidth is often the limiting factor in processing
data from S3, especially with larger CSV and JSON datasets.

Apache Hadoop's S3A Client has experimental support for this feature, with the
following warnings:

* The filtering is being done in S3 itself. If the source files cannot be parsed,
that's not something which can be fixed in Hadoop or layers above.
* It is not currently supported by third party S3 implementations, and unlikely
to be supported in future (the bandwidth constraints are less, so the value
less compelling).
* Performance *appears* best when the selection restricts the number of fields,
and projected columns: the less data returned, the faster the response.
* High-level support in tools such as Apache Hive and Spark will also be
evolving. Nobody has ever written CSV connectors with predicate pushdown before.
* The standard `FileInputFormat` readers of text (`LineRecordReader` etc) fail when the
amount of data returned is less than they expect. For this reason, S3 Select
*MUST NOT BE USED IN PRODUCTION MAPREDUCE JOBS*.

## Currently Implemented Features

* Ability to issue select queries on the command line.
* Proof of concept support in MapReduce queries.
* CSV input with/without compression.
* CSV output.

## Currently Unsupported

* Production-ready integration with the standard FileInputFormat and
Record Readers.
* Non-CSV output.
* JSON source files.
* Structured source file formats like Apache Parquet.
It's better here to directly use the Apache Spark, Hive, Impala, Flink or
similar, which all use the latest ASF-supported libraries.

## Enabling/Disabling S3 Select

S3 Select is enabled by default:

```xml
<property>
  <name>fs.s3a.select.enabled</name>
  <value>true</value>
  <description>Is S3 Select enabled?</description>
</property>
```

To disable it, set the option `fs.s3a.select.enabled` to `false`.

To probe to see if a FileSystem instance implements it,
`StreamCapabilities.hasCapability("s3a:fs.s3a.select.sql")` will be true
on an instance of the S3AFileSystem class if
the version of Hadoop supports S3 Select, *and* it is enabled for that
specific instance.

If this call returns false, then S3 Select calls will fail.

Rather than cast a filesystem to the `S3AFileSystem` class, cast it to
`org.apache.hadoop.fs.StreamCapabilities`; a class which was added in Hadoop 2.9.
This should result in less brittle code -and there is no need to have the
`hadoop-aws` JAR on the classpath at compile time.

```java
/**
 * Probe for a filesystem instance supporting S3 Select.
 * @param fs filesystem
 * @return true if the filesystem supports S3 Select.
 */
public static boolean hasS3SelectCapability(final FileSystem fs) {
  return (fs instanceof StreamCapabilities)
    && ((StreamCapabilities)fs).hasCapability("s3a:fs.s3a.select.sql");
}
```

## Making S3 Select calls through the Hadoop APIs

Applications can issue select queries through the Hadoop FileSystem/FileContext
 APIs via the asynchronous `openFile()` call added in Hadoop 3.3.

Use the `FileSystem.openFile(path)` or `FileContext.openFile(path)` methods
command to get a builder class for the open operations, then
set the mandatory s3 select options though multiple `must()` parameters.

```java
FileSystem.FSDataInputStreamBuilder builder =
    filesystem.openFile("s3a://bucket/path-to-file.csv")
        .must("fs.s3a.select.sql",
            "SELECT * FROM S3OBJECT s WHERE s.\"odd\" = `TRUE`")
        .must("fs.s3a.select.input.format", "CSV")
        .must("fs.s3a.select.input.compression", "NONE")
        .must("fs.s3a.select.input.csv.header", "use")
        .must("fs.s3a.select.output.format", "CSV")
        .must("fs.s3a.select.output.csv.field.delimiter", "\t")
        .must("fs.s3a.select.output.csv.quote.character", "\"")
        .must("fs.s3a.select.output.csv.quote.fields", "asneeded") ;
CompletableFuture<FSDataInputStream> future = builder.build();
try (FSDataInputStream select = future.get()) {
    // process the output
    byte[] bytes = new byte[8192];
    int actual = select.read(bytes);
}
```

When the Builder's `build()` call is made, if the FileSystem/FileContext
instance does not recognize any of the mandatory options it will fail.
The S3A connector does recognize them, and, if S3 Select has not been
disabled, will issue the Select query against the object store.

If the S3A connector has S3 Select disabled, it will fail with
an `UnsupportedOperationException`.

The `build()` call returns a `CompletableFuture<FSDataInputStream>`.
This future retrieves the result of the select call, which is executed
asynchronously in the S3A FileSystem instance's executor pool.

Errors in the SQL, missing file, permission failures and suchlike
will surface when the future is evaluated, *not the build call*.

In the returned stream, seeking and positioned reads do not work as usual,
because there are no absolute positions in the file to seek to.

1. backwards `seek()` calls will raise a `PathIOException`.
1. Forwards `seek()` calls will succeed, but only by reading and discarding
bytes. This will be slow.
1. All positioned read operations when the offset of the read is not the current position
will raise a `PathIOException`.
1. Positioned read operations when the offset of the read *is* current position
   will succeed, but the position of the stream (as returned by `getPos()`)
   will be updated. This is not compliant with the filesystem specification.

This is all done as a best-effort attempt to support existing code which
often uses `seek()` to move forward in a split file after opening,
or does a series of positioned read calls.


### seek() behavior on `SelectInputStream`

The returned stream, of type `org.apache.hadoop.fs.s3a.select.SelectInputStream`,
only supports forward `seek()` operations.

A zero-byte seek operation is always valid:

```java
stream.seek(stream.getPos());
```

A negative seek operation will always fail:

```java
stream.seek(stream.getPos() - offset);
```

A forward seek operation will work, provided the final position is less
than the total length of the stream:

```java
stream.seek(stream.getPos() + offset);
```

If it is past the end of the file, an `EOFException` is raised.

*Important* Forward seeks are implemented by reading and discarding the
contents of the stream. The bigger the forward seek, the more data is thrown
away, the longer the operation takes. And, if the data is being paid for over
a long-haul S3 connection. the more the seek costs.

Calling `seek()` on a `SelectInputStream` should only be used with care.

The feature has been implemented for splittable queries across Selected data,
where the initial read starts with a `seek()` to the offset. However, for
reasons covered below, a codec should be used to declare the input unsplittable.

## Use with third-party S3-compatible object stores.

Third party object stores do not, at the time of writing, support S3 Select.
S3 Select operations against such stores will fail, presumably with a store-specific
error code.

To avoid problems, disable S3 Select entirely:

```xml
<property>
  <name>fs.s3a.select.enabled</name>
  <value>false</value>
</property>
```

This guarantees that the `hasCapability()` check will fail immediately,
rather than delaying the failure until an SQL query is attempted.

## Selecting data from the command line: `hadoop s3guard select`

The `s3guard select` command allows direct select statements to be made
of a path.

Usage:

```bash
hadoop s3guard select [OPTIONS] \
 [-limit rows] \
 [-header (use|none|ignore)] \
 [-out file] \
 [-compression (gzip|none)] \
 [-expected rows]
 [-inputformat csv]
 [-outputformat csv]
  <PATH> <SELECT QUERY>
```

The output is printed, followed by some summary statistics, unless the `-out`
option is used to declare a destination file. In this mode
status will be logged to the console, but the output of the query will be
saved directly to the output file.

### Example 1

Read the first 100 rows of the landsat dataset where cloud cover is zero:

```bash
hadoop s3guard select -header use -compression gzip -limit 100  \
  s3a://landsat-pds/scene_list.gz \
  "SELECT * FROM S3OBJECT s WHERE s.cloudCover = '0.0'"
```

### Example 2

Return the `entityId` column for all rows in the dataset where the cloud
cover was "0.0", and save it to the file `output.csv`:

```bash
hadoop s3guard select -header use -out s3a://mybucket/output.csv \
  -compression gzip \
  s3a://landsat-pds/scene_list.gz \
  "SELECT s.entityId from S3OBJECT s WHERE s.cloudCover = '0.0'"
```

This file will:

1. Be UTF-8 encoded.
1. Have quotes on all columns returned.
1. Use commas as a separator.
1. Not have any header.

The output can be saved to a file with the `-out` option. Note also that
`-D key=value` settings can be used to control the operation, if placed after
the `s3guard` command and before `select`


```bash
hadoop s3guard \
  -D  s.s3a.select.output.csv.quote.fields=asneeded \
  select \
  -header use \
  -compression gzip \
  -limit 500 \
  -inputformat csv \
  -outputformat csv \
  -out s3a://hwdev-steve-new/output.csv \
  s3a://landsat-pds/scene_list.gz \
  "SELECT s.entityId from S3OBJECT s WHERE s.cloudCover = '0.0'"
```


## Use in MR/Analytics queries: Work in Progress

S3 Select support in analytics queries is a work in progress. It does
not work reliably with large source files where the work is split up.

As a proof of concept *only*, S3 Select queries can be made through
MapReduce jobs which use any Hadoop `RecordReader`
class which uses the new `openFile()` API.

Currently this consists of the following MRv2 readers.

```
org.apache.hadoop.mapreduce.lib.input.LineRecordReader
org.apache.hadoop.mapreduce.lib.input.FixedLengthRecordReader
```

And a limited number of the MRv1 record readers:

```
org.apache.hadoop.mapred.LineRecordReader
```

All of these readers use the new API and can be have its optional/mandatory
options set via the `JobConf` used when creating/configuring the reader.

These readers are instantiated within input formats; the following
formats therefore support S3 Select.

```
org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat
org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
org.apache.hadoop.mapreduce.lib.input.TextInputFormat
org.apache.hadoop.mapred.KeyValueTextInputFormat
org.apache.hadoop.mapred.TextInputFormat
org.apache.hadoop.mapred.lib.NLineInputFormat
```

All `JobConf` options which begin with the prefix `mapreduce.job.input.file.option.`
will have that prefix stripped and the remainder used as the name for an option
when opening the file.

All `JobConf` options which being with the prefix `mapreduce.job.input.file.must.`
will be converted into mandatory options.

To use an S3 Select call, set the following options

```
mapreduce.job.input.file.must.fs.s3a.select.sql = <SQL STATEMENT>
mapreduce.job.input.file.must.fs.s3a.select.input.format = CSV
mapreduce.job.input.file.must.fs.s3a.select.output.format = CSV
```

Further options may be set to tune the behaviour, for example:

```java
jobConf.set("mapreduce.job.input.file.must.fs.s3a.select.input.csv.header", "use");
```

*Note* How to tell if a reader has migrated to the new `openFile()` builder
API:

Set a mandatory option which is not known; if the job does not fail then
an old reader is being used.

```java
jobConf.set("mapreduce.job.input.file.must.unknown.option", "anything");
```


### Querying Compressed objects

S3 Select queries can be made against gzipped source files; the S3A input
stream receives the output in text format, rather than as a (re)compressed
stream.

To read a gzip file, set `fs.s3a.select.input.compression` to `gzip`.

```java
jobConf.set("mapreduce.job.input.file.must.fs.s3a.select.input.compression",
  "gzip");
```


Most of the Hadoop RecordReader classes automatically choose a decompressor
based on the extension of the source file. This causes problems when
reading `.gz` files, because S3 Select is automatically decompressing and
returning csv-formatted text.

By default, a query across gzipped files will fail with the error
"IOException: not a gzip file"

To avoid this problem, declare that the job should switch to the
"Passthrough Codec" for all files with a ".gz" extension:

```java
jobConf.set("io.compression.codecs",
    "org.apache.hadoop.io.compress.PassthroughCodec");
jobConf.set("io.compress.passthrough.extension", ".gz");
```

Obviously, this breaks normal `.gz` decompression: only set it on S3 Select
jobs.

## S3 Select configuration options.

Consult the javadocs for `org.apache.hadoop.fs.s3a.select.SelectConstants`.

The listed options can be set in `core-site.xml`, supported by S3A per-bucket
configuration, and can be set programmatically on the `Configuration` object
use to configure a new filesystem instance.

Any of these options can be set in the builder returned by the `openFile()` call
—simply set them through a chain of `builder.must()` operations.

```xml
<property>
  <name>fs.s3a.select.input.format</name>
  <value>csv</value>
  <description>Input format</description>
</property>

<property>
  <name>fs.s3a.select.output.format</name>
  <value>csv</value>
  <description>Output format</description>
</property>

<property>
  <name>fs.s3a.select.input.csv.comment.marker</name>
  <value>#</value>
  <description>In S3 Select queries: the marker for comment lines in CSV files</description>
</property>

<property>
  <name>fs.s3a.select.input.csv.record.delimiter</name>
  <value>\n</value>
  <description>In S3 Select queries over CSV files: the record delimiter.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.field.delimiter</name>
  <value>,</value>
  <description>In S3 Select queries over CSV files: the field delimiter.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.quote.character</name>
  <value>"</value>
  <description>In S3 Select queries over CSV files: quote character.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.quote.escape.character</name>
  <value>\\</value>
  <description>In S3 Select queries over CSV files: quote escape character.
    \t is remapped to the TAB character, \r to CR \n to newline. \\ to \
    and \" to "
  </description>
</property>

<property>
  <name>fs.s3a.select.input.csv.header</name>
  <value>none</value>
  <description>In S3 Select queries over CSV files: what is the role of the header? One of "none", "ignore" and "use"</description>
</property>

<property>
  <name>fs.s3a.select.input.compression</name>
  <value>none</value>
  <description>In S3 Select queries, the source compression
    algorithm. One of: "none" and "gzip"</description>
</property>

<property>
  <name>fs.s3a.select.output.csv.quote.fields</name>
  <value>always</value>
  <description>
    In S3 Select queries: should fields in generated CSV Files be quoted?
    One of: "always", "asneeded".
  </description>
</property>

<property>
  <name>fs.s3a.select.output.csv.quote.character</name>
  <value>"</value>
  <description>
    In S3 Select queries: the quote character for generated CSV Files.
  </description>
</property>

<property>
  <name>fs.s3a.select.output.csv.quote.escape.character</name>
  <value>\\</value>
  <description>
    In S3 Select queries: the quote escape character for generated CSV Files.
  </description>
</property>

<property>
  <name>fs.s3a.select.output.csv.record.delimiter</name>
  <value>\n</value>
  <description>
    In S3 Select queries: the record delimiter for generated CSV Files.
  </description>
</property>

<property>
  <name>fs.s3a.select.output.csv.field.delimiter</name>
  <value>,</value>
  <description>
    In S3 Select queries: the field delimiter for generated CSV Files.
  </description>
</property>

<property>
  <name>fs.s3a.select.errors.include.sql</name>
  <value>false</value>
  <description>
    Include the SQL statement in errors: this is useful for development but
    may leak security and Personally Identifying Information in production,
    so must be disabled there.
  </description>
</property>
```

## Security and Privacy

SQL Injection attacks are the classic attack on data.
Because S3 Select is a read-only API, the classic ["Bobby Tables"](https://xkcd.com/327/)
attack to gain write access isn't going to work. Even so: sanitize your inputs.

CSV does have security issues of its own, specifically:

*Excel and other spreadsheets may interpret some fields beginning with special
characters as formula, and execute them*

S3 Select does not appear vulnerable to this, but in workflows where untrusted
data eventually ends up in a spreadsheet (including Google Document spreadsheets),
the data should be sanitized/audited first. There is no support for
such sanitization in S3 Select or in the S3A connector.

Logging Select statements may expose secrets if they are in the statement.
Even if they are just logged, this may potentially leak Personally Identifying
Information as covered in the EU GDPR legislation and equivalents.

For both privacy and security reasons, SQL statements are not included
in exception strings by default, nor logged at INFO level.

To enable them, set `fs.s3a.select.errors.include.sql` to `true`, either in the
site/application configuration, or as an option in the builder for a
single request. When set, the request will also be logged at
the INFO level of the log `org.apache.hadoop.fs.s3a.select.SelectBinding`.

Personal Identifiable Information is not printed in the AWS S3 logs.
Those logs contain only the SQL keywords from the query planner.
All column names and literals are masked. Following is a sample log example:

*Query:*

```sql
SELECT * FROM S3OBJECT s;
```

*Log:*

```sql
select (project (list (project_all))) (from (as str0 (id str1 case_insensitive)))
```

Note also that:

1. Debug-level Hadoop logs for the module `org.apache.hadoop.fs.s3a` and other
components's debug logs may also log the SQL statements (e.g. aws-sdk HTTP logs).

The best practise here is: only enable SQL in exceptions while developing
SQL queries, especially in an application/notebook where the exception
text is a lot easier to see than the application logs.

In production: don't log or report. If you do, all logs and output must be
considered sensitive from security and privacy perspectives.

The `hadoop s3guard select` command does enable the logging, so
can be used as an initial place to experiment with the SQL syntax.
Rationale: if you are constructing SQL queries on the command line,
your shell history is already tainted with the query.

### Links

* [CVE-2014-3524](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2014-3524).
* [The Absurdly Underestimated Dangers of CSV Injection](http://georgemauer.net/2017/10/07/csv-injection.html).
* [Comma Separated Vulnerabilities](https://www.contextis.com/blog/comma-separated-vulnerabilities).

### SQL Syntax

The SQL Syntax directly supported by the AWS S3 Select API is [documented by
Amazon](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html).

* Use single quotes for all constants, not double quotes.
* All CSV column values are strings unless cast to a type
* Simple `SELECT` calls, no `JOIN`.

### CSV formats

"CSV" is less a format, more "a term meaning the data is in some nonstandard
line-by-line" text file, and there are even "multiline CSV files".

S3 Select only supports a subset of the loose "CSV" concept, as covered in
the AWS documentation. There are also limits on how many columns and how
large a single line may be.

The specific quotation character, field and record delimiters, comments and escape
characters can be configured in the Hadoop configuration.

### Consistency, Concurrency and Error handling

**Consistency**

Since November 2020, AWS S3 has been fully consistent.
This also applies to S3 Select.
We do not know what happens if an object is overwritten while a query is active.


**Concurrency**

The outcome of what happens when source file is overwritten while the result of
a select call is overwritten is undefined.

The input stream returned by the operation is *NOT THREAD SAFE*.

**Error Handling**

If an attempt to issue an S3 select call fails, the S3A connector will
reissue the request if-and-only-if it believes a retry may succeed.
That is: it considers the operation to be idempotent and if the failure is
considered to be a recoverable connectivity problem or a server-side rejection
which can be retried (500, 503).

If an attempt to read data from an S3 select stream (`org.apache.hadoop.fs.s3a.select.SelectInputStream)` fails partway through the read, *no attempt is made to retry the operation*

In contrast, the normal S3A input stream tries to recover from (possibly transient)
failures by attempting to reopen the file.


## Performance

The select operation is best when the least amount of data is returned by
the query, as this reduces the amount of data downloaded.

* Limit the number of columns projected to only those needed.
* Use `LIMIT` to set an upper limit on the rows read, rather than implementing
a row counter in application code and closing the stream when reached.
This avoids having to abort the HTTPS connection and negotiate a new one
on the next S3 request.

The select call itself can be slow, especially when the source is a multi-MB
compressed file with aggressive filtering in the `WHERE` clause.
Assumption: the select query starts at row 1 and scans through each row,
and does not return data until it has matched one or more rows.

If the asynchronous nature of the `openFile().build().get()` sequence
can be taken advantage of, by performing other work before or in parallel
to the `get()` call: do it.

## Troubleshooting

Getting S3 Select code to work is hard, though those knowledgeable in SQL
will find it easier.

Problems can be split into:

1. Basic configuration of the client to issue the query.
1. Bad SQL select syntax and grammar.
1. Datatype casting issues
1. Bad records/data in source files.
1. Failure to configure MR jobs to work correctly.
1. Failure of MR jobs due to

The exceptions here are all based on the experience during writing tests;
more may surface with broader use.

All failures other than network errors on request initialization are considered
unrecoverable and will not be reattempted.

As parse-time errors always state the line and column of an error, you can
simplify debugging by breaking a SQL statement across lines, e.g.

```java
String sql = "SELECT\n"
    + "s.entityId \n"
    + "FROM " + "S3OBJECT s WHERE\n"
    + "s.\"cloudCover\" = '100.0'\n"
    + " LIMIT 100";
```
Now if the error is declared as "line 4", it will be on the select conditions;
the column offset will begin from the first character on that row.

The SQL Statements issued are only included in exceptions if `fs.s3a.select.errors.include.sql`
is explicitly set to true. This can be done in an application during development,
or in a `openFile()` option parameter. This should only be done during development,
to reduce the risk of logging security or privacy information.


### "mid-query" failures on large datasets

S3 Select returns paged results; the source file is _not_ filtered in
one go in the initial request.

This means that errors related to the content of the data (type casting, etc)
may only surface partway through the read. The errors reported in such a
case may be different than those raised on reading the first page of data,
where it will happen earlier on in the read process.

### External Resources on for troubleshooting

See:

* [SELECT Command Reference](https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html)
* [SELECT Object Content](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html)

### IOException: "not a gzip file"

This surfaces when trying to read in data from a `.gz` source file through an MR
or other analytics query, and the gzip codec has tried to parse it.

```
java.io.IOException: not a gzip file
at org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.processBasicHeader(BuiltInGzipDecompressor.java:496)
at org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.executeHeaderState(BuiltInGzipDecompressor.java:257)
at org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor.decompress(BuiltInGzipDecompressor.java:186)
at org.apache.hadoop.io.compress.DecompressorStream.decompress(DecompressorStream.java:111)
at org.apache.hadoop.io.compress.DecompressorStream.read(DecompressorStream.java:105)
at java.io.InputStream.read(InputStream.java:101)
at org.apache.hadoop.util.LineReader.fillBuffer(LineReader.java:182)
at org.apache.hadoop.util.LineReader.readCustomLine(LineReader.java:306)
at org.apache.hadoop.util.LineReader.readLine(LineReader.java:174)
at org.apache.hadoop.mapreduce.lib.input.LineRecordReader.skipUtfByteOrderMark(LineRecordReader.java:158)
at org.apache.hadoop.mapreduce.lib.input.LineRecordReader.nextKeyValue(LineRecordReader.java:198)
```

The underlying problem is that the gzip decompressor is automatically enabled
when the source file ends with the ".gz" extension. Because S3 Select
returns decompressed data, the codec fails.

The workaround here is to declare that the job should add the "Passthrough Codec"
to its list of known decompressors, and that this codec should declare the
file format it supports to be ".gz".

```
io.compression.codecs = org.apache.hadoop.io.compress.PassthroughCodec
io.compress.passthrough.extension = .gz
```

### AWSBadRequestException `InvalidColumnIndex`


Your SQL is wrong and the element at fault is considered an unknown column
name.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
  Select: SELECT * FROM S3OBJECT WHERE odd = true on test/testSelectOddLines.csv:
  com.amazonaws.services.s3.model.AmazonS3Exception:
  The column index at line 1, column 30 is invalid.
  Please check the service documentation and try again.
  (Service: Amazon S3; Status Code: 400; Error Code: InvalidColumnIndex;
```

Here it's the first line of the query, column 30. Paste the query
into an editor and position yourself on the line and column at fault.

```sql
SELECT * FROM S3OBJECT WHERE odd = true
                             ^ HERE
```

Another example:

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Select:
SELECT * FROM S3OBJECT s WHERE s._1 = "true" on test/testSelectOddLines.csv:
  com.amazonaws.services.s3.model.AmazonS3Exception:
  The column index at line 1, column 39 is invalid.
  Please check the service documentation and try again.
  (Service: Amazon S3; Status Code: 400;
  Error Code: InvalidColumnIndex;
```

Here it is because strings must be single quoted, not double quoted.

```sql
SELECT * FROM S3OBJECT s WHERE s._1 = "true"
                                      ^ HERE
```

S3 select uses double quotes to wrap column names, interprets the string
as column "true", and fails with a non-intuitive message.

*Tip*: look for the element at fault and treat the `InvalidColumnIndex`
message as a parse-time message, rather than the definitive root
cause of the problem.

### AWSBadRequestException `ParseInvalidPathComponent`

Your SQL is wrong.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s.'odd' is "true" on test/testSelectOddLines.csv
: com.amazonaws.services.s3.model.AmazonS3Exception: Invalid Path component,
  expecting either an IDENTIFIER or STAR, got: LITERAL,at line 1, column 34.
  (Service: Amazon S3; Status Code: 400; Error Code: ParseInvalidPathComponent;

```

```
SELECT * FROM S3OBJECT s WHERE s.'odd' is "true" on test/testSelectOddLines.csv
                                 ^ HERE
```


### AWSBadRequestException  `ParseExpectedTypeName`

Your SQL is still wrong.

```

org.apache.hadoop.fs.s3a.AWSBadRequestException:
 Select: SELECT * FROM S3OBJECT s WHERE s.odd = "true"
on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception
: Expected type name, found QUOTED_IDENTIFIER:'true' at line 1, column 41.
(Service: Amazon S3; Status Code: 400; Error Code: ParseExpectedTypeName;
```

### `ParseUnexpectedToken`

Your SQL is broken.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s.5 = `true` on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception:
Unexpected token found LITERAL:5d-1 at line 1, column 33.
(Service: Amazon S3; Status Code: 400; Error Code: ParseUnexpectedToken;
```
### `ParseUnexpectedOperator`

Your SQL is broken.

```
com.amazonaws.services.s3.model.AmazonS3Exception: Unexpected operator OPERATOR:'%' at line 1, column 45.
(Service: Amazon S3; Status Code: 400;
Error Code: ParseUnexpectedOperator; Request ID: E87F30C57436B459;
S3 Extended Request ID: UBFOIgkQxBBL+bcBFPaZaPBsjdnd8NRz3NFWAgcctqm3n6f7ib9FMOpR+Eu1Cy6cNMYHCpJbYEY
 =:ParseUnexpectedOperator: Unexpected operator OPERATOR:'%' at line 1, column 45.
at java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:357)
at java.util.concurrent.CompletableFuture.get(CompletableFuture.java:1895)
```

### `MissingHeaders`

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Select: SELECT * FROM S3OBJECT s WHERE s."odd" = `true` on test/testSelectOddLines.csv:
com.amazonaws.services.s3.model.AmazonS3Exception:
Some headers in the query are missing from the file.
Please check the file and try again.
(Service: Amazon S3; Status Code: 400; Error Code: MissingHeaders;
```

1. There's a header used in the query which doesn't match any in the document
itself.
1. The header option for the select query is set to "none" or "ignore", and
you are trying to use a header named there.

This can happen if you are trying to use double quotes for constants in the
SQL expression.

```
SELECT * FROM S3OBJECT s WHERE s."odd" = "true" on test/testSelectOddLines.csv:
                                         ^ HERE
```

Double quotes (") may only be used when naming columns; for constants
single quotes are required.

### Method not allowed

```
org.apache.hadoop.fs.s3a.AWSS3IOException: Select on test/testSelectWholeFile:
com.amazonaws.services.s3.model.AmazonS3Exception: The specified method is not
allowed against this resource. (Service: Amazon S3; Status Code: 405;
Error Code: MethodNotAllowed;
```

You are trying to use S3 Select to read data which for some reason
you are not allowed to.

### AWSBadRequestException `InvalidTextEncoding`

The file couldn't be parsed. This can happen if you try to read a `.gz` file
and forget to set the compression in the select request.

That can be done through the `fs.s3a.select.compression` option.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
  Select: '" SELECT * FROM S3OBJECT s WHERE endstation_name = 'Bayswater Road: Hyde Park' "
  on s3a://example/dataset.csv.gz:
  com.amazonaws.services.s3.model.AmazonS3Exception:
   UTF-8 encoding is required. The text encoding error was found near byte 8,192.
    (Service: Amazon S3; Status Code: 400; Error Code: InvalidTextEncoding
```

### AWSBadRequestException  `InvalidCompressionFormat` "GZIP is not applicable to the queried object"

A SELECT call has been made using a compression which doesn't match that of the
source object, such as it being a plain text file.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Select:
 '" SELECT * FROM S3OBJECT s WHERE endstation_name = 'Bayswater Road: Hyde Park' "
  on s3a://example/dataset.csv:
   com.amazonaws.services.s3.model.AmazonS3Exception:
    GZIP is not applicable to the queried object. Please correct the request and try again.
     (Service: Amazon S3; Status Code: 400; Error Code: InvalidCompressionFormat;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:212)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
...
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: GZIP is not applicable to the queried object.
 Please correct the request and try again.
  Service: Amazon S3; Status Code: 400; Error Code: InvalidCompressionFormat;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse
  ...
```


### AWSBadRequestException  `UnsupportedStorageClass`

S3 Select doesn't work with some storage classes like Glacier or Reduced Redundancy.
Make sure you've set `fs.s3a.create.storage.class` to a supported storage class for S3 Select.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
    Select on s3a://example/dataset.csv.gz:
    com.amazonaws.services.s3.model.AmazonS3Exception:
     We do not support REDUCED_REDUNDANCY storage class.
     Please check the service documentation and try again.
     (Service: Amazon S3; Status Code: 400; Error Code: UnsupportedStorageClass
```

### `PathIOException`: "seek() not supported"

The input stream returned by the select call does not support seeking
backwards in the stream.

Similarly, `PositionedReadable` operations will fail when used to read
data any offset other than that of `getPos()`.

```
org.apache.hadoop.fs.PathIOException: `s3a://landsat-pds/landsat.csv.gz': seek() not supported

  at org.apache.hadoop.fs.s3a.select.SelectInputStream.unsupported(SelectInputStream.java:254)
  at org.apache.hadoop.fs.s3a.select.SelectInputStream.seek(SelectInputStream.java:243)
  at org.apache.hadoop.fs.FSDataInputStream.seek(FSDataInputStream.java:66)
```

There is no fix for this. You can move forward in a file using `skip(offset)`;
bear in mind that the return value indicates what offset was skipped -it
may be less than expected.

### `IllegalArgumentException`: "Unknown mandatory key "fs.s3a.select.sql"

The filesystem is not an S3A filesystem, and the s3a select option is not recognized.

```
java.lang.IllegalArgumentException: Unknown mandatory key "fs.s3a.select.sql"
at com.google.common.base.Preconditions.checkArgument(Preconditions.java:88)
at org.apache.hadoop.fs.AbstractFSBuilder.lambda$rejectUnknownMandatoryKeys$0(AbstractFSBuilder.java:331)
at java.lang.Iterable.forEach(Iterable.java:75)
at java.util.Collections$UnmodifiableCollection.forEach(Collections.java:1080)
at org.apache.hadoop.fs.AbstractFSBuilder.rejectUnknownMandatoryKeys(AbstractFSBuilder.java:330)
at org.apache.hadoop.fs.filesystem.openFileWithOptions(FileSystem.java:3541)
at org.apache.hadoop.fs.FileSystem$FSDataInputStreamBuilder.build(FileSystem.java:4442)
```

* Verify that the URL has an "s3a:" prefix.
* If it does, there may be a non-standard S3A implementation, or some
a filtering/relaying class has been placed in front of the S3AFilesystem.

### `IllegalArgumentException`: "Unknown mandatory key in non-select file I/O"

The file options to tune an S3 select call are only valid when a SQL expression
is set in the `fs.s3a.select.sql` option. If not, any such option added as a `must()` value
will fail.

```
java.lang.IllegalArgumentException: Unknown mandatory key for s3a://example/test/testSelectOptionsOnlyOnSelectCalls.csv in non-select file I/O "fs.s3a.select.input.csv.header"

  at com.google.common.base.Preconditions.checkArgument(Preconditions.java:115)
  at org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.lambda$rejectUnknownMandatoryKeys$0(AbstractFSBuilderImpl.java:352)
  at java.lang.Iterable.forEach(Iterable.java:75)
  at java.util.Collections$UnmodifiableCollection.forEach(Collections.java:1080)
  at org.apache.hadoop.fs.impl.AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(AbstractFSBuilderImpl.java:351)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.openFileWithOptions(S3AFileSystem.java:3736)
  at org.apache.hadoop.fs.FileSystem$FSDataInputStreamBuilder.build(FileSystem.java:4471)
```

Requiring these options without providing a SQL query is invariably an error.
Fix: add the SQL statement, or use `opt()` calls to set the option.

If the `fs.s3a.select.sql` option is set, and still a key is rejected, then
either the spelling of the key is wrong, it has leading or trailing spaces,
or it is an option not supported in that specific release of Hadoop.


### PathIOException : "seek() backwards from  not supported"

Backwards seeks in an S3 Select `SelectInputStream` are not supported.

```
org.apache.hadoop.fs.PathIOException: `s3a://landsat-pds/scene_list.gz':
  seek() backwards from 16387 to 0 not supported

  at org.apache.hadoop.fs.s3a.select.SelectInputStream.unsupported(SelectInputStream.java:288)
  at org.apache.hadoop.fs.s3a.select.SelectInputStream.seek(SelectInputStream.java:253)
  at org.apache.hadoop.fs.FSDataInputStream.seek(FSDataInputStream.java:66)
```

### InvalidTableAlias

The SELECT refers to the name of a column which is not recognized

* the name of a column is wrong, here `s.oddf`.
* headers are not enabled for the CSV source file. Fix: enable.
* a generated alias is used e.g `s._1`, but headers have been enabled.
Fix. disable, or use the header name.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
 SELECT * FROM S3OBJECT WHERE s."oddf" = 'true'
 on s3a://example/test/testParseBrokenCSVFile:
 com.amazonaws.services.s3.model.AmazonS3Exception:
 Invalid table alias is specified at line 1, column 30.
  Please check the file and try again. (Service: Amazon S3; Status Code: 400; Error Code: InvalidTableAlias;
   Invalid table alias is specified at line 1, column 30. Please check the file and try again.
    (Service: Amazon S3; Status Code: 400;
    Error Code: InvalidTableAlias;
    Request ID: 8693B86A52CFB91C;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:225)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:265)
  ...
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception:
 Invalid table alias is specified at line 1, column 30.
  Please check the file and try again.
   (Service: Amazon S3; Status Code: 400; Error Code: InvalidTableAlias; Request ID: 8693B86A52CFB91C;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1640)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1304)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1058)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
```

###  `AWSBadRequestException` "Attempt to convert from one data type to another failed: cast from STRING to TIMESTAMP."

A string field could not be converted to a timestamp because one or more of its entries were not parseable
with the given timestamp.

Example, from a spreadsheet where "timestamp" is normally a well-formatted timestamp field,
but in one column it is just "Tuesday"

```sql
SELECT CAST(s.date AS TIMESTAMP) FROM S3OBJECT s
```

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Select on s3a://example/test/testParseBrokenCSVFile:
com.amazonaws.services.s3.model.AmazonS3Exception:
Attempt to convert from one data type to another failed: cast from STRING to TIMESTAMP.
(Service: Amazon S3; Status Code: 400; Error Code: CastFailed;
Request ID: E2158FE45AF2049A; S3 Extended Request ID: iM40fzGuaPt6mQo0QxDDX+AY1bAgSVD1sKErFq6Y4GDJYHIAnmc00i0EvGGnH+0MFCFhKIivIrQ=),
S3 Extended Request ID: iM40fzGuaPt6mQo0QxDDX+AY1bAgSVD1sKErFq6Y4GDJYHIAnmc00i0EvGGnH+0MFCFhKIivIrQ=:CastFailed:
Attempt to convert from one data type to another failed: cast from STRING to TIMESTAMP.
(Service: Amazon S3; Status Code: 400; Error Code: CastFailed; Request ID: E2158FE45AF2049A; S3 Extended Request ID: iM40fzGuaPt6mQo0QxDDX+AY1bAgSVD1sKErFq6Y4GDJYHIAnmc00i0EvGGnH+0MFCFhKIivIrQ=)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:225)
  at org.apache.hadoop.fs.s3a.Invoker.once(Invoker.java:111)
  at org.apache.hadoop.fs.s3a.Invoker.lambda$retry$3(Invoker.java:265)
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception:
 Attempt to convert from one data type to another failed: cast from STRING to TIMESTAMP.
  (Service: Amazon S3; Status Code: 400; Error Code: CastFailed;)

```

There's no way to recover from a bad record here; no option to skip invalid
rows.

*Note:* This is an example stack trace *without* the SQL being printed.
