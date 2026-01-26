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

S3 Select is a feature for Amazon S3 introduced in April 2018. It allows for
SQL-like SELECT expressions to be applied to files in some structured
formats, including CSV and JSON.

It is no longer supported in Hadoop releases.

Any Hadoop release built on the [AWS V2 SDK](./aws_sdk_upgrade.html)
will reject calls to open files using the select APIs.

If a build of Hadoop with S3 Select is desired, the relevant
classes can be found in hadoop trunk commit `8bf72346a59c`.

## Consequences of the removal

The path capabilities probe `fs.s3a.capability.select.sql` returns "false" for any and all
`s3a://` paths.

Any `openFile()` call where a SQL query is passed in as a `must()` clause
SHALL raise `UnsupportedOperationException`:
```java
// fails
openFile("s3a://bucket/path")
  .must("fs.s3a.select.sql", "SELECT ...")
  .get();
```

Any `openFile()` call to an S3A Path where a SQL query is passed in as a `may()`
clause SHALL be logged at WARN level the first time it is invoked, then ignored.
```java
// ignores the option after printing a warning.
openFile("s3a://bucket/path")
  .may("fs.s3a.select.sql", "SELECT ...")
  .get();
```

The `hadoop s3guard select` command is no longer supported.

Previously, the command would either generate an S3 select or a error (with exit code 42 being
the one for not enough arguments):

```
hadoop s3guard select
select [OPTIONS] [-limit rows] [-header (use|none|ignore)] [-out path] [-expected rows]
  [-compression (gzip|bzip2|none)] [-inputformat csv] [-outputformat csv] <PATH> <SELECT QUERY>
        make an S3 Select call

[main] INFO  util.ExitUtil (ExitUtil.java:terminate(241)) - Exiting with status 42:
       42: Too few arguments
```

Now it will fail with exit code 55 always:

```
hadoop s3guard select

[main] INFO  util.ExitUtil (ExitUtil.java:terminate(241)) - Exiting with status 55:
       55: S3 Select is no longer supported

```