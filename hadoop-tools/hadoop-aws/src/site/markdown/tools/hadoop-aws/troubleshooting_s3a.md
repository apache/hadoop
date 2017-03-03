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

# Troubleshooting S3A

Here are some lower level details and hints on troubleshooting and tuning
the S3A client.

## Logging at lower levels

The AWS SDK and the Apache HTTP components can be configured to log at
more detail, as can S3A itself.

```properties
log4j.logger.org.apache.hadoop.fs.s3a=DEBUG
log4j.logger.com.amazonaws.request=DEBUG
log4j.logger.org.apache.http=DEBUG
log4j.logger.org.apache.http.wire=ERROR
```

Be aware that logging HTTP headers may leak sensitive AWS account information,
so should not be shared.

## Advanced: network performance

An example of this is covered in [HADOOP-13871](https://issues.apache.org/jira/browse/HADOOP-13871).

1. For public data, use `curl`:

        curl -O https://landsat-pds.s3.amazonaws.com/scene_list.gz
1. Use `nettop` to monitor a processes connections.

Consider reducing the connection timeout of the s3a connection.

```xml
<property>
  <name>fs.s3a.connection.timeout</name>
  <value>15000</value>
</property>
```
This *may* cause the client to react faster to network pauses.
