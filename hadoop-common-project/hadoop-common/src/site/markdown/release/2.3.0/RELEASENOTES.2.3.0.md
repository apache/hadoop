
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop  2.3.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-9241](https://issues.apache.org/jira/browse/HADOOP-9241) | *Trivial* | **DU refresh interval is not configurable**

The 'du' (disk usage command from Unix) script refresh monitor is now configurable in the same way as its 'df' counterpart, via the property 'fs.du.interval', the default of which is 10 minute (in ms).


---

* [HADOOP-8545](https://issues.apache.org/jira/browse/HADOOP-8545) | *Major* | **Filesystem Implementation for OpenStack Swift**

<!-- markdown -->
Added file system implementation for OpenStack Swift.
There are two implementation: block and native (similar to Amazon S3 integration).
Data locality issue solved by patch in Swift, commit procedure to OpenStack is in progress.

To use implementation add to core-site.xml following:

```xml
	<property>
	        <name>fs.swift.impl</name>
	    	<value>com.mirantis.fs.SwiftFileSystem</value>
	</property>
	<property>
	    	<name>fs.swift.block.impl</name>
	         <value>com.mirantis.fs.block.SwiftBlockFileSystem</value>
        </property>
```

In MapReduce job specify following configs for OpenStack Keystone authentication:

```java
conf.set("swift.auth.url", "http://172.18.66.117:5000/v2.0/tokens");
conf.set("swift.tenant", "superuser");
conf.set("swift.username", "admin1");
conf.set("swift.password", "password");
conf.setInt("swift.http.port", 8080);
conf.setInt("swift.https.port", 443);
```

Additional information specified on github: https://github.com/DmitryMezhensky/Hadoop-and-Swift-integration


---

* [MAPREDUCE-1176](https://issues.apache.org/jira/browse/MAPREDUCE-1176) | *Major* | **FixedLengthInputFormat and FixedLengthRecordReader**

Addition of FixedLengthInputFormat and FixedLengthRecordReader in the org.apache.hadoop.mapreduce.lib.input package. These two classes can be used when you need to read data from files containing fixed length (fixed width) records. Such files have no CR/LF (or any combination thereof), no delimiters etc, but each record is a fixed length, and extra data is padded with spaces. The data is one gigantic line within a file. When creating a job that specifies this input format, the job must have the "mapreduce.input.fixedlengthinputformat.record.length" property set as follows myJobConf.setInt("mapreduce.input.fixedlengthinputformat.record.length",[myFixedRecordLength]);

Please see javadoc for more details.


---

* [HDFS-5502](https://issues.apache.org/jira/browse/HDFS-5502) | *Major* | **Fix HTTPS support in HsftpFileSystem**

Fix the https support in HsftpFileSystem. With the change the client now verifies the server certificate. In particular, client side will verify the Common Name of the certificate using a strategy specified by the configuration property "hadoop.ssl.hostname.verifier".


---

* [HADOOP-10047](https://issues.apache.org/jira/browse/HADOOP-10047) | *Major* | **Add a directbuffer Decompressor API to hadoop**

Direct Bytebuffer decompressors for Zlib (Deflate & Gzip) and Snappy


---

* [HDFS-4997](https://issues.apache.org/jira/browse/HDFS-4997) | *Major* | **libhdfs doesn't return correct error codes in most cases**

libhdfs now returns correct codes in errno. Previously, due to a bug, many functions set errno to 255 instead of the more specific error code.


---

* [HDFS-5536](https://issues.apache.org/jira/browse/HDFS-5536) | *Major* | **Implement HTTP policy for Namenode and DataNode**

Add new HTTP policy configuration. Users can use "dfs.http.policy" to control the HTTP endpoints for NameNode and DataNode. Specifically, The following values are supported:
- HTTP\_ONLY : Service is provided only on http
- HTTPS\_ONLY : Service is provided only on https
- HTTP\_AND\_HTTPS : Service is provided both on http and https

hadoop.ssl.enabled and dfs.https.enabled are deprecated. When the deprecated configuration properties are still configured, currently http policy is decided based on the following rules:
1. If dfs.http.policy is set to HTTPS\_ONLY or HTTP\_AND\_HTTPS. It picks the specified policy, otherwise it proceeds to 2~4.
2. It picks HTTPS\_ONLY if hadoop.ssl.enabled equals to true.
3. It picks HTTP\_AND\_HTTPS if dfs.https.enable equals to true.
4. It picks HTTP\_ONLY for other configurations.


---

* [HDFS-4983](https://issues.apache.org/jira/browse/HDFS-4983) | *Major* | **Numeric usernames do not work with WebHDFS FS**

Add a new configuration property "dfs.webhdfs.user.provider.user.pattern" for specifying user name filters for WebHDFS.


---

* [HDFS-5663](https://issues.apache.org/jira/browse/HDFS-5663) | *Major* | **make the retry time and interval value configurable in openInfo()**

Makes the retries and time between retries getting the length of the last block on file configurable.  Below are the new configurations.

dfs.client.retry.times.get-last-block-length
dfs.client.retry.interval-ms.get-last-block-length

They are set to the 3 and 4000 respectively, these being what was previously hardcoded.


---

* [HDFS-5704](https://issues.apache.org/jira/browse/HDFS-5704) | *Major* | **Change OP\_UPDATE\_BLOCKS  with a new OP\_ADD\_BLOCK**

Add a new editlog record (OP\_ADD\_BLOCK) that only records allocation of the new block instead of the entire block list, on every block allocation.



