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

## Using the Hadoop S3A Connector with Apache Iceberg

The Apache Hadoop S3A Connector can be used to access data on S3 stores through Apache Iceberg

It:
* Uses an AWS v2 client qualified across a broad set of applications, including Apache Spark, Apache Hive, Apache HBase and more. This may lag the Iceberg artifacts.
* Is qualified with third party stores on every release.
* Contains detection and recovery for S3 failures beyond that in the AWS SDK -recovery added "one support call at a time".
* Supports scatter/gather IO "Vector IO" for high-performance Parquet reads.
* Supports Amazon S3 Express One Zone storage, FIPS endpoints, Client-Side encryption S3 Access Points and S3 Access Grants.
* Supports OpenSSL as an optional TLS transport layer -for tangible performance improvements over the JDK implementation.
* Includes [auditing via the S3 Server Logs](./auditing.html), which can be used to answer important questions such as "who deleted all the files?" and "which job is triggering throttling?".
* Collects [client-side statistics](https://apachecon.com/acasia2022/sessions/bigdata-1191.html) for identification of performance and connectivity issues.
* Note: it does not support S3 Dual Stack, S3 Acceleration or S3 Tags.

To use the S3A Connector, here are the instructions:

1. To store data using S3A, specify the `warehouse` catalog property to be an S3A path, e.g. `s3a://my-bucket/my-warehouse` 
2. For `HiveCatalog` to also store metadata using S3A, specify the Hadoop config property `hive.metastore.warehouse.dir` to be an S3A path.
3. Add [hadoop-aws](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws) as a runtime dependency of your compute engine. The version of this module must be the exact same version as all other hadoop binaries on the classpath.
4. For the latest features, bug fixes and best performance, use the latest versions of all hadoop and hadoop-aws libraries.
5. Use the same shaded version of the AWS SDK `bundle.jar` as the `hadoop-aws` module was built and tested with. Older versions are unlikely to work, newer versions will be unqualified and may cause regressions
6. Configure AWS settings based on [hadoop-aws documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html).

### Maximizing Parquet and Iceberg Performance through the S3A Connector

For best performance
* The S3A connector should be configured for the Parquet and iceberg workloads
based on the recommedations of [Maximizing Performance when working with the S3A Connector](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html).
* Iceberg should be configured to use the Hadoop Bulk Delete API when deleting files.
* Parquet should be configured to use the Vector IO API for parallelized data retrieval.

The recommended settings are listed below:

| Property                           | Recommended value | Description                                                                            |
|------------------------------------|-------------------|----------------------------------------------------------------------------------------|
| `iceberg.hadoop.bulk.delete.enabled` | `true`          | Iceberg to use Hadoop bulk delete API, where available                                 |
| `parquet.hadoop.vectored.io.enabled` | `true`          | Use Vector IO in Parquet Reads, where available                                        |
|` fs.s3a.vectored.read.min.seek.size` | `128K`          | Threshold below which adjacent Vector IO ranges are coalesced into single GET requests |
| `fs.s3a.experimental.input.fadvise`  | `parquet,vector,random,adaptive` | Preferred read policy when opening files.                             |

```shell
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3a://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
    --conf spark.sql.catalog.my_catalog.iceberg.hadoop.bulk.delete.enabled=true \
    --conf spark.sql.catalog.my_catalog.parquet.hadoop.vectored.io.enabled=true \
    --conf spark.hadoop.fs.s3a.vectored.read.min.seek.size=128K \
    --conf spark.hadoop.fs.s3a.experimental.input.fadvise=parquet,vector,random,adaptive
```

The property `fs.s3a.vectored.read.min.seek.size` sets the threshold below which adjacent requests are coalesced into single GET requests.
This can compensate for S3 request latency by combining requests and discarding the data between them.
The recommended value is based on the [Facebook Velox paper](https://research.facebook.com/publications/velox-metas-unified-execution-engine/)

> IO reads for nearby columns are typically coalesced (merged) if the gap between them is small enough (currently about 20K for SSD and 500K for disaggregated storage), aiming to serve neighboring reads in as few IO reads as possible.


