
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
# Apache Hadoop  1.0.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-1722](https://issues.apache.org/jira/browse/HADOOP-1722) | *Major* | **Make streaming to handle non-utf8 byte array**

Streaming allows binary (or other non-UTF8) streams.


---

* [MAPREDUCE-3851](https://issues.apache.org/jira/browse/MAPREDUCE-3851) | *Major* | **Allow more aggressive action on detection of the jetty issue**

added new configuration variables to control when TT aborts if it sees a certain number of exceptions:

    // Percent of shuffle exceptions (out of sample size) seen before it's
    // fatal - acceptable values are from 0 to 1.0, 0 disables the check.
    // ie. 0.3 = 30% of the last X number of requests matched the exception,
    // so abort.
      conf.getFloat(
          "mapreduce.reduce.shuffle.catch.exception.percent.limit.fatal", 0);

    // The number of trailing requests we track, used for the fatal
    // limit calculation
      conf.getInt("mapreduce.reduce.shuffle.catch.exception.sample.size", 1000);



