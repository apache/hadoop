/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.uploader;

/**
 * Default white list and black list implementations.
 */
final class DefaultJars {
  static final String DEFAULT_EXCLUDED_MR_JARS =
      ".*hadoop-yarn-server-applicationhistoryservice.*\\.jar," +
          ".*hadoop-yarn-server-nodemanager.*\\.jar," +
          ".*hadoop-yarn-server-resourcemanager.*\\.jar," +
          ".*hadoop-yarn-server-router.*\\.jar," +
          ".*hadoop-yarn-server-sharedcachemanager.*\\.jar," +
          ".*hadoop-yarn-server-timeline-pluginstorage.*\\.jar," +
          ".*hadoop-yarn-server-timelineservice.*\\.jar," +
          ".*hadoop-yarn-server-timelineservice-hbase.*\\.jar,";

  static final String DEFAULT_MR_JARS =
      "$HADOOP_HOME/share/hadoop/common/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/common/lib/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/hdfs/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/hdfs/lib/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/mapreduce/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/mapreduce/lib/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/yarn/.*\\.jar," +
          "$HADOOP_HOME/share/hadoop/yarn/lib/.*\\.jar,";

  private DefaultJars() {}
}
