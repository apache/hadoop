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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class KeyFieldBasedPartitioner<K2, V2> implements Partitioner<K2, V2> {

  private int numOfPartitionFields;

  private String keyFieldSeparator;

  public void configure(JobConf job) {
    this.keyFieldSeparator = job.get("map.output.key.field.separator", "\t");
    this.numOfPartitionFields = job.getInt("num.key.fields.for.partition", 0);
  }

  /** Use {@link Object#hashCode()} to partition. */
  public int getPartition(K2 key, V2 value,
      int numReduceTasks) {
    String partitionKeyStr = key.toString();
    String[] fields = partitionKeyStr.split(this.keyFieldSeparator);
    if (this.numOfPartitionFields > 0
        && this.numOfPartitionFields < fields.length) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < this.numOfPartitionFields; i++) {
        sb.append(fields[i]).append(this.keyFieldSeparator);
      }
      partitionKeyStr = sb.toString();
      if (partitionKeyStr.length() > 0) {
        partitionKeyStr = partitionKeyStr.substring(0,
            partitionKeyStr.length() - 1);
      }
    }
    return (partitionKeyStr.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}
