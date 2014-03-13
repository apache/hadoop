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
package org.apache.hadoop.fs.slive;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * The partitioner partitions the map output according to the operation type.
 * The partition number is the hash of the operation type modular the total
 * number of the reducers.
 */
public class SlivePartitioner implements Partitioner<Text, Text> {
  @Override // JobConfigurable
  public void configure(JobConf conf) {}

  @Override // Partitioner
  public int getPartition(Text key, Text value, int numPartitions) {
    OperationOutput oo = new OperationOutput(key, value);
    return (oo.getOperationType().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}
