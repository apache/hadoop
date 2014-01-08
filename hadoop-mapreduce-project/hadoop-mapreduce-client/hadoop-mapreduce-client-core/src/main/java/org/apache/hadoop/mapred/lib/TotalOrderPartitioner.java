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


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Partitioner effecting a total order by reading split points from
 * an externally generated source.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TotalOrderPartitioner<K ,V>
    extends org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner<K, V>
    implements Partitioner<K,V> {

  public TotalOrderPartitioner() { }

  public void configure(JobConf job) {
    super.setConf(job);
  }

  /**
   * Set the path to the SequenceFile storing the sorted partition keyset.
   * It must be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt>
   * keys in the SequenceFile.
   * @deprecated Use 
   * {@link #setPartitionFile(Configuration, Path)}
   * instead
   */
  @Deprecated
  public static void setPartitionFile(JobConf job, Path p) {
    org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.
            setPartitionFile(job, p);
  }

  /**
   * Get the path to the SequenceFile storing the sorted partition keyset.
   * @see #setPartitionFile(JobConf,Path)
   * @deprecated Use 
   * {@link #getPartitionFile(Configuration)}
   * instead
   */
  @Deprecated
  public static String getPartitionFile(JobConf job) {
    return org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.
            getPartitionFile(job);
  }
}
