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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An abstract {@link org.apache.hadoop.mapred.InputFormat} that returns {@link CombineFileSplit}'s
 * in {@link org.apache.hadoop.mapred.InputFormat#getSplits(JobConf, int)} method. 
 * Splits are constructed from the files under the input paths. 
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack. 
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default spliting behaviour in Hadoop: each
 * block is a locally processed split.
 * Subclasses implement {@link org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit, JobConf, Reporter)}
 * to construct <code>RecordReader</code>'s for <code>CombineFileSplit</code>'s.
 * @see CombineFileSplit
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat<K, V> 
  implements InputFormat<K, V>{

  /**
   * default constructor
   */
  public CombineFileInputFormat() {
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) 
    throws IOException {
    return super.getSplits(new Job(job)).toArray(new InputSplit[0]);
  }
  
  /**
   * Create a new pool and add the filters to it.
   * A split cannot have files from different pools.
   * @deprecated Use {@link #createPool(List)}.
   */
  @Deprecated
  protected void createPool(JobConf conf, List<PathFilter> filters) {
    createPool(filters);
  }

  /**
   * Create a new pool and add the filters to it. 
   * A pathname can satisfy any one of the specified filters.
   * A split cannot have files from different pools.
   * @deprecated Use {@link #createPool(PathFilter...)}.
   */
  @Deprecated
  protected void createPool(JobConf conf, PathFilter... filters) {
    createPool(filters);
  }

  /**
   * This is not implemented yet. 
   */
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException;

  // abstract method from super class implemented to return null
  public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(
      org.apache.hadoop.mapreduce.InputSplit split,
      TaskAttemptContext context) throws IOException {
    return null;
  }

}
