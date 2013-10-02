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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
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
    List<org.apache.hadoop.mapreduce.InputSplit> newStyleSplits =
      super.getSplits(new Job(job));
    InputSplit[] ret = new InputSplit[newStyleSplits.size()];
    for(int pos = 0; pos < newStyleSplits.size(); ++pos) {
      org.apache.hadoop.mapreduce.lib.input.CombineFileSplit newStyleSplit = 
        (org.apache.hadoop.mapreduce.lib.input.CombineFileSplit) newStyleSplits.get(pos);
      ret[pos] = new CombineFileSplit(job, newStyleSplit.getPaths(),
        newStyleSplit.getStartOffsets(), newStyleSplit.getLengths(),
        newStyleSplit.getLocations());
    }
    return ret;
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
  
  /** List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   * 
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    List<FileStatus> result = super.listStatus(new Job(job));
    return result.toArray(new FileStatus[result.size()]);
  }

  /**
   * Subclasses should avoid overriding this method and should instead only
   * override {@link #isSplitable(FileSystem, Path)}.  The implementation of
   * this method simply calls the other method to preserve compatibility.
   * @see <a href="https://issues.apache.org/jira/browse/MAPREDUCE-5530">
   * MAPREDUCE-5530</a>
   *
   * @param context the job context
   * @param file the file name to check
   * @return is this file splitable?
   */
  @InterfaceAudience.Private
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    try {
      return isSplitable(FileSystem.get(context.getConfiguration()), file);
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected boolean isSplitable(FileSystem fs, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(fs.getConf()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }
}
