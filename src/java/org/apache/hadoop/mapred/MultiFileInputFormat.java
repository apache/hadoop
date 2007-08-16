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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An abstract {@link InputFormat} that returns {@link MultiFileSplit}'s
 * in {@link #getSplits(JobConf, int)} method. Splits are constructed from 
 * the files under the input paths. Each split returned contains <i>nearly</i>
 * equal content length. <br>  
 * Subclasses implement {@link #getRecordReader(InputSplit, JobConf, Reporter)}
 * to construct <code>RecordReader</code>'s for <code>MultiFileSplit</code>'s.
 * @see MultiFileSplit
 */
public abstract class MultiFileInputFormat<K extends WritableComparable,
                                           V extends Writable>
  extends FileInputFormat<K, V> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) 
    throws IOException {
    
    MultiFileSplit[] splits = new MultiFileSplit[numSplits];
    Path[] paths = listPaths(job);
    long[] lengths = new long[paths.length];
    long totLength = 0;
    for(int i=0; i<paths.length; i++) {
      FileSystem fs = paths[i].getFileSystem(job);
      lengths[i] = fs.getContentLength(paths[i]);
      totLength += lengths[i];
    }
    float avgLengthPerSplit = ((float)totLength) / numSplits;
    long cumulativeLength = 0;

    int startIndex = 0;

    for(int i=0; i<numSplits; i++) {
      int splitSize = findSize(i, avgLengthPerSplit, cumulativeLength
          , startIndex, lengths);
      Path[] splitPaths = new Path[splitSize];
      long[] splitLengths = new long[splitSize];
      System.arraycopy(paths, startIndex, splitPaths , 0, splitSize);
      System.arraycopy(lengths, startIndex, splitLengths , 0, splitSize);
      splits[i] = new MultiFileSplit(job, splitPaths, splitLengths);
      startIndex += splitSize;
      for(long l: splitLengths) {
        cumulativeLength += l;
      }
    }
    return splits;
    
  }

  private int findSize(int splitIndex, float avgLengthPerSplit
      , long cumulativeLength , int startIndex, long[] lengths) {
    
    if(splitIndex == lengths.length - 1)
      return lengths.length - startIndex;
    
    long goalLength = (long)((splitIndex + 1) * avgLengthPerSplit);
    int partialLength = 0;
    // accumulate till just above the goal length;
    for(int i = startIndex; i < lengths.length; i++) {
      partialLength += lengths[i];
      if(partialLength + cumulativeLength >= goalLength) {
        return i - startIndex + 1;
      }
    }
    return lengths.length - startIndex;
  }
  
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter)
      throws IOException;
}
