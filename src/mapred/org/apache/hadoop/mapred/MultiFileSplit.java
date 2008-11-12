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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.Text;

/**
 * A sub-collection of input files. Unlike {@link FileSplit}, MultiFileSplit 
 * class does not represent a split of a file, but a split of input files 
 * into smaller sets. The atomic unit of split is a file. <br> 
 * MultiFileSplit can be used to implement {@link RecordReader}'s, with 
 * reading one record per file.
 * @see FileSplit
 * @see MultiFileInputFormat 
 */
public class MultiFileSplit implements InputSplit {

  private Path[] paths;
  private long[] lengths;
  private long totLength;
  private JobConf job;

  MultiFileSplit() {}
  
  public MultiFileSplit(JobConf job, Path[] files, long[] lengths) {
    this.job = job;
    this.lengths = lengths;
    this.paths = files;
    this.totLength = 0;
    for(long length : lengths) {
      totLength += length;
    }
  }

  public long getLength() {
    return totLength;
  }
  
  /** Returns an array containing the lengths of the files in 
   * the split*/ 
  public long[] getLengths() {
    return lengths;
  }
  
  /** Returns the length of the i<sup>th</sup> Path */
  public long getLength(int i) {
    return lengths[i];
  }
  
  /** Returns the number of Paths in the split */
  public int getNumPaths() {
    return paths.length;
  }

  /** Returns the i<sup>th</sup> Path */
  public Path getPath(int i) {
    return paths[i];
  }
  
  /** Returns all the Paths in the split */
  public Path[] getPaths() {
    return paths;
  }

  public String[] getLocations() throws IOException {
    HashSet<String> hostSet = new HashSet<String>();
    for (Path file : paths) {
      FileSystem fs = file.getFileSystem(job);
      FileStatus status = fs.getFileStatus(file);
      BlockLocation[] blkLocations = fs.getFileBlockLocations(status,
                                          0, status.getLen());
      if (blkLocations != null && blkLocations.length > 0) {
        addToSet(hostSet, blkLocations[0].getHosts());
      }
    }
    return hostSet.toArray(new String[hostSet.size()]);
  }

  private void addToSet(Set<String> set, String[] array) {
    for(String s:array)
      set.add(s); 
  }

  public void readFields(DataInput in) throws IOException {
    totLength = in.readLong();
    int arrLength = in.readInt();
    lengths = new long[arrLength];
    for(int i=0; i<arrLength;i++) {
      lengths[i] = in.readLong();
    }
    int filesLength = in.readInt();
    paths = new Path[filesLength];
    for(int i=0; i<filesLength;i++) {
      paths[i] = new Path(Text.readString(in));
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(totLength);
    out.writeInt(lengths.length);
    for(long length : lengths)
      out.writeLong(length);
    out.writeInt(paths.length);
    for(Path p : paths) {
      Text.writeString(out, p.toString());
    }
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for(int i=0; i < paths.length; i++) {
      sb.append(paths[i].toUri().getPath() + ":0+" + lengths[i]);
      if (i < paths.length -1) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }
}

