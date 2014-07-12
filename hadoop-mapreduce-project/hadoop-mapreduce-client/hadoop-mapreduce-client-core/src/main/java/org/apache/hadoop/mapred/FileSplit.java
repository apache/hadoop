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
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader(InputSplit,JobConf,Reporter)}. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileSplit extends org.apache.hadoop.mapreduce.InputSplit 
                       implements InputSplitWithLocationInfo {
  org.apache.hadoop.mapreduce.lib.input.FileSplit fs; 
  protected FileSplit() {
    fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit();
  }

  /** Constructs a split.
   * @deprecated
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   */
  @Deprecated
  public FileSplit(Path file, long start, long length, JobConf conf) {
    this(file, start, length, (String[])null);
  }

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public FileSplit(Path file, long start, long length, String[] hosts) {
    fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit(file, start,
           length, hosts);
  }
  
  /** Constructs a split with host information
  *
  * @param file the file name
  * @param start the position of the first byte in the file to process
  * @param length the number of bytes in the file to process
  * @param hosts the list of hosts containing the block, possibly null
  * @param inMemoryHosts the list of hosts containing the block in memory
  */
 public FileSplit(Path file, long start, long length, String[] hosts,
     String[] inMemoryHosts) {
   fs = new org.apache.hadoop.mapreduce.lib.input.FileSplit(file, start,
          length, hosts, inMemoryHosts);
 }
  
  public FileSplit(org.apache.hadoop.mapreduce.lib.input.FileSplit fs) {
    this.fs = fs;
  }

  /** The file containing this split's data. */
  public Path getPath() { return fs.getPath(); }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return fs.getStart(); }
  
  /** The number of bytes in the file to process. */
  public long getLength() { return fs.getLength(); }

  public String toString() { return fs.toString(); }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    fs.write(out);
  }
  public void readFields(DataInput in) throws IOException {
    fs.readFields(in);
  }

  public String[] getLocations() throws IOException {
    return fs.getLocations();
  }
  
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return fs.getLocationInfo();
  }
}
