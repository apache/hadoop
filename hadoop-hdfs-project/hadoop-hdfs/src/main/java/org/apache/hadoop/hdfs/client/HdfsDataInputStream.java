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
package org.apache.hadoop.hdfs.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

/**
 * The Hdfs implementation of {@link FSDataInputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsDataInputStream extends FSDataInputStream {
  public HdfsDataInputStream(DFSInputStream in) throws IOException {
    super(in);
  }

  /**
   * Get the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return ((DFSInputStream) in).getCurrentDatanode();
  }

  /**
   * Get the block containing the target position.
   */
  public ExtendedBlock getCurrentBlock() {
    return ((DFSInputStream) in).getCurrentBlock();
  }

  /**
   * Get the collection of blocks that has already been located.
   */
  public synchronized List<LocatedBlock> getAllBlocks() throws IOException {
    return ((DFSInputStream) in).getAllBlocks();
  }

  /**
   * Get the visible length of the file. It will include the length of the last
   * block even if that is in UnderConstruction state.
   * 
   * @return The visible length of the file.
   */
  public long getVisibleLength() throws IOException {
    return ((DFSInputStream) in).getFileLength();
  }

  /**
   * Get statistics about the reads which this DFSInputStream has done.
   * Note that because HdfsDataInputStream is buffered, these stats may
   * be higher than you would expect just by adding up the number of
   * bytes read through HdfsDataInputStream.
   */
  public synchronized DFSInputStream.ReadStatistics getReadStatistics() {
    return ((DFSInputStream) in).getReadStatistics();
  }
}
