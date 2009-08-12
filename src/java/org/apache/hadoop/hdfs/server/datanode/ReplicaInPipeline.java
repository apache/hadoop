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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/** 
 * This class defines a replica in a pipeline, which
 * includes a persistent replica being written to by a dfs client or
 * a temporary replica being replicated by a source datanode or
 * being copied for the balancing purpose.
 * 
 * The base class implements a temporary replica
 */
class ReplicaInPipeline extends ReplicaInfo {
  private long bytesAcked;
  private long bytesOnDisk;
  private List<Thread> threads = new ArrayList<Thread>();
  
  /**
   * Constructor for a zero length replica
   * @param blockId block id
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param state replica state
   */
    ReplicaInPipeline(long blockId, long genStamp, 
        FSVolume vol, File dir) {
    this( blockId, 0L, genStamp, vol, dir, null);
  }

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param threads a list of threads that are writing to this replica
   */
  ReplicaInPipeline(Block block, 
      FSVolume vol, File dir, List<Thread> threads) {
    this( block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(),
        vol, dir, threads);
  }

  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param threads a list of threads that are writing to this replica
   */
  ReplicaInPipeline(long blockId, long len, long genStamp,
      FSVolume vol, File dir, List<Thread> threads ) {
    super( blockId, len, genStamp, vol, dir);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    setThreads(threads);
    this.threads.add(Thread.currentThread());
  }

  @Override  //ReplicaInfo
  long getVisibleLen() throws IOException {
    // no bytes are visible
    throw new IOException("No bytes are visible for temporary replicas");
  }
  
  @Override  //ReplicaInfo
  ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }
  
  /**
   * Get the number of bytes acked
   * @return the number of bytes acked
   */
  long getBytesAcked() {
    return bytesAcked;
  }
  
  /**
   * Set the number bytes that have acked
   * @param bytesAcked
   */
  void setBytesAcked(long bytesAcked) {
    this.bytesAcked = bytesAcked;
  }
  
  /**
   * Get the number of bytes that have written to disk
   * @return the number of bytes that have written to disk
   */
  long getBytesOnDisk() {
    return bytesOnDisk;
  }
  
  /**
   * Set the number of bytes on disk
   * @param bytesOnDisk number of bytes on disk
   */
  void setBytesOnDisk(long bytesOnDisk) {
    this.bytesOnDisk = bytesOnDisk;
  }
  
  /**
   * Set the threads that are writing to this replica
   * @param threads a list of threads writing to this replica
   */
  public void setThreads(List<Thread> threads) {
    if (threads != null) {
      threads.addAll(threads);
    }
  }
  
  /**
   * Get a list of threads writing to this replica 
   * @return a list of threads writing to this replica
   */
  public List<Thread> getThreads() {
    return threads;
  }
  
  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
}
