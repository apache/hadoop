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
  private Thread writer;
  
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
    this( blockId, 0L, genStamp, vol, dir, Thread.currentThread());
  }

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param writer a thread that is writing to this replica
   */
  ReplicaInPipeline(Block block, 
      FSVolume vol, File dir, Thread writer) {
    this( block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(),
        vol, dir, writer);
  }

  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param writer a thread that is writing to this replica
   */
  ReplicaInPipeline(long blockId, long len, long genStamp,
      FSVolume vol, File dir, Thread writer ) {
    super( blockId, len, genStamp, vol, dir);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    this.writer = writer;
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
   * Set the thread that is writing to this replica
   * @param writer a thread writing to this replica
   */
  public void setWriter(Thread writer) {
    this.writer = writer;
  }
  
  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }
  
  /**
   * Interrupt the writing thread and wait until it dies
   * @throws IOException the waiting is interrupted
   */
  void stopWriter() throws IOException {
    if (writer != null && writer != Thread.currentThread() && writer.isAlive()) {
      writer.interrupt();
      try {
        writer.join();
      } catch (InterruptedException e) {
        throw new IOException("Waiting for writer thread is interrupted.");
      }
    }
  }
  
  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
}
