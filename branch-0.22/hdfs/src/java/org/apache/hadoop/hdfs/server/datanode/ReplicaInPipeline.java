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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockWriteStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/** 
 * This class defines a replica in a pipeline, which
 * includes a persistent replica being written to by a dfs client or
 * a temporary replica being replicated by a source datanode or
 * being copied for the balancing purpose.
 * 
 * The base class implements a temporary replica
 */
class ReplicaInPipeline extends ReplicaInfo
                        implements ReplicaInPipelineInterface {
  private long bytesAcked;
  private long bytesOnDisk;
  private byte[] lastChecksum;  
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

  /**
   * Copy constructor.
   * @param from
   */
  ReplicaInPipeline(ReplicaInPipeline from) {
    super(from);
    this.bytesAcked = from.getBytesAcked();
    this.bytesOnDisk = from.getBytesOnDisk();
    this.writer = from.writer;
  }

  @Override
  public long getVisibleLength() {
    return -1;
  }
  
  @Override  //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }
  
  @Override // ReplicaInPipelineInterface
  public long getBytesAcked() {
    return bytesAcked;
  }
  
  @Override // ReplicaInPipelineInterface
  public void setBytesAcked(long bytesAcked) {
    this.bytesAcked = bytesAcked;
  }
  
  @Override // ReplicaInPipelineInterface
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }
  
  @Override // ReplicaInPipelineInterface
  public synchronized void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
    this.bytesOnDisk = dataLength;
    this.lastChecksum = lastChecksum;
  }
  
  @Override // ReplicaInPipelineInterface
  public synchronized ChunkChecksum getLastChecksumAndDataLen() {
    return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
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
  
  @Override // ReplicaInPipelineInterface
  public BlockWriteStreams createStreams(boolean isCreate, 
      int bytesPerChunk, int checksumSize) throws IOException {
    File blockFile = getBlockFile();
    File metaFile = getMetaFile();
    if (DataNode.LOG.isDebugEnabled()) {
      DataNode.LOG.debug("writeTo blockfile is " + blockFile +
                         " of size " + blockFile.length());
      DataNode.LOG.debug("writeTo metafile is " + metaFile +
                         " of size " + metaFile.length());
    }
    long blockDiskSize = 0L;
    long crcDiskSize = 0L;
    if (!isCreate) { // check on disk file
      blockDiskSize = bytesOnDisk;
      crcDiskSize = BlockMetadataHeader.getHeaderSize() +
      (blockDiskSize+bytesPerChunk-1)/bytesPerChunk*checksumSize;
      if (blockDiskSize>0 && 
          (blockDiskSize>blockFile.length() || crcDiskSize>metaFile.length())) {
        throw new IOException("Corrupted block: " + this);
      }
    }
    FileOutputStream blockOut = null;
    FileOutputStream crcOut = null;
    try {
      blockOut = new FileOutputStream(
          new RandomAccessFile( blockFile, "rw" ).getFD() );
      crcOut = new FileOutputStream(
          new RandomAccessFile( metaFile, "rw" ).getFD() );
      if (!isCreate) {
        blockOut.getChannel().position(blockDiskSize);
        crcOut.getChannel().position(crcDiskSize);
      }
      return new BlockWriteStreams(blockOut, crcOut);
    } catch (IOException e) {
      IOUtils.closeStream(blockOut);
      IOUtils.closeStream(crcOut);
      throw e;
    }
  }
  
  @Override
  public String toString() {
    return super.toString()
        + "\n  bytesAcked=" + bytesAcked
        + "\n  bytesOnDisk=" + bytesOnDisk;
  }
}
