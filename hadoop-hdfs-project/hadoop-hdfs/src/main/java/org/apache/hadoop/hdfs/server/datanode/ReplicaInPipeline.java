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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

/** 
 * This class defines a replica in a pipeline, which
 * includes a persistent replica being written to by a dfs client or
 * a temporary replica being replicated by a source datanode or
 * being copied for the balancing purpose.
 * 
 * The base class implements a temporary replica
 */
public class ReplicaInPipeline extends ReplicaInfo
                        implements ReplicaInPipelineInterface {
  private long bytesAcked;
  private long bytesOnDisk;
  private byte[] lastChecksum;  
  private Thread writer;

  /**
   * Bytes reserved for this replica on the containing volume.
   * Based off difference between the estimated maximum block length and
   * the bytes already written to this block.
   */
  private long bytesReserved;
  
  /**
   * Constructor for a zero length replica
   * @param blockId block id
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param bytesToReserve disk space to reserve for this replica, based on
   *                       the estimated maximum block length.
   */
  public ReplicaInPipeline(long blockId, long genStamp, 
        FsVolumeSpi vol, File dir, long bytesToReserve) {
    this(blockId, 0L, genStamp, vol, dir, Thread.currentThread(), bytesToReserve);
  }

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param writer a thread that is writing to this replica
   */
  ReplicaInPipeline(Block block, 
      FsVolumeSpi vol, File dir, Thread writer) {
    this( block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(),
        vol, dir, writer, 0L);
  }

  /**
   * Constructor
   * @param blockId block id
   * @param len replica length
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param writer a thread that is writing to this replica
   * @param bytesToReserve disk space to reserve for this replica, based on
   *                       the estimated maximum block length.
   */
  ReplicaInPipeline(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir, Thread writer, long bytesToReserve) {
    super( blockId, len, genStamp, vol, dir);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    this.writer = writer;
    this.bytesReserved = bytesToReserve;
  }

  /**
   * Copy constructor.
   * @param from where to copy from
   */
  public ReplicaInPipeline(ReplicaInPipeline from) {
    super(from);
    this.bytesAcked = from.getBytesAcked();
    this.bytesOnDisk = from.getBytesOnDisk();
    this.writer = from.writer;
    this.bytesReserved = from.bytesReserved;
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
    long newBytesAcked = bytesAcked - this.bytesAcked;
    this.bytesAcked = bytesAcked;

    // Once bytes are ACK'ed we can release equivalent space from the
    // volume's reservedForRbw count. We could have released it as soon
    // as the write-to-disk completed but that would be inefficient.
    getVolume().releaseReservedSpace(newBytesAcked);
    bytesReserved -= newBytesAcked;
  }
  
  @Override // ReplicaInPipelineInterface
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }

  @Override
  public long getBytesReserved() {
    return bytesReserved;
  }
  
  @Override
  public void releaseAllBytesReserved() {  // ReplicaInPipelineInterface
    getVolume().releaseReservedSpace(bytesReserved);
    bytesReserved = 0;
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
  public void stopWriter(long xceiverStopTimeout) throws IOException {
    if (writer != null && writer != Thread.currentThread() && writer.isAlive()) {
      writer.interrupt();
      try {
        writer.join(xceiverStopTimeout);
        if (writer.isAlive()) {
          final String msg = "Join on writer thread " + writer + " timed out";
          DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(writer));
          throw new IOException(msg);
        }
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
  public ReplicaOutputStreams createStreams(boolean isCreate, 
      DataChecksum requestedChecksum) throws IOException {
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
    
    // the checksum that should actually be used -- this
    // may differ from requestedChecksum for appends.
    final DataChecksum checksum;
    
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    
    if (!isCreate) {
      // For append or recovery, we must enforce the existing checksum.
      // Also, verify that the file has correct lengths, etc.
      boolean checkedMeta = false;
      try {
        BlockMetadataHeader header = BlockMetadataHeader.readHeader(metaRAF);
        checksum = header.getChecksum();
        
        if (checksum.getBytesPerChecksum() !=
            requestedChecksum.getBytesPerChecksum()) {
          throw new IOException("Client requested checksum " +
              requestedChecksum + " when appending to an existing block " +
              "with different chunk size: " + checksum);
        }
        
        int bytesPerChunk = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize();
        
        blockDiskSize = bytesOnDisk;
        crcDiskSize = BlockMetadataHeader.getHeaderSize() +
          (blockDiskSize+bytesPerChunk-1)/bytesPerChunk*checksumSize;
        if (blockDiskSize>0 && 
            (blockDiskSize>blockFile.length() || crcDiskSize>metaFile.length())) {
          throw new IOException("Corrupted block: " + this);
        }
        checkedMeta = true;
      } finally {
        if (!checkedMeta) {
          // clean up in case of exceptions.
          IOUtils.closeStream(metaRAF);
        }
      }
    } else {
      // for create, we can use the requested checksum
      checksum = requestedChecksum;
    }
    
    FileOutputStream blockOut = null;
    FileOutputStream crcOut = null;
    try {
      blockOut = new FileOutputStream(
          new RandomAccessFile( blockFile, "rw" ).getFD() );
      crcOut = new FileOutputStream(metaRAF.getFD() );
      if (!isCreate) {
        blockOut.getChannel().position(blockDiskSize);
        crcOut.getChannel().position(crcDiskSize);
      }
      return new ReplicaOutputStreams(blockOut, crcOut, checksum,
          getVolume().isTransientStorage());
    } catch (IOException e) {
      IOUtils.closeStream(blockOut);
      IOUtils.closeStream(metaRAF);
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
