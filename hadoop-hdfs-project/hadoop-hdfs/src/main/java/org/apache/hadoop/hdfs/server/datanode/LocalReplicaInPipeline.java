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
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
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
public class LocalReplicaInPipeline extends LocalReplica
                        implements ReplicaInPipeline {
  private long bytesAcked;
  private long bytesOnDisk;
  private byte[] lastChecksum;
  private AtomicReference<Thread> writer = new AtomicReference<Thread>();

  /**
   * Bytes reserved for this replica on the containing volume.
   * Based off difference between the estimated maximum block length and
   * the bytes already written to this block.
   */
  private long bytesReserved;
  private final long originalBytesReserved;

  /**
   * Constructor for a zero length replica.
   * @param blockId block id
   * @param genStamp replica generation stamp
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param bytesToReserve disk space to reserve for this replica, based on
   *                       the estimated maximum block length.
   */
  public LocalReplicaInPipeline(long blockId, long genStamp,
        FsVolumeSpi vol, File dir, long bytesToReserve) {
    this(blockId, 0L, genStamp, vol, dir, Thread.currentThread(),
        bytesToReserve);
  }

  /**
   * Constructor
   * @param block a block
   * @param vol volume where replica is located
   * @param dir directory path where block and meta files are located
   * @param writer a thread that is writing to this replica
   */
  LocalReplicaInPipeline(Block block,
      FsVolumeSpi vol, File dir, Thread writer) {
    this(block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(),
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
  LocalReplicaInPipeline(long blockId, long len, long genStamp,
      FsVolumeSpi vol, File dir, Thread writer, long bytesToReserve) {
    super(blockId, len, genStamp, vol, dir);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    this.writer.set(writer);
    this.bytesReserved = bytesToReserve;
    this.originalBytesReserved = bytesToReserve;
  }

  /**
   * Copy constructor.
   * @param from where to copy from
   */
  public LocalReplicaInPipeline(LocalReplicaInPipeline from) {
    super(from);
    this.bytesAcked = from.getBytesAcked();
    this.bytesOnDisk = from.getBytesOnDisk();
    this.writer.set(from.writer.get());
    this.bytesReserved = from.bytesReserved;
    this.originalBytesReserved = from.originalBytesReserved;
  }

  @Override
  public long getVisibleLength() {
    return -1;
  }

  @Override  //ReplicaInfo
  public ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }

  @Override // ReplicaInPipeline
  public long getBytesAcked() {
    return bytesAcked;
  }

  @Override // ReplicaInPipeline
  public void setBytesAcked(long bytesAcked) {
    long newBytesAcked = bytesAcked - this.bytesAcked;
    this.bytesAcked = bytesAcked;

    // Once bytes are ACK'ed we can release equivalent space from the
    // volume's reservedForRbw count. We could have released it as soon
    // as the write-to-disk completed but that would be inefficient.
    getVolume().releaseReservedSpace(newBytesAcked);
    bytesReserved -= newBytesAcked;
  }

  @Override // ReplicaInPipeline
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }

  @Override
  public long getBytesReserved() {
    return bytesReserved;
  }

  @Override
  public long getOriginalBytesReserved() {
    return originalBytesReserved;
  }

  @Override // ReplicaInPipeline
  public void releaseAllBytesReserved() {
    getVolume().releaseReservedSpace(bytesReserved);
    getVolume().releaseLockedMemory(bytesReserved);
    bytesReserved = 0;
  }

  @Override // ReplicaInPipeline
  public synchronized void setLastChecksumAndDataLen(long dataLength,
      byte[] checksum) {
    this.bytesOnDisk = dataLength;
    this.lastChecksum = checksum;
  }

  @Override // ReplicaInPipeline
  public synchronized ChunkChecksum getLastChecksumAndDataLen() {
    return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
  }

  @Override // ReplicaInPipeline
  public void setWriter(Thread writer) {
    this.writer.set(writer);
  }

  @Override
  public void interruptThread() {
    Thread thread = writer.get();
    if (thread != null && thread != Thread.currentThread()
        && thread.isAlive()) {
      thread.interrupt();
    }
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * Attempt to set the writer to a new value.
   */
  @Override // ReplicaInPipeline
  public boolean attemptToSetWriter(Thread prevWriter, Thread newWriter) {
    return writer.compareAndSet(prevWriter, newWriter);
  }

  /**
   * Interrupt the writing thread and wait until it dies.
   * @throws IOException the waiting is interrupted
   */
  @Override // ReplicaInPipeline
  public void stopWriter(long xceiverStopTimeout) throws IOException {
    while (true) {
      Thread thread = writer.get();
      if ((thread == null) || (thread == Thread.currentThread()) ||
          (!thread.isAlive())) {
        if (writer.compareAndSet(thread, null)) {
          return; // Done
        }
        // The writer changed.  Go back to the start of the loop and attempt to
        // stop the new writer.
        continue;
      }
      thread.interrupt();
      try {
        thread.join(xceiverStopTimeout);
        if (thread.isAlive()) {
          // Our thread join timed out.
          final String msg = "Join on writer thread " + thread + " timed out";
          DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(thread));
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

  @Override // ReplicaInPipeline
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
        if (blockDiskSize > 0 &&
            (blockDiskSize > blockFile.length() ||
               crcDiskSize>metaFile.length())) {
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
          new RandomAccessFile(blockFile, "rw").getFD());
      crcOut = new FileOutputStream(metaRAF.getFD());
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
  public OutputStream createRestartMetaStream() throws IOException {
    File blockFile = getBlockFile();
    File restartMeta = new File(blockFile.getParent()  +
        File.pathSeparator + "." + blockFile.getName() + ".restart");
    if (restartMeta.exists() && !restartMeta.delete()) {
      DataNode.LOG.warn("Failed to delete restart meta file: " +
          restartMeta.getPath());
    }
    return new FileOutputStream(restartMeta);
  }

  @Override
  public String toString() {
    return super.toString()
        + "\n  bytesAcked=" + bytesAcked
        + "\n  bytesOnDisk=" + bytesOnDisk;
  }

  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getOriginalReplica");
  }

  @Override
  public long getRecoveryID() {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support getRecoveryID");
  }

  @Override
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support setRecoveryID");
  }

  @Override
  public ReplicaRecoveryInfo createInfo(){
    throw new UnsupportedOperationException("Replica of type " + getState() +
        " does not support createInfo");
  }

  public void moveReplicaFrom(ReplicaInfo oldReplicaInfo, File newBlkFile)
      throws IOException {

    if (!(oldReplicaInfo instanceof LocalReplica)) {
      throw new IOException("The source replica with blk id "
          + oldReplicaInfo.getBlockId()
          + " should be derived from LocalReplica");
    }

    LocalReplica localReplica = (LocalReplica) oldReplicaInfo;

    File oldmeta = localReplica.getMetaFile();
    File newmeta = getMetaFile();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      NativeIO.renameTo(oldmeta, newmeta);
    } catch (IOException e) {
      throw new IOException("Block " + oldReplicaInfo + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to rbw dir " + newmeta, e);
    }

    File blkfile = localReplica.getBlockFile();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + blkfile + " to " + newBlkFile
          + ", file length=" + blkfile.length());
    }
    try {
      NativeIO.renameTo(blkfile, newBlkFile);
    } catch (IOException e) {
      try {
        NativeIO.renameTo(newmeta, oldmeta);
      } catch (IOException ex) {
        LOG.warn("Cannot move meta file " + newmeta +
            "back to the finalized directory " + oldmeta, ex);
      }
      throw new IOException("Block " + oldReplicaInfo + " reopen failed. " +
                              " Unable to move block file " + blkfile +
                              " to rbw dir " + newBlkFile, e);
    }
  }

  @Override // ReplicaInPipeline
  public ReplicaInfo getReplicaInfo() {
    return this;
  }
}
