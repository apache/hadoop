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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.LightWeightResizableGSet;

/**
 * This class is used by datanodes to maintain meta data of its replicas.
 * It provides a general interface for meta information of a replica.
 */
@InterfaceAudience.Private
abstract public class ReplicaInfo extends Block
    implements Replica, LightWeightResizableGSet.LinkedElement {

  /** For implementing {@link LightWeightResizableGSet.LinkedElement}. */
  private LightWeightResizableGSet.LinkedElement next;

  /** volume where the replica belongs. */
  private FsVolumeSpi volume;

  /** This is used by some tests and FsDatasetUtil#computeChecksum. */
  private static final FileIoProvider DEFAULT_FILE_IO_PROVIDER =
      new FileIoProvider(null, null);

  /**
   * Constructor.
   * @param block a block
   * @param vol volume where replica is located
   */
  ReplicaInfo(Block block, FsVolumeSpi vol) {
    this(vol, block.getBlockId(), block.getNumBytes(),
        block.getGenerationStamp());
  }

  /**
  * Constructor
  * @param vol volume where replica is located
  * @param blockId block id
  * @param len replica length
  * @param genStamp replica generation stamp
  */
  ReplicaInfo(FsVolumeSpi vol, long blockId, long len, long genStamp) {
    super(blockId, len, genStamp);
    this.volume = vol;
  }
  
  /**
   * Copy constructor.
   * @param from where to copy from
   */
  ReplicaInfo(ReplicaInfo from) {
    this(from, from.getVolume());
  }

  /**
   * @return the volume where this replica is located on disk
   */
  public FsVolumeSpi getVolume() {
    return volume;
  }

  /**
   * Get the {@link FileIoProvider} for disk IO operations.
   */
  public FileIoProvider getFileIoProvider() {
    // In tests and when invoked via FsDatasetUtil#computeChecksum, the
    // target volume for this replica may be unknown and hence null.
    // Use the DEFAULT_FILE_IO_PROVIDER with no-op hooks.
    return (volume != null) ? volume.getFileIoProvider()
        : DEFAULT_FILE_IO_PROVIDER;
  }

  /**
   * Set the volume where this replica is located on disk.
   */
  void setVolume(FsVolumeSpi vol) {
    this.volume = vol;
  }

  /**
   * Get the storageUuid of the volume that stores this replica.
   */
  @Override
  public String getStorageUuid() {
    return volume.getStorageID();
  }

  /**
   * Number of bytes reserved for this replica on disk.
   */
  public long getBytesReserved() {
    return 0;
  }

  /**
   * Get the {@code URI} for where the data of this replica is stored.
   * @return {@code URI} for the location of replica data.
   */
  abstract public URI getBlockURI();

  /**
   * Returns an {@link InputStream} to the replica's data.
   * @param seekOffset the offset at which the read is started from.
   * @return the {@link InputStream} to read the replica data.
   * @throws IOException if an error occurs in opening a stream to the data.
   */
  abstract public InputStream getDataInputStream(long seekOffset)
      throws IOException;

  /**
   * Returns an {@link OutputStream} to the replica's data.
   * @param append indicates if the block should be opened for append.
   * @return the {@link OutputStream} to write to the replica.
   * @throws IOException if an error occurs in creating an {@link OutputStream}.
   */
  abstract public OutputStream getDataOutputStream(boolean append)
      throws IOException;

  /**
   * @return true if the replica's data exists.
   */
  abstract public boolean blockDataExists();

  /**
   * Used to deletes the replica's block data.
   *
   * @return true if the replica's data is successfully deleted.
   */
  abstract public boolean deleteBlockData();

  /**
   * @return the length of the block on storage.
   */
  abstract public long getBlockDataLength();

  /**
   * Get the {@code URI} for where the metadata of this replica is stored.
   *
   * @return {@code URI} for the location of replica metadata.
   */
  abstract public URI getMetadataURI();

  /**
   * Returns an {@link InputStream} to the replica's metadata.
   * @param offset the offset at which the read is started from.
   * @return the {@link LengthInputStream} to read the replica metadata.
   * @throws IOException
   */
  abstract public LengthInputStream getMetadataInputStream(long offset)
      throws IOException;

  /**
   * Returns an {@link OutputStream} to the replica's metadata.
   * @param append indicates if the block metadata should be opened for append.
   * @return the {@link OutputStream} to write to the replica's metadata.
   * @throws IOException if an error occurs in creating an {@link OutputStream}.
   */
  abstract public OutputStream getMetadataOutputStream(boolean append)
      throws IOException;

  /**
   * @return true if the replica's metadata exists.
   */
  abstract public boolean metadataExists();

  /**
   * Used to deletes the replica's metadata.
   *
   * @return true if the replica's metadata is successfully deleted.
   */
  abstract public boolean deleteMetadata();

  /**
   * @return the length of the metadata on storage.
   */
  abstract public long getMetadataLength();

  /**
   * Rename the metadata {@link URI} to that referenced by {@code destURI}.
   *
   * @param destURI the target {@link URI}.
   * @return true if the rename is successful.
   * @throws IOException if an exception occurs in the rename.
   */
  abstract public boolean renameMeta(URI destURI) throws IOException;

  /**
   * Rename the data {@link URI} to that referenced by {@code destURI}.
   *
   * @param destURI the target {@link URI}.
   * @return true if the rename is successful.
   * @throws IOException if an exception occurs in the rename.
   */
  abstract public boolean renameData(URI destURI) throws IOException;

  /**
   * Update this replica with the {@link StorageLocation} found.
   * @param replicaLocation the {@link StorageLocation} found for this replica.
   */
  abstract public void updateWithReplica(StorageLocation replicaLocation);

  /**
   * Check whether the block was pinned.
   * @param localFS the local filesystem to use.
   * @return true if the block is pinned.
   * @throws IOException
   */
  abstract public boolean getPinning(LocalFileSystem localFS)
      throws IOException;

  /**
   * Set a block to be pinned on this datanode so that it cannot be moved
   * by Balancer/Mover.
   *
   * @param localFS the local filesystem to use.
   * @throws IOException if there is an exception in the pinning.
   */
  abstract public void setPinning(LocalFileSystem localFS) throws IOException;

  /**
   * Bump a replica's generation stamp to a new one.
   * Its on-disk meta file name is renamed to be the new one too.
   *
   * @param newGS new generation stamp
   * @throws IOException if the change fails
   */
  abstract public void bumpReplicaGS(long newGS) throws IOException;

  abstract public ReplicaInfo getOriginalReplica();

  /**
   * Get the recovery id.
   * @return the generation stamp that the replica will be bumped to
   */
  abstract public long getRecoveryID();

  /**
   * Set the recovery id.
   * @param recoveryId the new recoveryId
   */
  abstract public void setRecoveryID(long recoveryId);

  abstract public boolean breakHardLinksIfNeeded() throws IOException;

  abstract public ReplicaRecoveryInfo createInfo();

  abstract public int compareWith(ScanInfo info);

  abstract public void truncateBlock(long newLength) throws IOException;

  abstract public void copyMetadata(URI destination) throws IOException;

  abstract public void copyBlockdata(URI destination) throws IOException;

  /**
   * Number of bytes originally reserved for this replica. The actual
   * reservation is adjusted as data is written to disk.
   *
   * @return the number of bytes originally reserved for this replica.
   */
  public long getOriginalBytesReserved() {
    return 0;
  }

  @Override  //Object
  public String toString() {
    return getClass().getSimpleName()
        + ", " + super.toString()
        + ", " + getState()
        + "\n  getNumBytes()     = " + getNumBytes()
        + "\n  getBytesOnDisk()  = " + getBytesOnDisk()
        + "\n  getVisibleLength()= " + getVisibleLength()
        + "\n  getVolume()       = " + getVolume()
        + "\n  getBlockURI()     = " + getBlockURI();
  }

  @Override
  public boolean isOnTransientStorage() {
    return volume.isTransientStorage();
  }

  @Override
  public LightWeightResizableGSet.LinkedElement getNext() {
    return next;
  }

  @Override
  public void setNext(LightWeightResizableGSet.LinkedElement next) {
    this.next = next;
  }
}
