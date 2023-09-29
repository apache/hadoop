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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;


import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.common.AutoCloseDataSetLock;
import org.apache.hadoop.hdfs.server.common.DataNodeLockManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MountVolumeMap;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This is a service provider interface for the underlying storage that
 * stores replicas for a data node.
 * The default implementation stores replicas on local drives. 
 */
@InterfaceAudience.Private
public interface FsDatasetSpi<V extends FsVolumeSpi> extends FSDatasetMBean {
  /**
   * A factory for creating {@link FsDatasetSpi} objects.
   */
  abstract class Factory<D extends FsDatasetSpi<?>> {
    /** @return the configured factory. */
    public static Factory<?> getFactory(Configuration conf) {
      @SuppressWarnings("rawtypes")
      final Class<? extends Factory> clazz = conf.getClass(
          DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
          FsDatasetFactory.class,
          Factory.class);
      return ReflectionUtils.newInstance(clazz, conf);
    }

    /** Create a new object. */
    public abstract D newInstance(DataNode datanode, DataStorage storage,
        Configuration conf) throws IOException;

    /** Does the factory create simulated objects? */
    public boolean isSimulated() {
      return false;
    }
  }

  /**
   * It behaviors as an unmodifiable list of FsVolume. Individual FsVolume can
   * be obtained by using {@link #get(int)}.
   *
   * This also holds the reference counts for these volumes. It releases all the
   * reference counts in {@link #close()}.
   */
  class FsVolumeReferences implements Iterable<FsVolumeSpi>, Closeable {
    private final List<FsVolumeReference> references;

    public <S extends FsVolumeSpi> FsVolumeReferences(List<S> curVolumes) {
      references = new ArrayList<>();
      for (FsVolumeSpi v : curVolumes) {
        try {
          references.add(v.obtainReference());
        } catch (ClosedChannelException e) {
          // This volume has been closed.
        }
      }
    }

    private static class FsVolumeSpiIterator implements
        Iterator<FsVolumeSpi> {
      private final List<FsVolumeReference> references;
      private int idx = 0;

      FsVolumeSpiIterator(List<FsVolumeReference> refs) {
        references = refs;
      }

      @Override
      public boolean hasNext() {
        return idx < references.size();
      }

      @Override
      public FsVolumeSpi next() {
        int refIdx = idx++;
        return references.get(refIdx).getVolume();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public Iterator<FsVolumeSpi> iterator() {
      return new FsVolumeSpiIterator(references);
    }

    /**
     * Get the number of volumes.
     */
    public int size() {
      return references.size();
    }

    /**
     * Get the volume for a given index.
     */
    public FsVolumeSpi get(int index) {
      return references.get(index).getVolume();
    }

    /**
     * Get the reference for a given index.
     */
    public FsVolumeReference getReference(int index) {
      return references.get(index);
    }

    @Override
    public void close() throws IOException {
      IOException ioe = null;
      for (FsVolumeReference ref : references) {
        try {
          ref.close();
        } catch (IOException e) {
          ioe = e;
        }
      }
      references.clear();
      if (ioe != null) {
        throw ioe;
      }
    }
  }

  /**
   * Returns a list of FsVolumes that hold reference counts.
   *
   * The caller must release the reference of each volume by calling
   * {@link FsVolumeReferences#close()}.
   */
  FsVolumeReferences getFsVolumeReferences();

  /**
   * Add a new volume to the FsDataset.
   *
   * If the FSDataset supports block scanning, this function registers
   * the new volume with the block scanner.
   *
   * @param location      The storage location for the new volume.
   * @param nsInfos       Namespace information for the new volume.
   */
  void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException;

  /**
   * Removes a collection of volumes from FsDataset.
   *
   * If the FSDataset supports block scanning, this function removes
   * the volumes from the block scanner.
   *
   * @param volumes  The paths of the volumes to be removed.
   * @param clearFailure set true to clear the failure information about the
   *                     volumes.
   */
  void removeVolumes(Collection<StorageLocation> volumes, boolean clearFailure);

  /** @return a storage with the given storage ID */
  DatanodeStorage getStorage(final String storageUuid);

  /** @return one or more storage reports for attached volumes. */
  StorageReport[] getStorageReports(String bpid)
      throws IOException;

  /** @return the volume that contains a replica of the block. */
  V getVolume(ExtendedBlock b);

  /** @return a volume information map (name {@literal =>} info). */
  Map<String, Object> getVolumeInfoMap();

  /**
   * Returns info about volume failures.
   *
   * @return info about volume failures, possibly null
   */
  VolumeFailureSummary getVolumeFailureSummary();

  /**
   * Gets a list of references to the finalized blocks for the given block pool.
   * <p>
   * Callers of this function should call
   * {@link FsDatasetSpi#acquireDatasetLockManager} to avoid blocks' status being
   * changed during list iteration.
   * </p>
   * @return a list of references to the finalized blocks for the given block
   *         pool.
   */
  List<ReplicaInfo> getFinalizedBlocks(String bpid);

  /**
   * Check whether the in-memory block record matches the block on the disk,
   * and, in case that they are not matched, update the record or mark it
   * as corrupted.
   */
  void checkAndUpdate(String bpid, ScanInfo info) throws IOException;

  /**
   * @param b - the block
   * @return a stream if the meta-data of the block exists;
   *         otherwise, return null.
   * @throws IOException
   */
  LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException;

  /**
   * Returns the specified block's on-disk length (excluding metadata).
   * @return   the specified block's on-disk length (excluding metadta)
   * @throws IOException on error
   */
  long getLength(ExtendedBlock b) throws IOException;

  /**
   * Get reference to the replica meta info in the replicasMap. 
   * To be called from methods that are synchronized on
   * implementations of {@link FsDatasetSpi}
   * @return replica from the replicas map
   */
  @Deprecated
  Replica getReplica(String bpid, long blockId);

  /**
   * @return replica meta information
   */
  String getReplicaString(String bpid, long blockId);

  /**
   * @return the generation stamp stored with the block.
   */
  Block getStoredBlock(String bpid, long blkid) throws IOException;

  /**
   * Returns an input stream at specified offset of the specified block.
   * @param b block
   * @param seekOffset offset with in the block to seek to
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
            throws IOException;

  /**
   * Returns an input stream at specified offset of the specified block.
   * The block is still in the tmp directory and is not finalized
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException;

  /**
   * Creates a temporary replica and returns the meta information of the replica
   * .
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  ReplicaHandler createTemporary(StorageType storageType, String storageId,
      ExtendedBlock b, boolean isTransfer) throws IOException;

  /**
   * Creates a RBW replica and returns the meta info of the replica
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  ReplicaHandler createRbw(StorageType storageType, String storageId,
      ExtendedBlock b, boolean allowLazyPersist) throws IOException;

  /**
   * Recovers a RBW replica and returns the meta info of the replica.
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param minBytesRcvd the minimum number of bytes that the replica could have
   * @param maxBytesRcvd the maximum number of bytes that the replica could have
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  ReplicaHandler recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException;

  /**
   * Covert a temporary replica to a RBW.
   * @param temporary the temporary replica being converted
   * @return the result RBW
   */
  ReplicaInPipeline convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException;

  /**
   * Append to a finalized replica and returns the meta info of the replica.
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meata info of the replica which is being written to
   * @throws IOException
   */
  ReplicaHandler append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;

  /**
   * Recover a failed append to a finalized replica and returns the meta
   * info of the replica.
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meta info of the replica which is being written to
   * @throws IOException
   */
  ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException;
  
  /**
   * Recover a failed pipeline close.
   * It bumps the replica's generation stamp and finalize it if RBW replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the storage uuid of the replica.
   * @throws IOException
   */
  Replica recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
      ) throws IOException;
  
  /**
   * Finalizes the block previously opened for writing using writeToBlock.
   * The block size is what is in the parameter b and it must match the amount
   *  of data written
   * @param b Block to be finalized
   * @param fsyncDir whether to sync the directory changes to durable device.
   * @throws IOException
   * @throws ReplicaNotFoundException if the replica can not be found when the
   * block is been finalized. For instance, the block resides on an HDFS volume
   * that has been removed.
   */
  void finalizeBlock(ExtendedBlock b, boolean fsyncDir) throws IOException;

  /**
   * Unfinalizes the block previously opened for writing using writeToBlock.
   * The temporary file associated with this block is deleted.
   * @throws IOException
   */
  void unfinalizeBlock(ExtendedBlock b) throws IOException;

  /**
   * Returns one block report per volume.
   * @param bpid Block Pool Id
   * @return - a map of DatanodeStorage to block report for the volume.
   */
  Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid);

  /**
   * Returns the cache report - the full list of cached block IDs of a
   * block pool.
   * @param   bpid Block Pool Id
   * @return  the cache report - the full list of cached block IDs.
   */
  List<Long> getCacheReport(String bpid);

  /** Does the dataset contain the block? */
  boolean contains(ExtendedBlock block);

  /**
   * Check if a block is valid.
   *
   * @param b           The block to check.
   * @param minLength   The minimum length that the block must have.  May be 0.
   * @param state       If this is null, it is ignored.  If it is non-null, we
   *                        will check that the replica has this state.
   *
   * @throws ReplicaNotFoundException          If the replica is not found
   *
   * @throws UnexpectedReplicaStateException   If the replica is not in the 
   *                                             expected state.
   * @throws FileNotFoundException             If the block file is not found or there 
   *                                              was an error locating it.
   * @throws EOFException                      If the replica length is too short.
   * 
   * @throws IOException                       May be thrown from the methods called. 
   */
  void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException,
      FileNotFoundException, EOFException, IOException;
      
  
  /**
   * Is the block valid?
   * @return - true if the specified block is valid
   */
  boolean isValidBlock(ExtendedBlock b);

  /**
   * Is the block a valid RBW?
   * @return - true if the specified block is a valid RBW
   */
  boolean isValidRbw(ExtendedBlock b);

  /**
   * Invalidates the specified blocks
   * @param bpid Block pool Id
   * @param invalidBlks - the blocks to be invalidated
   * @throws IOException
   */
  void invalidate(String bpid, Block invalidBlks[]) throws IOException;

  /**
   * Invalidate a block which is not found on disk.
   * @param bpid the block pool ID.
   * @param block The block to be invalidated.
   */
  void invalidateMissingBlock(String bpid, Block block) throws IOException;

  /**
   * Caches the specified block
   * @param bpid Block pool id
   * @param blockIds - block ids to cache
   */
  void cache(String bpid, long[] blockIds);

  /**
   * Uncaches the specified blocks
   * @param bpid Block pool id
   * @param blockIds - blocks ids to uncache
   */
  void uncache(String bpid, long[] blockIds);

  /**
   * Determine if the specified block is cached.
   * @param bpid Block pool id
   * @param blockId - block id
   * @return true if the block is cached
   */
  boolean isCached(String bpid, long blockId);

    /**
     * Check if all the data directories are healthy
     * @param failedVolumes
     */
  void handleVolumeFailures(Set<FsVolumeSpi> failedVolumes);

  /**
   * Shutdown the FSDataset
   */
  void shutdown();

  /**
   * Sets the file pointer of the checksum stream so that the last checksum
   * will be overwritten
   * @param b block
   * @param outs The streams for the data file and checksum file
   * @param checksumSize number of bytes each checksum has
   * @throws IOException
   */
  void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams outs, int checksumSize) throws IOException;

  /**
   * Checks how many valid storage volumes there are in the DataNode.
   * @return true if more than the minimum number of valid volumes are left 
   * in the FSDataSet.
   */
  boolean hasEnoughResource();

  /**
   * Get visible length of the specified replica.
   */
  long getReplicaVisibleLength(final ExtendedBlock block) throws IOException;

  /**
   * Initialize a replica recovery.
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   */
  ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock
      ) throws IOException;

  /**
   * Update replica's generation stamp and length and finalize it.
   * @return the ID of storage that stores the block
   */
  Replica updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException;

  /**
   * add new block pool ID
   * @param bpid Block pool Id
   * @param conf Configuration
   */
  void addBlockPool(String bpid, Configuration conf) throws IOException;

  /**
   * Shutdown and remove the block pool from underlying storage.
   * @param bpid Block pool Id to be removed
   */
  void shutdownBlockPool(String bpid) ;

  /**
   * Deletes the block pool directories. If force is false, directories are 
   * deleted only if no block files exist for the block pool. If force 
   * is true entire directory for the blockpool is deleted along with its
   * contents.
   * @param bpid BlockPool Id to be deleted.
   * @param force If force is false, directories are deleted only if no
   *        block files exist for the block pool, otherwise entire 
   *        directory for the blockpool is deleted along with its contents.
   * @throws IOException
   */
  void deleteBlockPool(String bpid, boolean force) throws IOException;

  /**
   * Get {@link BlockLocalPathInfo} for the given block.
   */
  BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b
      ) throws IOException;

  /**
   * Enable 'trash' for the given dataset. When trash is enabled, files are
   * moved to a separate trash directory instead of being deleted immediately.
   * This can be useful for example during rolling upgrades.
   */
  void enableTrash(String bpid);

  /**
   * Clear trash
   */
  void clearTrash(String bpid);

  /**
   * @return true when trash is enabled
   */
  boolean trashEnabled(String bpid);

  /**
   * Create a marker file indicating that a rolling upgrade is in progress.
   */
  void setRollingUpgradeMarker(String bpid) throws IOException;

  /**
   * Delete the rolling upgrade marker file if it exists.
   * @param bpid
   */
  void clearRollingUpgradeMarker(String bpid) throws IOException;

  /**
   * submit a sync_file_range request to AsyncDiskService.
   */
  void submitBackgroundSyncFileRangeRequest(final ExtendedBlock block,
      final ReplicaOutputStreams outs, final long offset, final long nbytes,
      final int flags);

  /**
   * Callback from RamDiskAsyncLazyPersistService upon async lazy persist task end
   */
  void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, V targetVolume);

   /**
    * Callback from RamDiskAsyncLazyPersistService upon async lazy persist task fail
    */
   void onFailLazyPersist(String bpId, long blockId);

    /**
     * Move block from one storage to another storage
     */
   ReplicaInfo moveBlockAcrossStorage(final ExtendedBlock block,
        StorageType targetStorageType, String storageId) throws IOException;

  /**
   * Set a block to be pinned on this datanode so that it cannot be moved
   * by Balancer/Mover.
   *
   * It is a no-op when dfs.datanode.block-pinning.enabled is set to false.
   */
  void setPinning(ExtendedBlock block) throws IOException;

  /**
   * Check whether the block was pinned
   */
  boolean getPinning(ExtendedBlock block) throws IOException;

  /**
   * Confirm whether the block is deleting
   */
  boolean isDeletingBlock(String bpid, long blockId);

  /**
   * Moves a given block from one volume to another volume. This is used by disk
   * balancer.
   *
   * @param block       - ExtendedBlock
   * @param destination - Destination volume
   * @return Old replica info
   */
  ReplicaInfo moveBlockAcrossVolumes(final ExtendedBlock block,
      FsVolumeSpi destination) throws IOException;

  /***
   * Acquire lock Manager for the data set. This prevents other threads from
   * modifying the volume map structure inside the datanode.
   * @return The AutoClosable read lock instance.
   */
  DataNodeLockManager<? extends AutoCloseDataSetLock> acquireDatasetLockManager();

  /**
   * Deep copy the replica info belonging to given block pool.
   * @param bpid Specified block pool id.
   * @return A set of replica info.
   * @throws IOException
   */
  Set<? extends Replica> deepCopyReplica(String bpid) throws IOException;

  /**
   * Get relationship between disk mount and FsVolume.
   * @return Disk mount and FsVolume relationship.
   * @throws IOException
   */
  MountVolumeMap getMountVolumeMap() throws IOException;

  /**
   * Get the volume list.
   */
  List<FsVolumeImpl> getVolumeList();

  /**
   * Set the last time in milliseconds when the directory scanner successfully ran.
   * @param time the last time in milliseconds when the directory scanner successfully ran.
   */
  default void setLastDirScannerFinishTime(long time) {}
}
