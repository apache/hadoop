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


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
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
  public static abstract class Factory<D extends FsDatasetSpi<?>> {
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
   * Create rolling logs.
   *
   * @param prefix the prefix of the log names.
   * @return rolling logs
   */
  public RollingLogs createRollingLogs(String bpid, String prefix
      ) throws IOException;

  /** @return a list of volumes. */
  public List<V> getVolumes();

  /** @return a storage with the given storage ID */
  public DatanodeStorage getStorage(final String storageUuid);

  /** @return one or more storage reports for attached volumes. */
  public StorageReport[] getStorageReports(String bpid)
      throws IOException;

  /** @return the volume that contains a replica of the block. */
  public V getVolume(ExtendedBlock b);

  /** @return a volume information map (name => info). */
  public Map<String, Object> getVolumeInfoMap();

  /** @return a list of finalized blocks for the given block pool. */
  public List<FinalizedReplica> getFinalizedBlocks(String bpid);

  /**
   * Check whether the in-memory block record matches the block on the disk,
   * and, in case that they are not matched, update the record or mark it
   * as corrupted.
   */
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol);

  /**
   * @param b - the block
   * @return a stream if the meta-data of the block exists;
   *         otherwise, return null.
   * @throws IOException
   */
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException;

  /**
   * Returns the specified block's on-disk length (excluding metadata)
   * @param b
   * @return   the specified block's on-disk length (excluding metadta)
   * @throws IOException
   */
  public long getLength(ExtendedBlock b) throws IOException;

  /**
   * Get reference to the replica meta info in the replicasMap. 
   * To be called from methods that are synchronized on {@link FSDataset}
   * @param blockId
   * @return replica from the replicas map
   */
  @Deprecated
  public Replica getReplica(String bpid, long blockId);

  /**
   * @return replica meta information
   */
  public String getReplicaString(String bpid, long blockId);

  /**
   * @return the generation stamp stored with the block.
   */
  public Block getStoredBlock(String bpid, long blkid) throws IOException;
  
  /**
   * Returns an input stream at specified offset of the specified block
   * @param b
   * @param seekOffset
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
            throws IOException;

  /**
   * Returns an input stream at specified offset of the specified block
   * The block is still in the tmp directory and is not finalized
   * @param b
   * @param blkoff
   * @param ckoff
   * @return an input stream to read the contents of the specified block,
   *  starting at the offset
   * @throws IOException
   */
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException;

  /**
   * Creates a temporary replica and returns the meta information of the replica
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface createTemporary(ExtendedBlock b
      ) throws IOException;

  /**
   * Creates a RBW replica and returns the meta info of the replica
   * 
   * @param b block
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface createRbw(ExtendedBlock b
      ) throws IOException;

  /**
   * Recovers a RBW replica and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param minBytesRcvd the minimum number of bytes that the replica could have
   * @param maxBytesRcvd the maximum number of bytes that the replica could have
   * @return the meta info of the replica which is being written to
   * @throws IOException if an error occurs
   */
  public ReplicaInPipelineInterface recoverRbw(ExtendedBlock b, 
      long newGS, long minBytesRcvd, long maxBytesRcvd) throws IOException;

  /**
   * Covert a temporary replica to a RBW.
   * @param temporary the temporary replica being converted
   * @return the result RBW
   */
  public ReplicaInPipelineInterface convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException;

  /**
   * Append to a finalized replica and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meata info of the replica which is being written to
   * @throws IOException
   */
  public ReplicaInPipelineInterface append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;

  /**
   * Recover a failed append to a finalized replica
   * and returns the meta info of the replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the meta info of the replica which is being written to
   * @throws IOException
   */
  public ReplicaInPipelineInterface recoverAppend(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException;
  
  /**
   * Recover a failed pipeline close
   * It bumps the replica's generation stamp and finalize it if RBW replica
   * 
   * @param b block
   * @param newGS the new generation stamp for the replica
   * @param expectedBlockLen the number of bytes the replica is expected to have
   * @return the storage uuid of the replica.
   * @throws IOException
   */
  public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen
      ) throws IOException;
  
  /**
   * Finalizes the block previously opened for writing using writeToBlock.
   * The block size is what is in the parameter b and it must match the amount
   *  of data written
   * @param b
   * @throws IOException
   */
  public void finalizeBlock(ExtendedBlock b) throws IOException;

  /**
   * Unfinalizes the block previously opened for writing using writeToBlock.
   * The temporary file associated with this block is deleted.
   * @param b
   * @throws IOException
   */
  public void unfinalizeBlock(ExtendedBlock b) throws IOException;

  /**
   * Returns one block report per volume.
   * @param bpid Block Pool Id
   * @return - a map of DatanodeStorage to block report for the volume.
   */
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid);

  /**
   * Returns the cache report - the full list of cached block IDs of a
   * block pool.
   * @param   bpid Block Pool Id
   * @return  the cache report - the full list of cached block IDs.
   */
  public List<Long> getCacheReport(String bpid);

  /** Does the dataset contain the block? */
  public boolean contains(ExtendedBlock block);

  /**
   * Is the block valid?
   * @param b
   * @return - true if the specified block is valid
   */
  public boolean isValidBlock(ExtendedBlock b);

  /**
   * Is the block a valid RBW?
   * @param b
   * @return - true if the specified block is a valid RBW
   */
  public boolean isValidRbw(ExtendedBlock b);

  /**
   * Invalidates the specified blocks
   * @param bpid Block pool Id
   * @param invalidBlks - the blocks to be invalidated
   * @throws IOException
   */
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException;

  /**
   * Caches the specified blocks
   * @param bpid Block pool id
   * @param blockIds - block ids to cache
   */
  public void cache(String bpid, long[] blockIds);

  /**
   * Uncaches the specified blocks
   * @param bpid Block pool id
   * @param blockIds - blocks ids to uncache
   */
  public void uncache(String bpid, long[] blockIds);

  /**
   * Determine if the specified block is cached.
   * @param bpid Block pool id
   * @param blockIds - block id
   * @returns true if the block is cached
   */
  public boolean isCached(String bpid, long blockId);

    /**
     * Check if all the data directories are healthy
     * @throws DiskErrorException
     */
  public void checkDataDir() throws DiskErrorException;

  /**
   * Shutdown the FSDataset
   */
  public void shutdown();

  /**
   * Sets the file pointer of the checksum stream so that the last checksum
   * will be overwritten
   * @param b block
   * @param outs The streams for the data file and checksum file
   * @param checksumSize number of bytes each checksum has
   * @throws IOException
   */
  public void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams outs, int checksumSize) throws IOException;

  /**
   * Checks how many valid storage volumes there are in the DataNode.
   * @return true if more than the minimum number of valid volumes are left 
   * in the FSDataSet.
   */
  public boolean hasEnoughResource();

  /**
   * Get visible length of the specified replica.
   */
  long getReplicaVisibleLength(final ExtendedBlock block) throws IOException;

  /**
   * Initialize a replica recovery.
   * @return actual state of the replica on this data-node or 
   * null if data-node does not have the replica.
   */
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock
      ) throws IOException;

  /**
   * Update replica's generation stamp and length and finalize it.
   * @return the ID of storage that stores the block
   */
  public String updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newLength) throws IOException;

  /**
   * add new block pool ID
   * @param bpid Block pool Id
   * @param conf Configuration
   */
  public void addBlockPool(String bpid, Configuration conf) throws IOException;
  
  /**
   * Shutdown and remove the block pool from underlying storage.
   * @param bpid Block pool Id to be removed
   */
  public void shutdownBlockPool(String bpid) ;
  
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
  public void deleteBlockPool(String bpid, boolean force) throws IOException;
  
  /**
   * Get {@link BlockLocalPathInfo} for the given block.
   */
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b
      ) throws IOException;

  /**
   * Get a {@link HdfsBlocksMetadata} corresponding to the list of blocks in 
   * <code>blocks</code>.
   * 
   * @param bpid pool to query
   * @param blockIds List of block ids for which to return metadata
   * @return metadata Metadata for the list of blocks
   * @throws IOException
   */
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String bpid,
      long[] blockIds) throws IOException;

  /**
   * Enable 'trash' for the given dataset. When trash is enabled, files are
   * moved to a separate trash directory instead of being deleted immediately.
   * This can be useful for example during rolling upgrades.
   */
  public void enableTrash(String bpid);

  /**
   * Restore trash
   */
  public void restoreTrash(String bpid);

  /**
   * @return true when trash is enabled
   */
  public boolean trashEnabled(String bpid);
}

