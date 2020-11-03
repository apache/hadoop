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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

/**
 * Provide block access for FsDataset white box tests.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface FsDatasetTestUtils {

  abstract class Factory<D extends FsDatasetTestUtils> {
    /**
     * By default, it returns FsDatasetImplTestUtilsFactory.
     *
     * @return The configured Factory.
     */
    public static Factory<?> getFactory(Configuration conf) {
      String className = conf.get(
          DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
          FsDatasetFactory.class.getName());
      Preconditions.checkState(className.contains("Factory"));
      className = className.replaceFirst("(\\$)?Factory$", "TestUtilsFactory");
      final Class<? extends Factory> clazz = conf.getClass(
          className,
          FsDatasetImplTestUtilsFactory.class,
          Factory.class);
      return ReflectionUtils.newInstance(clazz, conf);
    }

    /**
     * Create a new instance of FsDatasetTestUtils.
     */
    public abstract D newInstance(DataNode datanode);

    /**
     * @return True for SimulatedFsDataset
     */
    public boolean isSimulated() {
      return false;
    }

    /**
     * Get the default number of data directories for underlying storage per
     * DataNode.
     *
     * @return The default number of data dirs per DataNode.
     */
    abstract public int getDefaultNumOfDataDirs();
  }

  /**
   * A replica to be corrupted.
   *
   * It is safe to corrupt this replica even if the MiniDFSCluster is shutdown.
   */
  interface MaterializedReplica {

    /**
     * Corrupt the block file of the replica.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException if I/O error.
     */
    void corruptData() throws IOException;

    /**
     * Corrupt the block file with the given content.
     * @param newContent the new content written to the block file.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException if I/O error.
     */
    void corruptData(byte[] newContent) throws IOException;

    /**
     * Truncate the block file of the replica to the newSize.
     * @param newSize the new size of the block file.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException if I/O error.
     */
    void truncateData(long newSize) throws IOException;

    /**
     * Delete the block file of the replica.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException if I/O error.
     */
    void deleteData() throws IOException;

    /**
     * Corrupt the metadata file of the replica.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException if I/O error.
     */
    void corruptMeta() throws IOException;

    /**
     * Delete the metadata file of the replcia.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException I/O error.
     */
    void deleteMeta() throws IOException;

    /**
     * Truncate the metadata file of the replica to the newSize.
     * @throws FileNotFoundException if the block file does not exist.
     * @throws IOException I/O error.
     */
    void truncateMeta(long newSize) throws IOException;

    /**
     * Make the replica unreachable, perhaps by renaming it to an
     * invalid file name.
     * @throws IOException On I/O error.
     */
    void makeUnreachable() throws IOException;
  }

  /**
   * Get a materialized replica to corrupt its block / crc later.
   * @param block the block of this replica begone to.
   * @return a replica to corrupt. Return null if the replica does not exist
   * in this dataset.
   * @throws ReplicaNotFoundException if the replica does not exists on the
   *         dataset.
   */
  MaterializedReplica getMaterializedReplica(ExtendedBlock block)
          throws ReplicaNotFoundException;

  /**
   * Create a finalized replica and add it into the FsDataset.
   */
  Replica createFinalizedReplica(ExtendedBlock block) throws IOException;

  /**
   * Create a finalized replica on a particular volume, and add it into
   * the FsDataset.
   */
  Replica createFinalizedReplica(FsVolumeSpi volume, ExtendedBlock block)
      throws IOException;

  /**
   * Create a {@link ReplicaInPipeline} and add it into the FsDataset.
   */
  Replica createReplicaInPipeline(ExtendedBlock block) throws IOException;

  /**
   * Create a {@link ReplicaInPipeline} and add it into the FsDataset.
   */
  Replica createReplicaInPipeline(FsVolumeSpi volume, ExtendedBlock block)
      throws IOException;

  /**
   * Create a {@link ReplicaBeingWritten} and add it into the FsDataset.
   */
  Replica createRBW(ExtendedBlock block) throws IOException;

  /**
   * Create a {@link ReplicaBeingWritten} on the particular volume, and add it
   * into the FsDataset.
   */
  Replica createRBW(FsVolumeSpi volume, ExtendedBlock block) throws IOException;

  /**
   * Create a {@link ReplicaWaitingToBeRecovered} object and add it into the
   * FsDataset.
   */
  Replica createReplicaWaitingToBeRecovered(ExtendedBlock block)
      throws IOException;

  /**
   * Create a {@link ReplicaWaitingToBeRecovered} on the particular volume,
   * and add it into the FsDataset.
   */
  Replica createReplicaWaitingToBeRecovered(
      FsVolumeSpi volume, ExtendedBlock block) throws IOException;

  /**
   * Create a {@link ReplicaUnderRecovery} object and add it into the FsDataset.
   */
  Replica createReplicaUnderRecovery(ExtendedBlock block, long recoveryId)
      throws IOException;

  /**
   * Check the stored files / data of a replica.
   * @param replica a replica object.
   * @throws IOException
   */
  void checkStoredReplica(final Replica replica) throws IOException;

  /**
   * Create dummy replicas for block data and metadata.
   * @param block the block of which replica to be created.
   * @throws IOException on I/O error.
   */
  void injectCorruptReplica(ExtendedBlock block) throws IOException;

  /**
   * Get the replica of a block. Returns null if it does not exist.
   * @param block the block whose replica will be returned.
   * @return Replica for the block.
   */
  Replica fetchReplica(ExtendedBlock block);

  /**
   * @return The default value of number of data dirs per DataNode in
   * MiniDFSCluster.
   */
  int getDefaultNumOfDataDirs();

  /**
   * Obtain the raw capacity of underlying storage per DataNode.
   */
  long getRawCapacity() throws IOException;

  /**
   * Get the persistently stored length of the block.
   */
  long getStoredDataLength(ExtendedBlock block) throws IOException;

  /**
   * Get the persistently stored generation stamp.
   */
  long getStoredGenerationStamp(ExtendedBlock block) throws IOException;

  /**
   * Change the persistently stored generation stamp.
   * @param block the block whose generation stamp will be changed
   * @param newGenStamp the new generation stamp
   * @throws IOException
   */
  void changeStoredGenerationStamp(ExtendedBlock block, long newGenStamp)
      throws IOException;

  /** Get all stored replicas in the specified block pool. */
  Iterator<Replica> getStoredReplicas(String bpid) throws IOException;

  /**
   * Get the number of pending async deletions.
   */
  long getPendingAsyncDeletions();

  /**
   * Verify the existence of the block pool.
   *
   * @param bpid block pool ID
   * @throws IOException if the block pool does not exist.
   */
  void verifyBlockPoolExists(String bpid) throws IOException;

  /**
   * Verify that the block pool does not exist.
   *
   * @param bpid block pool ID
   * @throws IOException if the block pool does exist.
   */
  void verifyBlockPoolMissing(String bpid) throws IOException;
}