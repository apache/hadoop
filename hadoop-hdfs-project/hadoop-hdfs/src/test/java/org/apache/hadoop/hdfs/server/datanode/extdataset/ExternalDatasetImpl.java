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

package org.apache.hadoop.hdfs.server.datanode.extdataset;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.metrics2.MetricsCollector;

public class ExternalDatasetImpl implements FsDatasetSpi<ExternalVolumeImpl> {

  private final DatanodeStorage storage = new DatanodeStorage(
      DatanodeStorage.generateUuid(), DatanodeStorage.State.NORMAL,
      StorageType.DEFAULT);

  @Override
  public FsVolumeReferences getFsVolumeReferences() {
    return null;
  }

  @Override
  public void addVolume(StorageLocation location, List<NamespaceInfo> nsInfos)
      throws IOException {
  }

  @Override
  public void removeVolumes(Collection<StorageLocation> volumes,
      boolean clearFailure) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatanodeStorage getStorage(String storageUuid) {
    return null;
  }

  @Override
  public StorageReport[] getStorageReports(String bpid) throws IOException {
    StorageReport[] result = new StorageReport[1];
    result[0] = new StorageReport(storage, false, 0, 0, 0, 0, 0);
    return result;
  }

  @Override
  public ExternalVolumeImpl getVolume(ExtendedBlock b) {
    return null;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    return null;
  }

  @Override
  public List<ReplicaInfo> getFinalizedBlocks(String bpid) {
    return null;
  }

  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) {
  }

  @Override
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    return new LengthInputStream(null, 0);
  }

  @Override
  public long getLength(ExtendedBlock b) throws IOException {
    return 0;
  }

  @Override
  @Deprecated
  public Replica getReplica(String bpid, long blockId) {
    return new ExternalReplica();
  }

  @Override
  public String getReplicaString(String bpid, long blockId) {
    return null;
  }

  @Override
  public Block getStoredBlock(String bpid, long blkid) throws IOException {
    return new Block();
  }

  @Override
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
      throws IOException {
    return null;
  }

  @Override
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException {
    return new ReplicaInputStreams(null, null, null);
  }

  @Override
  public ReplicaHandler createTemporary(StorageType t, ExtendedBlock b)
      throws IOException {
    return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
  }

  @Override
  public ReplicaHandler createRbw(StorageType t, ExtendedBlock b, boolean tf)
      throws IOException {
    return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
  }

  @Override
  public ReplicaHandler recoverRbw(ExtendedBlock b, long newGS,
      long minBytesRcvd, long maxBytesRcvd) throws IOException {
    return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
  }

  @Override
  public ReplicaInPipeline convertTemporaryToRbw(
      ExtendedBlock temporary) throws IOException {
    return new ExternalReplicaInPipeline();
  }

  @Override
  public ReplicaHandler append(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
  }

  @Override
  public ReplicaHandler recoverAppend(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    return new ReplicaHandler(new ExternalReplicaInPipeline(), null);
  }

  @Override
  public Replica recoverClose(ExtendedBlock b, long newGS, long expectedBlkLen)
      throws IOException {
    return null;
  }

  @Override
  public void finalizeBlock(ExtendedBlock b) throws IOException {
  }

  @Override
  public void unfinalizeBlock(ExtendedBlock b) throws IOException {
  }

  @Override
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {
    final Map<DatanodeStorage, BlockListAsLongs> result =
	new HashMap<DatanodeStorage, BlockListAsLongs>();

    result.put(storage, BlockListAsLongs.EMPTY);
    return result;
  }

  @Override
  public List<Long> getCacheReport(String bpid) {
    return null;
  }

  @Override
  public boolean contains(ExtendedBlock block) {
    return false;
  }

  @Override
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state) throws ReplicaNotFoundException, UnexpectedReplicaStateException, FileNotFoundException, EOFException, IOException {

  }

  @Override
  public boolean isValidBlock(ExtendedBlock b) {
    return false;
  }

  @Override
  public boolean isValidRbw(ExtendedBlock b) {
    return false;
  }

  @Override
  public void invalidate(String bpid, Block[] invalidBlks) throws IOException {
  }

  @Override
  public void cache(String bpid, long[] blockIds) {
  }

  @Override
  public void uncache(String bpid, long[] blockIds) {
  }

  @Override
  public boolean isCached(String bpid, long blockId) {
    return false;
  }

  @Override
  public Set<StorageLocation> checkDataDir() {
    return null;
  }

  @Override
  public void shutdown() {
  }

  @Override
  public void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams outs, int checksumSize) throws IOException {
  }

  @Override
  public boolean hasEnoughResource() {
    return false;
  }

  @Override
  public long getReplicaVisibleLength(ExtendedBlock block) throws IOException {
    return 0;
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    return new ReplicaRecoveryInfo(0, 0, 0, ReplicaState.FINALIZED);
  }

  @Override
  public Replica updateReplicaUnderRecovery(ExtendedBlock oldBlock,
      long recoveryId, long newBlockId, long newLength) throws IOException {
    return null;
  }

  @Override
  public void addBlockPool(String bpid, Configuration conf) throws IOException {
  }

  @Override
  public void shutdownBlockPool(String bpid) {
  }

  @Override
  public void deleteBlockPool(String bpid, boolean force) throws IOException {
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b)
      throws IOException {
    return new BlockLocalPathInfo(null, "file", "metafile");
  }

  @Override
  public void enableTrash(String bpid) {

  }

  @Override
  public void clearTrash(String bpid) {

  }

  @Override
  public boolean trashEnabled(String bpid) {
    return false;
  }

  @Override
  public void setRollingUpgradeMarker(String bpid) throws IOException {

  }

  @Override
  public void clearRollingUpgradeMarker(String bpid) throws IOException {

  }

  @Override
  public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block, FileDescriptor fd, long offset, long nbytes, int flags) {

  }

  @Override
  public void onCompleteLazyPersist(String bpId, long blockId, long creationTime, File[] savedFiles, ExternalVolumeImpl targetVolume) {

  }

  @Override
  public void onFailLazyPersist(String bpId, long blockId) {

  }

  @Override
  public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block, StorageType targetStorageType) throws IOException {
    return null;
  }

  @Override
  public long getBlockPoolUsed(String bpid) throws IOException {
    return 0;
  }

  @Override
  public long getDfsUsed() throws IOException {
    return 0;
  }

  @Override
  public long getCapacity() throws IOException {
    return 0;
  }

  @Override
  public long getRemaining() throws IOException {
    return 0;
  }

  @Override
  public String getStorageInfo() {
    return null;
  }

  @Override
  public int getNumFailedVolumes() {
    return 0;
  }

  @Override
  public String[] getFailedStorageLocations() {
    return null;
  }

  @Override
  public long getLastVolumeFailureDate() {
    return 0;
  }

  @Override
  public long getEstimatedCapacityLostTotal() {
    return 0;
  }

  @Override
  public VolumeFailureSummary getVolumeFailureSummary() {
    return null;
  }

  @Override
  public long getCacheUsed() {
    return 0;
  }

  @Override
  public long getCacheCapacity() {
    return 0;
  }

  @Override
  public long getNumBlocksCached() {
    return 0;
  }

  @Override
  public long getNumBlocksFailedToCache() {
    return 0;
  }

  @Override
  public long getNumBlocksFailedToUncache() {
    return 0;
  }

  /**
   * Get metrics from the metrics source
   *
   * @param collector to contain the resulting metrics snapshot
   * @param all if true, return all metrics even if unchanged.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    try {
      DataNodeMetricHelper.getMetrics(collector, this, "ExternalDataset");
    } catch (Exception e){
        //ignore exceptions
    }
  }

  @Override
  public void setPinning(ExtendedBlock block) throws IOException {    
  }

  @Override
  public boolean getPinning(ExtendedBlock block) throws IOException {
    return false;
  }
  
  @Override
  public boolean isDeletingBlock(String bpid, long blockId) {
    return false;
  }

  @Override
  public ReplicaInfo moveBlockAcrossVolumes(ExtendedBlock block,
                                            FsVolumeSpi destination)
      throws IOException {
    return null;
  }

  @Override
  public AutoCloseableLock acquireDatasetLock() {
    return null;
  }
}
