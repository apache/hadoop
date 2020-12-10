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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.thirdparty.com.google.common.math.LongMath;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImplTestUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;

/**
 * This class implements a simulated FSDataset.
 *
 * Blocks that are created are recorded but their data (plus their CRCs) are
 *  discarded.
 * Fixed data is returned when blocks are read; a null CRC meta file is
 * created for such data.
 *
 * This FSDataset does not remember any block information across its
 * restarts; it does however offer an operation to inject blocks
 *  (See the TestInectionForSImulatedStorage()
 * for a usage example of injection.
 *
 * Note the synchronization is coarse grained - it is at each method.
 */
public class SimulatedFSDataset implements FsDatasetSpi<FsVolumeSpi> {
  public final static int BYTE_MASK = 0xff;
  private final static int DEFAULT_NUM_SIMULATED_DATA_DIRS = 1;
  static class Factory extends FsDatasetSpi.Factory<SimulatedFSDataset> {
    @Override
    public SimulatedFSDataset newInstance(DataNode datanode,
        DataStorage storage, Configuration conf) throws IOException {
      return new SimulatedFSDataset(datanode, storage, conf);
    }

    @Override
    public boolean isSimulated() {
      return true;
    }
  }

  /**
   * Used to change the default number of data storages and to mark the
   * FSDataset as simulated.
   */
  static class TestUtilsFactory
      extends FsDatasetTestUtils.Factory<FsDatasetTestUtils> {
    @Override
    public FsDatasetTestUtils newInstance(DataNode datanode) {
      return new FsDatasetImplTestUtils(datanode) {
        @Override
        public int getDefaultNumOfDataDirs() {
          return DEFAULT_NUM_SIMULATED_DATA_DIRS;
        }
      };
    }

    @Override
    public boolean isSimulated() {
      return true;
    }

    @Override
    public int getDefaultNumOfDataDirs() {
      return DEFAULT_NUM_SIMULATED_DATA_DIRS;
    }

  }

  public static void setFactory(Configuration conf) {
    conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
        Factory.class.getName());
    conf.setClass("org.apache.hadoop.hdfs.server.datanode." +
            "SimulatedFSDatasetTestUtilsFactory",
        TestUtilsFactory.class, FsDatasetTestUtils.Factory.class
    );
  }

  public static byte simulatedByte(Block b, long offsetInBlk) {
    byte firstByte = (byte) (b.getBlockId() & BYTE_MASK);
    return (byte) ((firstByte + offsetInBlk % 29) & BYTE_MASK);
  }

  public static final String CONFIG_PROPERTY_CAPACITY =
      "dfs.datanode.simulateddatastorage.capacity";

  public static final long DEFAULT_CAPACITY = 2L<<40; // 1 terabyte

  public static final String CONFIG_PROPERTY_STATE =
      "dfs.datanode.simulateddatastorage.state";
  private static final DatanodeStorage.State DEFAULT_STATE =
      DatanodeStorage.State.NORMAL;

  static final byte[] nullCrcFileData;

  private final AutoCloseableLock datasetLock;
  private final FileIoProvider fileIoProvider;

  static {
    DataChecksum checksum = DataChecksum.newDataChecksum(
        DataChecksum.Type.NULL, 16*1024 );
    byte[] nullCrcHeader = checksum.getHeader();
    nullCrcFileData =  new byte[2 + nullCrcHeader.length];
    nullCrcFileData[0] = (byte) ((BlockMetadataHeader.VERSION >>> 8) & 0xff);
    nullCrcFileData[1] = (byte) (BlockMetadataHeader.VERSION & 0xff);
    for (int i = 0; i < nullCrcHeader.length; i++) {
      nullCrcFileData[i+2] = nullCrcHeader[i];
    }
  }

  // information about a single block
  private class BInfo implements ReplicaInPipeline {
    final Block theBlock;
    private boolean finalized = false; // if not finalized => ongoing creation
    SimulatedOutputStream oStream = null;
    private long bytesAcked;
    private long bytesRcvd;
    private boolean pinned = false;
    BInfo(String bpid, Block b, boolean forWriting) throws IOException {
      theBlock = new Block(b);
      if (theBlock.getNumBytes() < 0L) {
        theBlock.setNumBytes(0L);
      }
      if (!getStorage(theBlock).alloc(bpid, theBlock.getNumBytes())) {
        // expected length - actual length may
        // be more - we find out at finalize
        DataNode.LOG.warn("Lack of free storage on a block alloc");
        throw new IOException("Creating block, no free space available");
      }

      if (forWriting) {
        finalized = false;
        oStream = new SimulatedOutputStream();
      } else {
        finalized = true;
        oStream = null;
      }
    }

    @Override
    public String getStorageUuid() {
      return getStorage(theBlock).getStorageUuid();
    }

    @Override
    synchronized public long getGenerationStamp() {
      return theBlock.getGenerationStamp();
    }

    @Override
    synchronized public long getNumBytes() {
      if (!finalized) {
         return bytesRcvd;
      } else {
        return theBlock.getNumBytes();
      }
    }

    @Override
    synchronized public void setNumBytes(long length) {
      if (!finalized) {
         bytesRcvd = length;
      } else {
        theBlock.setNumBytes(length);
      }
    }

    synchronized SimulatedInputStream getIStream() {
      if (!finalized) {
        // throw new IOException("Trying to read an unfinalized block");
         return new SimulatedInputStream(oStream.getLength(), theBlock);
      } else {
        return new SimulatedInputStream(theBlock.getNumBytes(), theBlock);
      }
    }

    synchronized void finalizeBlock(String bpid, long finalSize)
        throws IOException {
      if (finalized) {
        throw new IOException(
            "Finalizing a block that has already been finalized" +
            theBlock.getBlockId());
      }
      if (oStream == null) {
        DataNode.LOG.error("Null oStream on unfinalized block - bug");
        throw new IOException("Unexpected error on finalize");
      }

      if (oStream.getLength() != finalSize) {
        DataNode.LOG.warn("Size passed to finalize (" + finalSize +
                    ")does not match what was written:" + oStream.getLength());
        throw new IOException(
          "Size passed to finalize does not match the amount of data written");
      }
      // We had allocated the expected length when block was created;
      // adjust if necessary
      long extraLen = finalSize - theBlock.getNumBytes();
      if (extraLen > 0L) {
        if (!getStorage(theBlock).alloc(bpid, extraLen)) {
          DataNode.LOG.warn("Lack of free storage on a block alloc");
          throw new IOException("Creating block, no free space available");
        }
      } else {
        getStorage(theBlock).free(bpid, -extraLen);
      }
      theBlock.setNumBytes(finalSize);

      finalized = true;
      oStream = null;
      return;
    }

    synchronized void unfinalizeBlock() throws IOException {
      if (!finalized) {
        throw new IOException("Unfinalized a block that's not finalized "
            + theBlock);
      }
      finalized = false;
      oStream = new SimulatedOutputStream();
      long blockLen = theBlock.getNumBytes();
      oStream.setLength(blockLen);
      bytesRcvd = blockLen;
      bytesAcked = blockLen;
    }

    SimulatedInputStream getMetaIStream() {
      return new SimulatedInputStream(nullCrcFileData);
    }

    synchronized boolean isFinalized() {
      return finalized;
    }

    @Override
    synchronized public ReplicaOutputStreams createStreams(boolean isCreate,
        DataChecksum requestedChecksum)
        throws IOException {
      if (finalized) {
        throw new IOException("Trying to write to a finalized replica "
            + theBlock);
      } else {
        SimulatedOutputStream crcStream = new SimulatedOutputStream();
        return new ReplicaOutputStreams(oStream, crcStream, requestedChecksum,
            getStorage(theBlock).getVolume(), fileIoProvider);
      }
    }

    @Override
    public OutputStream createRestartMetaStream() throws IOException {
      return new SimulatedOutputStream();
    }

    @Override
    synchronized public long getBlockId() {
      return theBlock.getBlockId();
    }

    @Override
    synchronized public long getVisibleLength() {
      return getBytesAcked();
    }

    @Override
    public ReplicaState getState() {
      return finalized ? ReplicaState.FINALIZED : ReplicaState.RBW;
    }

    @Override
    synchronized public long getBytesAcked() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return bytesAcked;
      }
    }

    @Override
    synchronized public void setBytesAcked(long bytesAcked) {
      if (!finalized) {
        this.bytesAcked = bytesAcked;
      }
    }

    @Override
    public void releaseAllBytesReserved() {
    }

    @Override
    synchronized public long getBytesOnDisk() {
      if (finalized) {
        return theBlock.getNumBytes();
      } else {
        return oStream.getLength();
      }
    }

    @Override
    public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
      oStream.setLength(dataLength);
    }

    @Override
    public ChunkChecksum getLastChecksumAndDataLen() {
      return new ChunkChecksum(oStream.getLength(), null);
    }

    @Override
    public boolean isOnTransientStorage() {
      return false;
    }

    @Override
    public ReplicaInfo getReplicaInfo() {
      return null;
    }

    @Override
    public void setWriter(Thread writer) {
    }

    @Override
    public void interruptThread() {
    }

    @Override
    public boolean attemptToSetWriter(Thread prevWriter, Thread newWriter) {
      return false;
    }

    @Override
    public void stopWriter(long xceiverStopTimeout) throws IOException {
    }

    @Override
    public void waitForMinLength(long minLength, long time, TimeUnit unit)
        throws IOException {
      final long deadLine = System.currentTimeMillis() + unit.toMillis(time);
      do {
        if (getBytesOnDisk() >= minLength) {
          return;
        }
        try {
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      } while (deadLine > System.currentTimeMillis());
      throw new IOException("Minimum length was not achieved within timeout");
    }
  }

  /**
   * Class is used for tracking block pool storage utilization similar
   * to {@link BlockPoolSlice}
   */
  private static class SimulatedBPStorage {
    // in bytes
    private long used;
    private final Map<Block, BInfo> blockMap = new TreeMap<>();

    long getUsed() {
      return used;
    }

    void alloc(long amount) {
      used += amount;
    }

    void free(long amount) {
      used -= amount;
    }

    Map<Block, BInfo> getBlockMap() {
      return blockMap;
    }

    SimulatedBPStorage() {
      used = 0L;
    }
  }

  /**
   * Class used for tracking datanode level storage utilization similar
   * to {@link FSVolumeSet}
   */
  static class SimulatedStorage {
    private final Map<String, SimulatedBPStorage> map =
        new ConcurrentHashMap<>();

    private final long capacity;  // in bytes
    private final DatanodeStorage dnStorage;
    private final SimulatedVolume volume;

    synchronized long getFree() {
      return capacity - getUsed();
    }

    long getCapacity() {
      return capacity;
    }

    synchronized long getUsed() {
      long used = 0L;
      for (SimulatedBPStorage bpStorage : map.values()) {
        used += bpStorage.getUsed();
      }
      return used;
    }

    synchronized long getBlockPoolUsed(String bpid) throws IOException {
      return getBPStorage(bpid).getUsed();
    }

    int getNumFailedVolumes() {
      return 0;
    }

    synchronized boolean alloc(String bpid, long amount) throws IOException {
      if (getFree() >= amount) {
        getBPStorage(bpid).alloc(amount);
        return true;
      }
      return false;
    }

    synchronized void free(String bpid, long amount) throws IOException {
      getBPStorage(bpid).free(amount);
    }

    SimulatedStorage(long cap, DatanodeStorage.State state,
        FileIoProvider fileIoProvider, Configuration conf) {
      capacity = cap;
      dnStorage = new DatanodeStorage(
          "SimulatedStorage-" + DatanodeStorage.generateUuid(),
          state, StorageType.DEFAULT);
      DataNodeVolumeMetrics volumeMetrics =
          DataNodeVolumeMetrics.create(conf, dnStorage.getStorageID());
      this.volume = new SimulatedVolume(this, fileIoProvider, volumeMetrics);
    }

    synchronized void addBlockPool(String bpid) {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage != null) {
        return;
      }
      map.put(bpid, new SimulatedBPStorage());
    }

    synchronized void removeBlockPool(String bpid) {
      map.remove(bpid);
    }

    private SimulatedBPStorage getBPStorage(String bpid) throws IOException {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage == null) {
        throw new IOException("block pool " + bpid + " not found");
      }
      return bpStorage;
    }

    String getStorageUuid() {
      return dnStorage.getStorageID();
    }

    DatanodeStorage getDnStorage() {
      return dnStorage;
    }

    synchronized StorageReport getStorageReport(String bpid) {
      return new StorageReport(dnStorage,
          false, getCapacity(), getUsed(), getFree(),
          map.get(bpid).getUsed(), 0L);
    }

    SimulatedVolume getVolume() {
      return volume;
    }

    Map<Block, BInfo> getBlockMap(String bpid) throws IOException {
      SimulatedBPStorage bpStorage = map.get(bpid);
      if (bpStorage == null) {
        throw new IOException("Nonexistent block pool: " + bpid);
      }
      return bpStorage.getBlockMap();
    }
  }

  static class SimulatedVolume implements FsVolumeSpi {
    private final SimulatedStorage storage;
    private final FileIoProvider fileIoProvider;
    private final DataNodeVolumeMetrics metrics;

    SimulatedVolume(final SimulatedStorage storage,
                    final FileIoProvider fileIoProvider,
                    final DataNodeVolumeMetrics metrics) {
      this.storage = storage;
      this.fileIoProvider = fileIoProvider;
      this.metrics = metrics;
    }

    @Override
    public FsVolumeReference obtainReference() throws ClosedChannelException {
      return new FsVolumeReference() {
        @Override
        public void close() throws IOException {
          // no-op.
        }

        @Override
        public FsVolumeSpi getVolume() {
          return SimulatedVolume.this;
        }
      };
    }

    @Override
    public String getStorageID() {
      return storage.getStorageUuid();
    }

    @Override
    public String[] getBlockPoolList() {
      return new String[0];
    }

    @Override
    public long getAvailable() throws IOException {
      return storage.getCapacity() - storage.getUsed();
    }

    @Override
    public StorageType getStorageType() {
      return StorageType.DISK;
    }

    @Override
    public boolean isTransientStorage() {
      return false;
    }

    @Override
    public void reserveSpaceForReplica(long bytesToReserve) {
    }

    @Override
    public void releaseLockedMemory(long bytesToRelease) {
    }

    @Override
    public void releaseReservedSpace(long bytesToRelease) {
    }

    @Override
    public BlockIterator newBlockIterator(String bpid, String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockIterator loadBlockIterator(String bpid, String name)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FsDatasetSpi getDataset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageLocation getStorageLocation() {
      try {
        return StorageLocation.parse("[DISK]file:///simulated");
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public URI getBaseURI() {
      return null;
    }

    @Override
    public DF getUsageStats(Configuration conf) {
      return null;
    }

    @Override
    public byte[] loadLastPartialChunkChecksum(
        File blockFile, File metaFile) throws IOException {
      return null;
    }

    @Override
    public void compileReport(String bpid,
        Collection<ScanInfo> report, ReportCompiler reportCompiler)
        throws InterruptedException, IOException {
    }

    @Override
    public FileIoProvider getFileIoProvider() {
      return fileIoProvider;
    }

    @Override
    public DataNodeVolumeMetrics getMetrics() {
      return metrics;
    }

    @Override
    public VolumeCheckResult check(VolumeCheckContext context)
        throws Exception {
      return VolumeCheckResult.HEALTHY;
    }
  }

  private final List<SimulatedStorage> storages;
  private final String datanodeUuid;
  private final DataNode datanode;

  public List<SimulatedStorage> getStorages() {
    return storages;
  }

  public SimulatedFSDataset(DataStorage storage, Configuration conf) {
    this(null, storage, conf);
  }

  public SimulatedFSDataset(DataNode datanode, DataStorage storage, Configuration conf) {
    this.datanode = datanode;
    int storageCount;
    if (storage != null && storage.getNumStorageDirs() > 0) {
      storageCount = storage.getNumStorageDirs();
      for (int i = 0; i < storage.getNumStorageDirs(); ++i) {
        DataStorage.createStorageID(storage.getStorageDir(i), false, conf);
      }
      this.datanodeUuid = storage.getDatanodeUuid();
    } else {
      storageCount = DataNode.getStorageLocations(conf).size();
      this.datanodeUuid = "SimulatedDatanode-" + DataNode.generateUuid();
    }

    registerMBean(datanodeUuid);
    this.fileIoProvider = new FileIoProvider(conf, datanode);

    this.datasetLock = new AutoCloseableLock();

    this.storages = new ArrayList<>();
    for (int i = 0; i < storageCount; i++) {
      this.storages.add(new SimulatedStorage(
          conf.getLong(CONFIG_PROPERTY_CAPACITY, DEFAULT_CAPACITY),
          conf.getEnum(CONFIG_PROPERTY_STATE, DEFAULT_STATE),
          fileIoProvider, conf));
    }
  }

  public synchronized void injectBlocks(String bpid,
      Iterable<? extends Block> injectBlocks) throws IOException {
    ExtendedBlock blk = new ExtendedBlock();
    if (injectBlocks != null) {
      for (Block b: injectBlocks) { // if any blocks in list is bad, reject list
        if (b == null) {
          throw new NullPointerException("Null blocks in block list");
        }
        blk.set(bpid, b);
        if (isValidBlock(blk)) {
          throw new IOException("Block already exists in  block list");
        }
      }

      for (SimulatedStorage storage : storages) {
        storage.addBlockPool(bpid);
      }

      for (Block b: injectBlocks) {
        BInfo binfo = new BInfo(bpid, b, false);
        getBlockMap(b, bpid).put(binfo.theBlock, binfo);
      }
    }
  }

  /** Get the storage that a given block lives within. */
  private SimulatedStorage getStorage(Block b) {
    return storages.get(LongMath.mod(b.getBlockId(), storages.size()));
  }

  /**
   * Get the block map that a given block lives within, assuming it is within
   * block pool bpid.
   * @param b The block to look for
   * @param bpid The block pool that contains b
   * @return The block map (non-null)
   * @throws IOException if bpid does not exist
   */
  private Map<Block, BInfo> getBlockMap(Block b, String bpid)
      throws IOException {
    return getStorage(b).getBlockMap(bpid);
  }

  /**
   * Get the block map that a given block lives within.
   * @param b The extended block to look for
   * @return The block map (non-null)
   * @throws IOException if b is in a nonexistent block pool
   */
  private Map<Block, BInfo> getBlockMap(ExtendedBlock b) throws IOException {
    return getBlockMap(b.getLocalBlock(), b.getBlockPoolId());
  }

  @Override // FsDatasetSpi
  public synchronized void finalizeBlock(ExtendedBlock b, boolean fsyncDir)
      throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    binfo.finalizeBlock(b.getBlockPoolId(), b.getNumBytes());
  }

  @Override // FsDatasetSpi
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException{
    if (isValidRbw(b)) {
      getBlockMap(b).remove(b.getLocalBlock());
    }
  }

  synchronized BlockListAsLongs getBlockReport(String bpid,
      SimulatedStorage storage) {
    BlockListAsLongs.Builder report = BlockListAsLongs.builder();
    try {
      for (BInfo b : storage.getBlockMap(bpid).values()) {
        if (b.isFinalized()) {
          report.add(b);
        }
      }
    } catch (IOException ioe) {
      DataNode.LOG.error("Exception while getting block reports", ioe);
    }
    return report.build();
  }

  @Override
  public synchronized Map<DatanodeStorage, BlockListAsLongs> getBlockReports(
      String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReports = new HashMap<>();
    for (SimulatedStorage storage : storages) {
      blockReports.put(storage.getDnStorage(), getBlockReport(bpid, storage));
    }
    return blockReports;
  }

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    return Collections.emptyList();
  }

  @Override // FSDatasetMBean
  public long getCapacity() {
    long total = 0L;
    for (SimulatedStorage storage : storages) {
      total += storage.getCapacity();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getDfsUsed() {
    long total = 0L;
    for (SimulatedStorage storage : storages) {
      total += storage.getUsed();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    long total = 0L;
    for (SimulatedStorage storage : storages) {
      total += storage.getBlockPoolUsed(bpid);
    }
    return total;
  }

  @Override // FSDatasetMBean
  public long getRemaining() {
    long total = 0L;
    for (SimulatedStorage storage : storages) {
      total += storage.getFree();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {
    int total = 0;
    for (SimulatedStorage storage : storages) {
      total += storage.getNumFailedVolumes();
    }
    return total;
  }

  @Override // FSDatasetMBean
  public String[] getFailedStorageLocations() {
    return null;
  }

  @Override // FSDatasetMBean
  public long getLastVolumeFailureDate() {
    return 0L;
  }

  @Override // FSDatasetMBean
  public long getEstimatedCapacityLostTotal() {
    return 0L;
  }

  @Override // FsDatasetSpi
  public VolumeFailureSummary getVolumeFailureSummary() {
    return new VolumeFailureSummary(ArrayUtils.EMPTY_STRING_ARRAY, 0, 0);
  }

  @Override // FSDatasetMBean
  public long getCacheUsed() {
    return 0L;
  }

  @Override // FSDatasetMBean
  public long getCacheCapacity() {
    return 0L;
  }

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return 0L;
  }

  @Override
  public long getNumBlocksFailedToCache() {
    return 0L;
  }

  @Override
  public long getNumBlocksFailedToUncache() {
    return 0L;
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
      DataNodeMetricHelper.getMetrics(collector, this, "SimulatedFSDataset");
    } catch (Exception e){
        //ignore Exceptions
    }
  }

  @Override // FsDatasetSpi
  public synchronized long getLength(ExtendedBlock b) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("Finalizing a non existing block " + b);
    }
    return binfo.getNumBytes();
  }

  @Override
  @Deprecated
  public Replica getReplica(String bpid, long blockId) {
    Block b = new Block(blockId);
    try {
      return getBlockMap(b, bpid).get(b);
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override
  public synchronized String getReplicaString(String bpid, long blockId) {
    Replica r = null;
    try {
      Block b = new Block(blockId);
      r = getBlockMap(b, bpid).get(b);
    } catch (IOException ioe) {
      // Ignore
    }
    return Objects.toString(r);
  }

  @Override // FsDatasetSpi
  public Block getStoredBlock(String bpid, long blkid) throws IOException {
    Block b = new Block(blkid);
    try {
      BInfo binfo = getBlockMap(b, bpid).get(b);
      if (binfo == null) {
        return null;
      }
      return new Block(blkid, binfo.getGenerationStamp(), binfo.getNumBytes());
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override // FsDatasetSpi
  public synchronized void invalidate(String bpid, Block[] invalidBlks)
      throws IOException {
    boolean error = false;
    if (invalidBlks == null) {
      return;
    }
    for (Block b: invalidBlks) {
      if (b == null) {
        continue;
      }
      Map<Block, BInfo> map = getBlockMap(b, bpid);
      BInfo binfo = map.get(b);
      if (binfo == null) {
        error = true;
        DataNode.LOG.warn("Invalidate: Missing block");
        continue;
      }
      getStorage(b).free(bpid, binfo.getNumBytes());
      map.remove(b);
      if (datanode != null) {
        datanode.notifyNamenodeDeletedBlock(new ExtendedBlock(bpid, b),
            binfo.getStorageUuid());
      }
    }
    if (error) {
      throw new IOException("Invalidate: Missing blocks.");
    }
  }

  @Override // FSDatasetSpi
  public void cache(String bpid, long[] cacheBlks) {
    throw new UnsupportedOperationException(
        "SimulatedFSDataset does not support cache operation!");
  }

  @Override // FSDatasetSpi
  public void uncache(String bpid, long[] uncacheBlks) {
    throw new UnsupportedOperationException(
        "SimulatedFSDataset does not support uncache operation!");
  }

  @Override // FSDatasetSpi
  public boolean isCached(String bpid, long blockId) {
    return false;
  }

  private BInfo getBInfo(final ExtendedBlock b) {
    try {
      return getBlockMap(b).get(b.getLocalBlock());
    } catch (IOException ioe) {
      return null;
    }
  }

  @Override // {@link FsDatasetSpi}
  public boolean contains(ExtendedBlock block) {
    return getBInfo(block) != null;
  }

  /**
   * Check if a block is valid.
   *
   * @param b The block to check.
   * @param minLength The minimum length that the block must have. May be 0.
   * @param state If this is null, it is ignored. If it is non-null, we will
   *          check that the replica has this state.
   *
   * @throws ReplicaNotFoundException If the replica is not found
   *
   * @throws UnexpectedReplicaStateException If the replica is not in the
   *           expected state.
   */
  @Override // {@link FsDatasetSpi}
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException {
    final BInfo binfo = getBInfo(b);

    if (binfo == null) {
      throw new ReplicaNotFoundException(b);
    }
    if ((state == ReplicaState.FINALIZED && !binfo.isFinalized()) ||
        (state != ReplicaState.FINALIZED && binfo.isFinalized())) {
      throw new UnexpectedReplicaStateException(b,state);
    }
  }

  @Override // FsDatasetSpi
  public synchronized boolean isValidBlock(ExtendedBlock b) {
    try {
      checkBlock(b, 0, ReplicaState.FINALIZED);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /* check if a block is created but not finalized */
  @Override
  public synchronized boolean isValidRbw(ExtendedBlock b) {
    try {
      checkBlock(b, 0, ReplicaState.RBW);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getStorageInfo();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler append(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null || !binfo.isFinalized()) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    binfo.unfinalizeBlock();
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      binfo.unfinalizeBlock();
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public Replica recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
      throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if (binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " is not valid, and cannot be appended to.");
    }
    if (!binfo.isFinalized()) {
      binfo.finalizeBlock(b.getBlockPoolId(), binfo.getNumBytes());
    }
    map.remove(b.getLocalBlock());
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return binfo;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler recoverRbw(
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    final Map<Block, BInfo> map = getBlockMap(b);
    BInfo binfo = map.get(b.getLocalBlock());
    if ( binfo == null) {
      throw new ReplicaNotFoundException("Block " + b
          + " does not exist, and cannot be appended to.");
    }
    if (binfo.isFinalized()) {
      throw new ReplicaAlreadyExistsException("Block " + b
          + " is valid, and cannot be written to.");
    }
    map.remove(b);
    binfo.theBlock.setGenerationStamp(newGS);
    map.put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createRbw(
      StorageType storageType, String storageId, ExtendedBlock b,
      boolean allowLazyPersist) throws IOException {
    return createTemporary(storageType, storageId, b, false);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createTemporary(StorageType storageType,
      String storageId, ExtendedBlock b, boolean isTransfer)
      throws IOException {
    if (isValidBlock(b)) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " is valid, and cannot be written to.");
    }
    if (isValidRbw(b)) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " is being written, and cannot be written to.");
    }
    BInfo binfo = new BInfo(b.getBlockPoolId(), b.getLocalBlock(), true);
    getBlockMap(b).put(binfo.theBlock, binfo);
    return new ReplicaHandler(binfo, null);
  }

  public synchronized InputStream getBlockInputStream(ExtendedBlock b)
      throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b);
    }
    return binfo.getIStream();
  }

  @Override // FsDatasetSpi
  public synchronized InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    InputStream result = getBlockInputStream(b);
    IOUtils.skipFully(result, seekOffset);
    return result;
  }

  /** Not supported */
  @Override // FsDatasetSpi
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, long blkoff,
      long ckoff) throws IOException {
    throw new IOException("Not supported");
  }

  @Override // FsDatasetSpi
  public synchronized LengthInputStream getMetaDataInputStream(ExtendedBlock b
      ) throws IOException {
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b);
    }
    if (!binfo.finalized) {
      throw new IOException("Block " + b +
          " is being written, its meta cannot be read");
    }
    final SimulatedInputStream sin = binfo.getMetaIStream();
    return new LengthInputStream(sin, sin.getLength());
  }

  @Override
  public void handleVolumeFailures(Set<FsVolumeSpi> failedVolumes) {
  }

  @Override // FsDatasetSpi
  public synchronized void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams stream, int checksumSize) throws IOException {
  }

  /**
   * Simulated input and output streams.
   */
  static private class SimulatedInputStream extends java.io.InputStream {
    final long length; // bytes
    int currentPos = 0;
    byte[] data = null;
    Block theBlock = null;

    /**
     * An input stream of size l with repeated bytes.
     * @param l size of the stream
     * @param iRepeatedData byte that is repeated in the stream
     */
    SimulatedInputStream(long l, Block b) {
      length = l;
      theBlock = b;
    }

    /**
     * An input stream of of the supplied data
     * @param iData data to construct the stream
     */
    SimulatedInputStream(byte[] iData) {
      data = iData;
      length = data.length;
    }

    /**
     * @return the lenght of the input stream
     */
    long getLength() {
      return length;
    }

    @Override
    public int read() throws IOException {
      if (currentPos >= length) {
        return -1;
      }
      if (data !=null) {
        return data[currentPos++];
      } else {
        return simulatedByte(theBlock, currentPos++) & BYTE_MASK;
      }
    }

    @Override
    public int read(byte[] b) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      }
      if (b.length == 0) {
        return 0;
      }
      if (currentPos >= length) { // EOF
        return -1;
      }
      int bytesRead = (int) Math.min(b.length, length-currentPos);
      if (data != null) {
        System.arraycopy(data, currentPos, b, 0, bytesRead);
      } else { // all data is zero
        for (int i = 0; i < bytesRead; i++) {
          b[i] = simulatedByte(theBlock, currentPos + i);
        }
      }
      currentPos += bytesRead;
      return bytesRead;
    }
  }

  /**
   * This class implements an output stream that merely throws its data away, but records its
   * length.
   */
  static private class SimulatedOutputStream extends OutputStream {
    long length = 0;

    /**
     * constructor for Simulated Output Steram
     */
    SimulatedOutputStream() {
    }

    /**
     *
     * @return the length of the data created so far.
     */
    long getLength() {
      return length;
    }

    /**
     */
    void setLength(long length) {
      this.length = length;
    }

    @Override
    public void write(int arg0) throws IOException {
      length++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      length += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      length += len;
    }
  }

  private ObjectName mbeanName;

  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<storageid>"
   *  We use storage id for MBean name since a minicluster within a single
   * Java VM may have multiple Simulated Datanodes.
   */
  void registerMBean(final String storageId) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in
    // package naming for mbeans and their impl.
    StandardMBean bean;

    try {
      bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-"+
                                  storageId, bean);
    } catch (NotCompliantMBeanException e) {
      DataNode.LOG.warn("Error registering FSDatasetState MBean", e);
    }

    DataNode.LOG.info("Registered FSDatasetState MBean");
  }

  @Override
  public void shutdown() {
    if (mbeanName != null) MBeans.unregister(mbeanName);
  }

  @Override
  public String getStorageInfo() {
    return "Simulated FSDataset-" + datanodeUuid;
  }

  @Override
  public boolean hasEnoughResource() {
    return true;
  }

  @Override
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException {
    ExtendedBlock b = rBlock.getBlock();
    BInfo binfo = getBlockMap(b).get(b.getLocalBlock());
    if (binfo == null) {
      throw new IOException("No such Block " + b);
    }

    return new ReplicaRecoveryInfo(binfo.getBlockId(), binfo.getBytesOnDisk(),
        binfo.getGenerationStamp(),
        binfo.isFinalized() ? ReplicaState.FINALIZED : ReplicaState.RBW);
  }

  @Override // FsDatasetSpi
  public Replica updateReplicaUnderRecovery(ExtendedBlock oldBlock,
                                        long recoveryId,
                                        long newBlockId,
                                        long newlength) throws IOException {
    return getBInfo(oldBlock);
  }

  @Override // FsDatasetSpi
  public long getReplicaVisibleLength(ExtendedBlock block) {
    return block.getNumBytes();
  }

  @Override // FsDatasetSpi
  public void addBlockPool(String bpid, Configuration conf) {
    for (SimulatedStorage storage : storages) {
      storage.addBlockPool(bpid);
    }
  }

  @Override // FsDatasetSpi
  public void shutdownBlockPool(String bpid) {
    for (SimulatedStorage storage : storages) {
      storage.removeBlockPool(bpid);
    }
  }

  @Override // FsDatasetSpi
  public void deleteBlockPool(String bpid, boolean force) {
     return;
  }

  @Override
  public ReplicaInPipeline convertTemporaryToRbw(ExtendedBlock temporary)
      throws IOException {
    final BInfo r = getBlockMap(temporary).get(temporary.getLocalBlock());
    if (r == null) {
      throw new IOException("Block not found, temporary=" + temporary);
    } else if (r.isFinalized()) {
      throw new IOException("Replica already finalized, temporary="
          + temporary + ", r=" + r);
    }
    return r;
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock b) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableTrash(String bpid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearTrash(String bpid) {
  }

  @Override
  public boolean trashEnabled(String bpid) {
    return false;
  }

  @Override
  public void setRollingUpgradeMarker(String bpid) {
  }

  @Override
  public void clearRollingUpgradeMarker(String bpid) {
  }

  @Override
  public void checkAndUpdate(String bpid, ScanInfo info) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FsVolumeReferences getFsVolumeReferences() {
    List<SimulatedVolume> volumes = new ArrayList<>();
    for (SimulatedStorage storage : storages) {
      volumes.add(storage.getVolume());
    }
    return new FsVolumeReferences(volumes);
  }

  @Override
  public void addVolume(
      final StorageLocation location,
      final List<NamespaceInfo> nsInfos) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    for (SimulatedStorage storage : storages) {
      if (storageUuid.equals(storage.getStorageUuid())) {
        return storage.getDnStorage();
      }
    }
    return null;
  }

  @Override
  public StorageReport[] getStorageReports(String bpid) {
    List<StorageReport> reports = new ArrayList<>();
    for (SimulatedStorage storage : storages) {
      reports.add(storage.getStorageReport(bpid));
    }
    return reports.toArray(new StorageReport[0]);
  }

  @Override
  public List<ReplicaInfo> getSortedFinalizedBlocks(String bpid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public FsVolumeSpi getVolume(ExtendedBlock b) {
    return getStorage(b.getLocalBlock()).getVolume();
  }

  @Override
  public synchronized void removeVolumes(Collection<StorageLocation> volumes,
      boolean clearFailure) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
      ReplicaOutputStreams outs, long offset, long nbytes, int flags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, FsVolumeSpi targetVolume) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onFailLazyPersist(String bpId, long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
      StorageType targetStorageType, String storageId) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setPinning(ExtendedBlock b) throws IOException {
    getBlockMap(b).get(b.getLocalBlock()).pinned = true;
  }

  @Override
  public boolean getPinning(ExtendedBlock b) throws IOException {
    return getBlockMap(b).get(b.getLocalBlock()).pinned;
  }

  @Override
  public boolean isDeletingBlock(String bpid, long blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReplicaInfo moveBlockAcrossVolumes(ExtendedBlock block,
      FsVolumeSpi destination) throws IOException {
    return null;
  }

  @Override
  public AutoCloseableLock acquireDatasetLock() {
    return datasetLock.acquire();
  }

  @Override
  public AutoCloseableLock acquireDatasetReadLock() {
    // No RW lock implementation in simulated dataset currently.
    return datasetLock.acquire();
  }

  @Override
  public Set<? extends Replica> deepCopyReplica(String bpid)
      throws IOException {
    Set<BInfo> replicas = new HashSet<>();
    for (SimulatedStorage s : storages) {
      replicas.addAll(s.getBlockMap(bpid).values());
    }
    return Collections.unmodifiableSet(replicas);
  }
}

