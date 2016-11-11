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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.InstrumentedLock;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
@InterfaceAudience.Private
class FsDatasetImpl implements FsDatasetSpi<FsVolumeImpl> {
  static final Log LOG = LogFactory.getLog(FsDatasetImpl.class);
  private final static boolean isNativeIOAvailable;
  private Timer timer;
  static {
    isNativeIOAvailable = NativeIO.isAvailable();
    if (Path.WINDOWS && !isNativeIOAvailable) {
      LOG.warn("Data node cannot fully support concurrent reading"
          + " and writing without native code extensions on Windows.");
    }
  }

  @Override // FsDatasetSpi
  public FsVolumeReferences getFsVolumeReferences() {
    return new FsVolumeReferences(volumes.getVolumes());
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override // FsDatasetSpi
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    List<StorageReport> reports;
    synchronized (statsLock) {
      List<FsVolumeImpl> curVolumes = volumes.getVolumes();
      reports = new ArrayList<>(curVolumes.size());
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
          StorageReport sr = new StorageReport(volume.toDatanodeStorage(),
              false,
              volume.getCapacity(),
              volume.getDfsUsed(),
              volume.getAvailable(),
              volume.getBlockPoolUsed(bpid),
              volume.getNonDfsUsed());
          reports.add(sr);
        } catch (ClosedChannelException e) {
          continue;
        }
      }
    }

    return reports.toArray(new StorageReport[reports.size()]);
  }

  @Override
  public FsVolumeImpl getVolume(final ExtendedBlock b) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final ReplicaInfo r =
          volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
      return r != null ? (FsVolumeImpl) r.getVolume() : null;
    }
  }

  @Override // FsDatasetSpi
  public Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo r = volumeMap.get(bpid, blkid);
      if (r == null) {
        return null;
      }
      return new Block(blkid, r.getBytesOnDisk(), r.getGenerationStamp());
    }
  }


  /**
   * This should be primarily used for testing.
   * @return clone of replica store in datanode memory
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
    if (r == null) {
      return null;
    }
    switch(r.getState()) {
    case FINALIZED:
    case RBW:
    case RWR:
    case RUR:
    case TEMPORARY:
      return new ReplicaBuilder(r.getState()).from(r).build();
    }
    return null;
  }
  
  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    ReplicaInfo info = getBlockReplica(b);
    if (info == null || !info.metadataExists()) {
      return null;
    }
    return info.getMetadataInputStream(0);
  }
    
  final DataNode datanode;
  final DataStorage dataStorage;
  private final FsVolumeList volumes;
  final Map<String, DatanodeStorage> storageMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  final Daemon lazyWriter;
  final FsDatasetCache cacheManager;
  private final Configuration conf;
  private final int volFailuresTolerated;
  private volatile boolean fsRunning;

  final ReplicaMap volumeMap;
  final Map<String, Set<Long>> deletingBlock;
  final RamDiskReplicaTracker ramDiskReplicaTracker;
  final RamDiskAsyncLazyPersistService asyncLazyPersistService;

  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;

  private final int smallBufferSize;

  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

  final LocalFileSystem localFS;

  private boolean blockPinningEnabled;
  private final int maxDataLength;

  @VisibleForTesting
  final AutoCloseableLock datasetLock;
  private final Condition datasetLockCondition;
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {
    this.fsRunning = true;
    this.datanode = datanode;
    this.dataStorage = storage;
    this.conf = conf;
    this.smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
    this.datasetLock = new AutoCloseableLock(
        new InstrumentedLock(getClass().getName(), LOG,
          conf.getTimeDuration(
            DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
            DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
          300));
    this.datasetLockCondition = datasetLock.newCondition();

    // The number of volumes required for operation is the total number
    // of volumes minus the number of failed volumes we can tolerate.
    volFailuresTolerated = datanode.getDnConf().getVolFailuresTolerated();

    Collection<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);
    List<VolumeFailureInfo> volumeFailureInfos = getInitialVolumeFailureInfos(
        dataLocations, storage);

    int volsConfigured = datanode.getDnConf().getVolsConfigured();
    int volsFailed = volumeFailureInfos.size();

    if (volsFailed > volFailuresTolerated) {
      throw new DiskErrorException("Too many failed volumes - "
          + "current valid volumes: " + storage.getNumStorageDirs() 
          + ", volumes configured: " + volsConfigured 
          + ", volumes failed: " + volsFailed
          + ", volume failures tolerated: " + volFailuresTolerated);
    }

    storageMap = new ConcurrentHashMap<String, DatanodeStorage>();
    volumeMap = new ReplicaMap(datasetLock);
    ramDiskReplicaTracker = RamDiskReplicaTracker.getInstance(conf, this);

    @SuppressWarnings("unchecked")
    final VolumeChoosingPolicy<FsVolumeImpl> blockChooserImpl =
        ReflectionUtils.newInstance(conf.getClass(
            DFSConfigKeys.DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY,
            RoundRobinVolumeChoosingPolicy.class,
            VolumeChoosingPolicy.class), conf);
    volumes = new FsVolumeList(volumeFailureInfos, datanode.getBlockScanner(),
        blockChooserImpl);
    asyncDiskService = new FsDatasetAsyncDiskService(datanode, this);
    asyncLazyPersistService = new RamDiskAsyncLazyPersistService(datanode, conf);
    deletingBlock = new HashMap<String, Set<Long>>();

    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      addVolume(dataLocations, storage.getStorageDir(idx));
    }
    setupAsyncLazyPersistThreads();

    cacheManager = new FsDatasetCache(this);

    // Start the lazy writer once we have built the replica maps.
    // We need to start the lazy writer even if MaxLockedMemory is set to
    // zero because we may have un-persisted replicas in memory from before
    // the process restart. To minimize the chances of data loss we'll
    // ensure they get written to disk now.
    if (ramDiskReplicaTracker.numReplicasNotPersisted() > 0 ||
        datanode.getDnConf().getMaxLockedMemory() > 0) {
      lazyWriter = new Daemon(new LazyWriter(conf));
      lazyWriter.start();
    } else {
      lazyWriter = null;
    }

    registerMBean(datanode.getDatanodeUuid());

    // Add a Metrics2 Source Interface. This is same
    // data as MXBean. We can remove the registerMbean call
    // in a release where we can break backward compatibility
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register("FSDatasetState", "FSDatasetState", this);

    localFS = FileSystem.getLocal(conf);
    blockPinningEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED,
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT);
    maxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
  }

  @Override
  public AutoCloseableLock acquireDatasetLock() {
    return datasetLock.acquire();
  }

  /**
   * Gets initial volume failure information for all volumes that failed
   * immediately at startup.  The method works by determining the set difference
   * between all configured storage locations and the actual storage locations in
   * use after attempting to put all of them into service.
   *
   * @return each storage location that has failed
   */
  private static List<VolumeFailureInfo> getInitialVolumeFailureInfos(
      Collection<StorageLocation> dataLocations, DataStorage storage) {
    Set<StorageLocation> failedLocationSet = Sets.newHashSetWithExpectedSize(
        dataLocations.size());
    for (StorageLocation sl: dataLocations) {
      LOG.info("Adding to failedLocationSet " + sl);
      failedLocationSet.add(sl);
    }
    for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
         it.hasNext(); ) {
      Storage.StorageDirectory sd = it.next();
      failedLocationSet.remove(sd.getStorageLocation());
      LOG.info("Removing from failedLocationSet " + sd.getStorageLocation());
    }
    List<VolumeFailureInfo> volumeFailureInfos = Lists.newArrayListWithCapacity(
        failedLocationSet.size());
    long failureDate = Time.now();
    for (StorageLocation failedStorageLocation: failedLocationSet) {
      volumeFailureInfos.add(new VolumeFailureInfo(failedStorageLocation,
          failureDate));
    }
    return volumeFailureInfos;
  }

  /**
   * Activate a volume to serve requests.
   * @throws IOException if the storage UUID already exists.
   */
  private void activateVolume(
      ReplicaMap replicaMap,
      Storage.StorageDirectory sd, StorageType storageType,
      FsVolumeReference ref) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      DatanodeStorage dnStorage = storageMap.get(sd.getStorageUuid());
      if (dnStorage != null) {
        final String errorMsg = String.format(
            "Found duplicated storage UUID: %s in %s.",
            sd.getStorageUuid(), sd.getVersionFile());
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
      volumeMap.addAll(replicaMap);
      storageMap.put(sd.getStorageUuid(),
          new DatanodeStorage(sd.getStorageUuid(),
              DatanodeStorage.State.NORMAL,
              storageType));
      asyncDiskService.addVolume((FsVolumeImpl) ref.getVolume());
      volumes.addVolume(ref);
    }
  }

  private void addVolume(Collection<StorageLocation> dataLocations,
      Storage.StorageDirectory sd) throws IOException {
    final StorageLocation storageLocation = sd.getStorageLocation();

    // If IOException raises from FsVolumeImpl() or getVolumeMap(), there is
    // nothing needed to be rolled back to make various data structures, e.g.,
    // storageMap and asyncDiskService, consistent.
    FsVolumeImpl fsVolume = new FsVolumeImplBuilder()
                              .setDataset(this)
                              .setStorageID(sd.getStorageUuid())
                              .setStorageDirectory(sd)
                              .setConf(this.conf)
                              .build();
    FsVolumeReference ref = fsVolume.obtainReference();
    ReplicaMap tempVolumeMap = new ReplicaMap(datasetLock);
    fsVolume.getVolumeMap(tempVolumeMap, ramDiskReplicaTracker);

    activateVolume(tempVolumeMap, sd, storageLocation.getStorageType(), ref);
    LOG.info("Added volume - " + storageLocation + ", StorageType: " +
        storageLocation.getStorageType());
  }

  @VisibleForTesting
  public FsVolumeImpl createFsVolume(String storageUuid,
      Storage.StorageDirectory sd,
      final StorageLocation location) throws IOException {
    return new FsVolumeImplBuilder()
        .setDataset(this)
        .setStorageID(storageUuid)
        .setStorageDirectory(sd)
        .setConf(conf)
        .build();
  }

  @Override
  public void addVolume(final StorageLocation location,
      final List<NamespaceInfo> nsInfos)
      throws IOException {
    // Prepare volume in DataStorage
    final DataStorage.VolumeBuilder builder;
    try {
      builder = dataStorage.prepareVolume(datanode, location, nsInfos);
    } catch (IOException e) {
      volumes.addVolumeFailureInfo(new VolumeFailureInfo(location, Time.now()));
      throw e;
    }

    final Storage.StorageDirectory sd = builder.getStorageDirectory();

    StorageType storageType = location.getStorageType();
    final FsVolumeImpl fsVolume =
        createFsVolume(sd.getStorageUuid(), sd, location);
    final ReplicaMap tempVolumeMap = new ReplicaMap(new AutoCloseableLock());
    ArrayList<IOException> exceptions = Lists.newArrayList();

    for (final NamespaceInfo nsInfo : nsInfos) {
      String bpid = nsInfo.getBlockPoolID();
      try {
        fsVolume.addBlockPool(bpid, this.conf, this.timer);
        fsVolume.getVolumeMap(bpid, tempVolumeMap, ramDiskReplicaTracker);
      } catch (IOException e) {
        LOG.warn("Caught exception when adding " + fsVolume +
            ". Will throw later.", e);
        exceptions.add(e);
      }
    }
    if (!exceptions.isEmpty()) {
      try {
        sd.unlock();
      } catch (IOException e) {
        exceptions.add(e);
      }
      throw MultipleIOException.createIOException(exceptions);
    }

    final FsVolumeReference ref = fsVolume.obtainReference();
    setupAsyncLazyPersistThread(fsVolume);

    builder.build();
    activateVolume(tempVolumeMap, sd, storageType, ref);
    LOG.info("Added volume - " + location + ", StorageType: " + storageType);
  }

  /**
   * Removes a set of volumes from FsDataset.
   * @param storageLocationsToRemove a set of
   * {@link StorageLocation}s for each volume.
   * @param clearFailure set true to clear failure information.
   */
  @Override
  public void removeVolumes(
      Collection<StorageLocation> storageLocationsToRemove,
      boolean clearFailure) {
    Map<String, List<ReplicaInfo>> blkToInvalidate = new HashMap<>();
    List<String> storageToRemove = new ArrayList<>();
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for (int idx = 0; idx < dataStorage.getNumStorageDirs(); idx++) {
        Storage.StorageDirectory sd = dataStorage.getStorageDir(idx);
        final StorageLocation sdLocation = sd.getStorageLocation();
        LOG.info("Checking removing StorageLocation " +
            sdLocation + " with id " + sd.getStorageUuid());
        if (storageLocationsToRemove.contains(sdLocation)) {
          LOG.info("Removing StorageLocation " + sdLocation + " with id " +
              sd.getStorageUuid() + " from FsDataset.");
          // Disable the volume from the service.
          asyncDiskService.removeVolume(sd.getStorageUuid());
          volumes.removeVolume(sdLocation, clearFailure);
          volumes.waitVolumeRemoved(5000, datasetLockCondition);

          // Removed all replica information for the blocks on the volume.
          // Unlike updating the volumeMap in addVolume(), this operation does
          // not scan disks.
          for (String bpid : volumeMap.getBlockPoolList()) {
            List<ReplicaInfo> blocks = new ArrayList<>();
            for (Iterator<ReplicaInfo> it =
                  volumeMap.replicas(bpid).iterator(); it.hasNext();) {
              ReplicaInfo block = it.next();
              final StorageLocation blockStorageLocation =
                  block.getVolume().getStorageLocation();
              LOG.info("checking for block " + block.getBlockId() +
                  " with storageLocation " + blockStorageLocation);
              if (blockStorageLocation.equals(sdLocation)) {
                blocks.add(block);
                it.remove();
              }
            }
            blkToInvalidate.put(bpid, blocks);
          }

          storageToRemove.add(sd.getStorageUuid());
        }
      }
      setupAsyncLazyPersistThreads();
    }

    // Call this outside the lock.
    for (Map.Entry<String, List<ReplicaInfo>> entry :
        blkToInvalidate.entrySet()) {
      String bpid = entry.getKey();
      List<ReplicaInfo> blocks = entry.getValue();
      for (ReplicaInfo block : blocks) {
        invalidate(bpid, block);
      }
    }

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for(String storageUuid : storageToRemove) {
        storageMap.remove(storageUuid);
      }
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    synchronized(statsLock) {
      return volumes.getDfsUsed();
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    synchronized(statsLock) {
      return volumes.getBlockPoolUsed(bpid);
    }
  }
  
  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    return getNumFailedVolumes() <= volFailuresTolerated;
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() throws IOException {
    synchronized(statsLock) {
      return volumes.getCapacity();
    }
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    synchronized(statsLock) {
      return volumes.getRemaining();
    }
  }

  /**
   * Return the number of failed volumes in the FSDataset.
   */
  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {
    return volumes.getVolumeFailureInfos().length;
  }

  @Override // FSDatasetMBean
  public String[] getFailedStorageLocations() {
    VolumeFailureInfo[] infos = volumes.getVolumeFailureInfos();
    List<String> failedStorageLocations = Lists.newArrayListWithCapacity(
        infos.length);
    for (VolumeFailureInfo info: infos) {
      failedStorageLocations.add(
          info.getFailedStorageLocation().getNormalizedUri().toString());
    }
    return failedStorageLocations.toArray(
        new String[failedStorageLocations.size()]);
  }

  @Override // FSDatasetMBean
  public long getLastVolumeFailureDate() {
    long lastVolumeFailureDate = 0;
    for (VolumeFailureInfo info: volumes.getVolumeFailureInfos()) {
      long failureDate = info.getFailureDate();
      if (failureDate > lastVolumeFailureDate) {
        lastVolumeFailureDate = failureDate;
      }
    }
    return lastVolumeFailureDate;
  }

  @Override // FSDatasetMBean
  public long getEstimatedCapacityLostTotal() {
    long estimatedCapacityLostTotal = 0;
    for (VolumeFailureInfo info: volumes.getVolumeFailureInfos()) {
      estimatedCapacityLostTotal += info.getEstimatedCapacityLost();
    }
    return estimatedCapacityLostTotal;
  }

  @Override // FsDatasetSpi
  public VolumeFailureSummary getVolumeFailureSummary() {
    VolumeFailureInfo[] infos = volumes.getVolumeFailureInfos();
    if (infos.length == 0) {
      return null;
    }
    List<String> failedStorageLocations = Lists.newArrayListWithCapacity(
        infos.length);
    long lastVolumeFailureDate = 0;
    long estimatedCapacityLostTotal = 0;
    for (VolumeFailureInfo info: infos) {
      failedStorageLocations.add(
          info.getFailedStorageLocation().getNormalizedUri().toString());
      long failureDate = info.getFailureDate();
      if (failureDate > lastVolumeFailureDate) {
        lastVolumeFailureDate = failureDate;
      }
      estimatedCapacityLostTotal += info.getEstimatedCapacityLost();
    }
    return new VolumeFailureSummary(
        failedStorageLocations.toArray(new String[failedStorageLocations.size()]),
        lastVolumeFailureDate, estimatedCapacityLostTotal);
  }

  @Override // FSDatasetMBean
  public long getCacheUsed() {
    return cacheManager.getCacheUsed();
  }

  @Override // FSDatasetMBean
  public long getCacheCapacity() {
    return cacheManager.getCacheCapacity();
  }

  @Override // FSDatasetMBean
  public long getNumBlocksFailedToCache() {
    return cacheManager.getNumBlocksFailedToCache();
  }

  @Override // FSDatasetMBean
  public long getNumBlocksFailedToUncache() {
    return cacheManager.getNumBlocksFailedToUncache();
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
      DataNodeMetricHelper.getMetrics(collector, this, "FSDatasetState");
    } catch (Exception e) {
        LOG.warn("Exception thrown while metric collection. Exception : "
          + e.getMessage());
    }
  }

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return cacheManager.getNumBlocksCached();
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FsDatasetSpi
  public long getLength(ExtendedBlock b) throws IOException {
    return getBlockReplica(b).getBlockDataLength();
  }

  /**
   * Get File name for a given block.
   */
  private ReplicaInfo getBlockReplica(ExtendedBlock b) throws IOException {
    return getBlockReplica(b.getBlockPoolId(), b.getBlockId());
  }
  
  /**
   * Get File name for a given block.
   */
  ReplicaInfo getBlockReplica(String bpid, long blockId) throws IOException {
    ReplicaInfo r = validateBlockFile(bpid, blockId);
    if (r == null) {
      throw new FileNotFoundException("BlockId " + blockId + " is not valid.");
    }
    return r;
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {

    ReplicaInfo info;
    synchronized(this) {
      info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    }

    if (info != null && info.getVolume().isTransientStorage()) {
      ramDiskReplicaTracker.touch(b.getBlockPoolId(), b.getBlockId());
      datanode.getMetrics().incrRamDiskBlocksReadHits();
    }

    if(info != null && info.blockDataExists()) {
      return info.getDataInputStream(seekOffset);
    } else {
      throw new IOException("No data exists for block " + b);
    }
  }

  /**
   * Get the meta info of a block stored in volumeMap. To find a block,
   * block pool Id, block Id and generation stamp must match.
   * @param b extended block
   * @return the meta replica information
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  ReplicaInfo getReplicaInfo(ExtendedBlock b)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    if (info == null) {
      if (volumeMap.get(b.getBlockPoolId(), b.getLocalBlock().getBlockId())
          == null) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      } else {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b);
      }
    }
    return info;
  }

  /**
   * Get the meta info of a block stored in volumeMap. Block is looked up
   * without matching the generation stamp.
   * @param bpid block pool Id
   * @param blkid block Id
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  @VisibleForTesting
  ReplicaInfo getReplicaInfo(String bpid, long blkid)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(bpid, blkid);
    if (info == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":" + blkid);
    }
    return info;
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  @Override // FsDatasetSpi
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b,
      long blkOffset, long metaOffset) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo info = getReplicaInfo(b);
      FsVolumeReference ref = info.getVolume().obtainReference();
      try {
        InputStream blockInStream = info.getDataInputStream(blkOffset);
        try {
          InputStream metaInStream = info.getMetadataInputStream(metaOffset);
          return new ReplicaInputStreams(blockInStream, metaInStream, ref);
        } catch (IOException e) {
          IOUtils.cleanup(null, blockInStream);
          throw e;
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
    }
  }

  static File moveBlockFiles(Block b, ReplicaInfo replicaInfo, File destdir)
      throws IOException {
    final File dstfile = new File(destdir, b.getBlockName());
    final File dstmeta = FsDatasetUtil.getMetaFile(dstfile, b.getGenerationStamp());
    try {
      replicaInfo.renameMeta(dstmeta.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move meta file for " + b
          + " from " + replicaInfo.getMetadataURI() + " to " + dstmeta, e);
    }
    try {
      replicaInfo.renameData(dstfile.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + b
          + " from " + replicaInfo.getBlockURI() + " to "
          + dstfile.getAbsolutePath(), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("addFinalizedBlock: Moved " + replicaInfo.getMetadataURI()
          + " to " + dstmeta + " and " + replicaInfo.getBlockURI()
          + " to " + dstfile);
    }
    return dstfile;
  }

  /**
   * Copy the block and meta files for the given block to the given destination.
   * @return the new meta and block files.
   * @throws IOException
   */
  static File[] copyBlockFiles(long blockId, long genStamp,
      ReplicaInfo srcReplica, File destRoot, boolean calculateChecksum,
      int smallBufferSize, final Configuration conf) throws IOException {
    final File destDir = DatanodeUtil.idToBlockDir(destRoot, blockId);
    // blockName is same as the filename for the block
    final File dstFile = new File(destDir, srcReplica.getBlockName());
    final File dstMeta = FsDatasetUtil.getMetaFile(dstFile, genStamp);
    return copyBlockFiles(srcReplica, dstMeta, dstFile, calculateChecksum,
        smallBufferSize, conf);
  }

  static File[] copyBlockFiles(ReplicaInfo srcReplica, File dstMeta,
                               File dstFile, boolean calculateChecksum,
                               int smallBufferSize, final Configuration conf)
      throws IOException {

    if (calculateChecksum) {
      computeChecksum(srcReplica, dstMeta, smallBufferSize, conf);
    } else {
      try {
        srcReplica.copyMetadata(dstMeta.toURI());
      } catch (IOException e) {
        throw new IOException("Failed to copy " + srcReplica + " metadata to "
            + dstMeta, e);
      }
    }
    try {
      srcReplica.copyBlockdata(dstFile.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to copy " + srcReplica + " block file to "
          + dstFile, e);
    }
    if (LOG.isDebugEnabled()) {
      if (calculateChecksum) {
        LOG.debug("Copied " + srcReplica.getMetadataURI() + " meta to "
            + dstMeta + " and calculated checksum");
      } else {
        LOG.debug("Copied " + srcReplica.getBlockURI() + " to " + dstFile);
      }
    }
    return new File[] {dstMeta, dstFile};
  }

  /**
   * Move block files from one storage to another storage.
   * @return Returns the Old replicaInfo
   * @throws IOException
   */
  @Override
  public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
      StorageType targetStorageType) throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(block);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + block);
    }
    if (replicaInfo.getNumBytes() != block.getNumBytes()) {
      throw new IOException("Corrupted replica " + replicaInfo
          + " with a length of " + replicaInfo.getNumBytes()
          + " expected length is " + block.getNumBytes());
    }
    if (replicaInfo.getVolume().getStorageType() == targetStorageType) {
      throw new ReplicaAlreadyExistsException("Replica " + replicaInfo
          + " already exists on storage " + targetStorageType);
    }

    if (replicaInfo.isOnTransientStorage()) {
      // Block movement from RAM_DISK will be done by LazyPersist mechanism
      throw new IOException("Replica " + replicaInfo
          + " cannot be moved from storageType : "
          + replicaInfo.getVolume().getStorageType());
    }

    FsVolumeReference volumeRef = null;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      volumeRef = volumes.getNextVolume(targetStorageType, block.getNumBytes());
    }
    try {
      moveBlock(block, replicaInfo, volumeRef);
    } finally {
      if (volumeRef != null) {
        volumeRef.close();
      }
    }

    // Replace the old block if any to reschedule the scanning.
    return replicaInfo;
  }

  /**
   * Moves a block from a given volume to another.
   *
   * @param block       - Extended Block
   * @param replicaInfo - ReplicaInfo
   * @param volumeRef   - Volume Ref - Closed by caller.
   * @return newReplicaInfo
   * @throws IOException
   */
  private ReplicaInfo moveBlock(ExtendedBlock block, ReplicaInfo replicaInfo,
                                FsVolumeReference volumeRef) throws
      IOException {

    FsVolumeImpl targetVolume = (FsVolumeImpl) volumeRef.getVolume();
    // Copy files to temp dir first
    ReplicaInfo newReplicaInfo = targetVolume.moveBlockToTmpLocation(block,
        replicaInfo, smallBufferSize, conf);

    // Finalize the copied files
    newReplicaInfo = finalizeReplica(block.getBlockPoolId(), newReplicaInfo);
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // Increment numBlocks here as this block moved without knowing to BPS
      FsVolumeImpl volume = (FsVolumeImpl) newReplicaInfo.getVolume();
      volume.incrNumBlocks(block.getBlockPoolId());
    }

    removeOldReplica(replicaInfo, newReplicaInfo, block.getBlockPoolId());
    return newReplicaInfo;
  }

  /**
   * Moves a given block from one volume to another volume. This is used by disk
   * balancer.
   *
   * @param block       - ExtendedBlock
   * @param destination - Destination volume
   * @return Old replica info
   */
  @Override
  public ReplicaInfo moveBlockAcrossVolumes(ExtendedBlock block, FsVolumeSpi
      destination) throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(block);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + block);
    }

    FsVolumeReference volumeRef = null;

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      volumeRef = destination.obtainReference();
    }

    try {
      moveBlock(block, replicaInfo, volumeRef);
    } finally {
      if (volumeRef != null) {
        volumeRef.close();
      }
    }
    return replicaInfo;
  }

  /**
   * Compute and store the checksum for a block file that does not already have
   * its checksum computed.
   *
   * @param srcReplica source {@link ReplicaInfo}, containing only the checksum
   *     header, not a calculated checksum
   * @param dstMeta destination meta file, into which this method will write a
   *     full computed checksum
   * @param smallBufferSize buffer size to use
   * @param conf the {@link Configuration}
   * @throws IOException
   */
  static void computeChecksum(ReplicaInfo srcReplica, File dstMeta,
      int smallBufferSize, final Configuration conf)
      throws IOException {
    File srcMeta = new File(srcReplica.getMetadataURI());
    final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(srcMeta,
        DFSUtilClient.getIoFileBufferSize(conf));
    final byte[] data = new byte[1 << 16];
    final byte[] crcs = new byte[checksum.getChecksumSize(data.length)];

    DataOutputStream metaOut = null;
    try {
      File parentFile = dstMeta.getParentFile();
      if (parentFile != null) {
        if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
          throw new IOException("Destination '" + parentFile
              + "' directory cannot be created");
        }
      }
      metaOut = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(dstMeta), smallBufferSize));
      BlockMetadataHeader.writeHeader(metaOut, checksum);

      int offset = 0;
      try (InputStream dataIn = srcReplica.getDataInputStream(0)) {

        for (int n; (n = dataIn.read(data, offset, data.length - offset)) != -1; ) {
          if (n > 0) {
            n += offset;
            offset = n % checksum.getBytesPerChecksum();
            final int length = n - offset;

            if (length > 0) {
              checksum.calculateChunkedSums(data, 0, length, crcs, 0);
              metaOut.write(crcs, 0, checksum.getChecksumSize(length));

              System.arraycopy(data, length, data, 0, offset);
            }
          }
        }
      }

      // calculate and write the last crc
      checksum.calculateChunkedSums(data, 0, offset, crcs, 0);
      metaOut.write(crcs, 0, 4);
    } finally {
      IOUtils.cleanup(null, metaOut);
    }
  }


  @Override  // FsDatasetSpi
  public ReplicaHandler append(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.

      // check the validity of the parameter
      if (newGS < b.getGenerationStamp()) {
        throw new IOException("The new generation stamp " + newGS +
            " should be greater than the replica " + b + "'s generation stamp");
      }
      ReplicaInfo replicaInfo = getReplicaInfo(b);
      LOG.info("Appending to " + replicaInfo);
      if (replicaInfo.getState() != ReplicaState.FINALIZED) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
      }
      if (replicaInfo.getNumBytes() != expectedBlockLen) {
        throw new IOException("Corrupted replica " + replicaInfo +
            " with a length of " + replicaInfo.getNumBytes() +
            " expected length is " + expectedBlockLen);
      }

      FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
      ReplicaInPipeline replica = null;
      try {
        replica = append(b.getBlockPoolId(), replicaInfo, newGS,
            b.getNumBytes());
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(replica, ref);
    }
  }
  
  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param bpid block pool Id
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate block length
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private ReplicaInPipeline append(String bpid,
      ReplicaInfo replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // If the block is cached, start uncaching it.
      if (replicaInfo.getState() != ReplicaState.FINALIZED) {
        throw new IOException("Only a Finalized replica can be appended to; "
            + "Replica with blk id " + replicaInfo.getBlockId() + " has state "
            + replicaInfo.getState());
      }
      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, replicaInfo.getBlockId());

      // If there are any hardlinks to the block, break them.  This ensures
      // we are not appending to a file that is part of a previous/ directory.
      replicaInfo.breakHardLinksIfNeeded();

      FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
      ReplicaInPipeline rip = v.append(bpid, replicaInfo,
          newGS, estimateBlockLen);
      if (rip.getReplicaInfo().getState() != ReplicaState.RBW) {
        throw new IOException("Append on block " + replicaInfo.getBlockId() +
            " returned a replica of state " + rip.getReplicaInfo().getState()
            + "; expected RBW");
      }
      // Replace finalized replica by a RBW replica in replicas map
      volumeMap.add(bpid, rip.getReplicaInfo());
      return rip;
    }
  }

  @SuppressWarnings("serial")
  private static class MustStopExistingWriter extends Exception {
    private final ReplicaInPipeline rip;

    MustStopExistingWriter(ReplicaInPipeline rip) {
      this.rip = rip;
    }

    ReplicaInPipeline getReplicaInPipeline() {
      return rip;
    }
  }

  private ReplicaInfo recoverCheck(ExtendedBlock b, long newGS, 
      long expectedBlockLen) throws IOException, MustStopExistingWriter {
    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
    
    // check state
    if (replicaInfo.getState() != ReplicaState.FINALIZED &&
        replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA + replicaInfo);
    }

    // check generation stamp
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + replicaGenerationStamp
          + ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }
    
    // stop the previous writer before check a replica's length
    long replicaLen = replicaInfo.getNumBytes();
    if (replicaInfo.getState() == ReplicaState.RBW) {
      ReplicaInPipeline rbw = (ReplicaInPipeline) replicaInfo;
      if (!rbw.attemptToSetWriter(null, Thread.currentThread())) {
        throw new MustStopExistingWriter(rbw);
      }
      // check length: bytesRcvd, bytesOnDisk, and bytesAcked should be the same
      if (replicaLen != rbw.getBytesOnDisk() 
          || replicaLen != rbw.getBytesAcked()) {
        throw new ReplicaAlreadyExistsException("RBW replica " + replicaInfo + 
            "bytesRcvd(" + rbw.getNumBytes() + "), bytesOnDisk(" + 
            rbw.getBytesOnDisk() + "), and bytesAcked(" + rbw.getBytesAcked() +
            ") are not the same.");
      }
    }
    
    // check block length
    if (replicaLen != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo + 
          " with a length of " + replicaLen + 
          " expected length is " + expectedBlockLen);
    }
    
    return replicaInfo;
  }

  @Override  // FsDatasetSpi
  public ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    while (true) {
      try {
        try (AutoCloseableLock lock = datasetLock.acquire()) {
          ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
          FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
          ReplicaInPipeline replica;
          try {
            // change the replica's state/gs etc.
            if (replicaInfo.getState() == ReplicaState.FINALIZED) {
              replica = append(b.getBlockPoolId(), replicaInfo,
                               newGS, b.getNumBytes());
            } else { //RBW
              replicaInfo.bumpReplicaGS(newGS);
              replica = (ReplicaInPipeline) replicaInfo;
            }
          } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
          }
          return new ReplicaHandler(replica, ref);
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline()
            .stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }

  @Override // FsDatasetSpi
  public Replica recoverClose(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    LOG.info("Recover failed close " + b);
    while (true) {
      try {
        try (AutoCloseableLock lock = datasetLock.acquire()) {
          // check replica's state
          ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
          // bump the replica's GS
          replicaInfo.bumpReplicaGS(newGS);
          // finalize the replica if RBW
          if (replicaInfo.getState() == ReplicaState.RBW) {
            finalizeReplica(b.getBlockPoolId(), replicaInfo);
          }
          return replicaInfo;
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline()
            .stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }
  
  @Override // FsDatasetSpi
  public ReplicaHandler createRbw(
      StorageType storageType, ExtendedBlock b, boolean allowLazyPersist)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
          b.getBlockId());
      if (replicaInfo != null) {
        throw new ReplicaAlreadyExistsException("Block " + b +
            " already exists in state " + replicaInfo.getState() +
            " and thus cannot be created.");
      }
      // create a new block
      FsVolumeReference ref = null;

      // Use ramdisk only if block size is a multiple of OS page size.
      // This simplifies reservation for partially used replicas
      // significantly.
      if (allowLazyPersist &&
          lazyWriter != null &&
          b.getNumBytes() % cacheManager.getOsPageSize() == 0 &&
          reserveLockedMemory(b.getNumBytes())) {
        try {
          // First try to place the block on a transient volume.
          ref = volumes.getNextTransientVolume(b.getNumBytes());
          datanode.getMetrics().incrRamDiskBlocksWrite();
        } catch (DiskOutOfSpaceException de) {
          // Ignore the exception since we just fall back to persistent storage.
        } finally {
          if (ref == null) {
            cacheManager.release(b.getNumBytes());
          }
        }
      }

      if (ref == null) {
        ref = volumes.getNextVolume(storageType, b.getNumBytes());
      }

      FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
      // create an rbw file to hold block in the designated volume

      if (allowLazyPersist && !v.isTransientStorage()) {
        datanode.getMetrics().incrRamDiskBlocksWriteFallback();
      }

      ReplicaInPipeline newReplicaInfo;
      try {
        newReplicaInfo = v.createRbw(b);
        if (newReplicaInfo.getReplicaInfo().getState() != ReplicaState.RBW) {
          throw new IOException("CreateRBW returned a replica of state "
              + newReplicaInfo.getReplicaInfo().getState()
              + " for block " + b.getBlockId());
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }

      volumeMap.add(b.getBlockPoolId(), newReplicaInfo.getReplicaInfo());
      return new ReplicaHandler(newReplicaInfo, ref);
    }
  }

  @Override // FsDatasetSpi
  public ReplicaHandler recoverRbw(
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    LOG.info("Recover RBW replica " + b);

    while (true) {
      try {
        try (AutoCloseableLock lock = datasetLock.acquire()) {
          ReplicaInfo replicaInfo =
              getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
          // check the replica's state
          if (replicaInfo.getState() != ReplicaState.RBW) {
            throw new ReplicaNotFoundException(
                ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
          }
          ReplicaInPipeline rbw = (ReplicaInPipeline)replicaInfo;
          if (!rbw.attemptToSetWriter(null, Thread.currentThread())) {
            throw new MustStopExistingWriter(rbw);
          }
          LOG.info("At " + datanode.getDisplayName() + ", Recovering " + rbw);
          return recoverRbwImpl(rbw, b, newGS, minBytesRcvd, maxBytesRcvd);
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline().stopWriter(
            datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }

  private ReplicaHandler recoverRbwImpl(ReplicaInPipeline rbw,
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // check generation stamp
      long replicaGenerationStamp = rbw.getGenerationStamp();
      if (replicaGenerationStamp < b.getGenerationStamp() ||
          replicaGenerationStamp > newGS) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
                ". Expected GS range is [" + b.getGenerationStamp() + ", " +
                newGS + "].");
      }

      // check replica length
      long bytesAcked = rbw.getBytesAcked();
      long numBytes = rbw.getNumBytes();
      if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd) {
        throw new ReplicaNotFoundException("Unmatched length replica " +
            rbw + ": BytesAcked = " + bytesAcked +
            " BytesRcvd = " + numBytes + " are not in the range of [" +
            minBytesRcvd + ", " + maxBytesRcvd + "].");
      }

      FsVolumeReference ref = rbw.getReplicaInfo()
          .getVolume().obtainReference();
      try {
        // Truncate the potentially corrupt portion.
        // If the source was client and the last node in the pipeline was lost,
        // any corrupt data written after the acked length can go unnoticed.
        if (numBytes > bytesAcked) {
          rbw.getReplicaInfo().truncateBlock(bytesAcked);
          rbw.setNumBytes(bytesAcked);
          rbw.setLastChecksumAndDataLen(bytesAcked, null);
        }

        // bump the replica's generation stamp to newGS
        rbw.getReplicaInfo().bumpReplicaGS(newGS);
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(rbw, ref);
    }
  }
  
  @Override // FsDatasetSpi
  public ReplicaInPipeline convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final long blockId = b.getBlockId();
      final long expectedGs = b.getGenerationStamp();
      final long visible = b.getNumBytes();
      LOG.info("Convert " + b + " from Temporary to RBW, visible length="
          + visible);

      final ReplicaInfo temp;
      {
        // get replica
        final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), blockId);
        if (r == null) {
          throw new ReplicaNotFoundException(
              ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
        }
        // check the replica's state
        if (r.getState() != ReplicaState.TEMPORARY) {
          throw new ReplicaAlreadyExistsException(
              "r.getState() != ReplicaState.TEMPORARY, r=" + r);
        }
        temp = r;
      }
      // check generation stamp
      if (temp.getGenerationStamp() != expectedGs) {
        throw new ReplicaAlreadyExistsException(
            "temp.getGenerationStamp() != expectedGs = " + expectedGs
                + ", temp=" + temp);
      }

      // TODO: check writer?
      // set writer to the current thread
      // temp.setWriter(Thread.currentThread());

      // check length
      final long numBytes = temp.getNumBytes();
      if (numBytes < visible) {
        throw new IOException(numBytes + " = numBytes < visible = "
            + visible + ", temp=" + temp);
      }
      // check volume
      final FsVolumeImpl v = (FsVolumeImpl) temp.getVolume();
      if (v == null) {
        throw new IOException("r.getVolume() = null, temp=" + temp);
      }

      final ReplicaInPipeline rbw = v.convertTemporaryToRbw(b, temp);

      if(rbw.getState() != ReplicaState.RBW) {
        throw new IOException("Expected replica state: " + ReplicaState.RBW
            + " obtained " + rbw.getState() + " for converting block "
            + b.getBlockId());
      }
      // overwrite the RBW in the volume map
      volumeMap.add(b.getBlockPoolId(), rbw.getReplicaInfo());
      return rbw;
    }
  }

  @Override // FsDatasetSpi
  public ReplicaHandler createTemporary(
      StorageType storageType, ExtendedBlock b) throws IOException {
    long startTimeMs = Time.monotonicNow();
    long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();
    ReplicaInfo lastFoundReplicaInfo = null;
    do {
      try (AutoCloseableLock lock = datasetLock.acquire()) {
        ReplicaInfo currentReplicaInfo =
            volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        if (currentReplicaInfo == lastFoundReplicaInfo) {
          if (lastFoundReplicaInfo != null) {
            invalidate(b.getBlockPoolId(), new Block[] { lastFoundReplicaInfo });
          }
          FsVolumeReference ref =
              volumes.getNextVolume(storageType, b.getNumBytes());
          FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
          ReplicaInPipeline newReplicaInfo;
          try {
            newReplicaInfo = v.createTemporary(b);
          } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
          }

          volumeMap.add(b.getBlockPoolId(), newReplicaInfo.getReplicaInfo());
          return new ReplicaHandler(newReplicaInfo, ref);
        } else {
          if (!(currentReplicaInfo.getGenerationStamp() < b.getGenerationStamp()
                && (currentReplicaInfo.getState() == ReplicaState.TEMPORARY
                    || currentReplicaInfo.getState() == ReplicaState.RBW))) {
            throw new ReplicaAlreadyExistsException("Block " + b
                + " already exists in state " + currentReplicaInfo.getState()
                + " and thus cannot be created.");
          }
          lastFoundReplicaInfo = currentReplicaInfo;
        }
      }

      // Hang too long, just bail out. This is not supposed to happen.
      long writerStopMs = Time.monotonicNow() - startTimeMs;
      if (writerStopMs > writerStopTimeoutMs) {
        LOG.warn("Unable to stop existing writer for block " + b + " after " 
            + writerStopMs + " miniseconds.");
        throw new IOException("Unable to stop existing writer for block " + b
            + " after " + writerStopMs + " miniseconds.");
      }

      // Stop the previous writer
      ((ReplicaInPipeline)lastFoundReplicaInfo).stopWriter(writerStopTimeoutMs);
    } while (true);
  }

  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  @Override // FsDatasetSpi
  public void adjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams streams, 
      int checksumSize) throws IOException {
    FileOutputStream file = (FileOutputStream)streams.getChecksumOut();
    FileChannel channel = file.getChannel();
    long oldPos = channel.position();
    long newPos = oldPos - checksumSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing meta file offset of block " + b + " from " +
          oldPos + " to " + newPos);
    }
    channel.position(newPos);
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  @Override // FsDatasetSpi
  public void finalizeBlock(ExtendedBlock b) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      if (Thread.interrupted()) {
        // Don't allow data modifications from interrupted threads
        throw new IOException("Cannot finalize block from Interrupted Thread");
      }
      ReplicaInfo replicaInfo = getReplicaInfo(b);
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        // this is legal, when recovery happens on a file that has
        // been opened for append but never modified
        return;
      }
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
  }

  private ReplicaInfo finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo newReplicaInfo = null;
      if (replicaInfo.getState() == ReplicaState.RUR &&
          replicaInfo.getOriginalReplica().getState()
          == ReplicaState.FINALIZED) {
        newReplicaInfo = replicaInfo.getOriginalReplica();
      } else {
        FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
        if (v == null) {
          throw new IOException("No volume for block " + replicaInfo);
        }

        newReplicaInfo = v.addFinalizedBlock(
            bpid, replicaInfo, replicaInfo, replicaInfo.getBytesReserved());
        if (v.isTransientStorage()) {
          releaseLockedMemory(
              replicaInfo.getOriginalBytesReserved()
                  - replicaInfo.getNumBytes(),
              false);
          ramDiskReplicaTracker.addReplica(
              bpid, replicaInfo.getBlockId(), v, replicaInfo.getNumBytes());
          datanode.getMetrics().addRamDiskBytesWrite(replicaInfo.getNumBytes());
        }
      }
      assert newReplicaInfo.getState() == ReplicaState.FINALIZED
          : "Replica should be finalized";
      volumeMap.add(bpid, newReplicaInfo);
      return newReplicaInfo;
    }
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FsDatasetSpi
  public void unfinalizeBlock(ExtendedBlock b) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
          b.getLocalBlock());
      if (replicaInfo != null &&
          replicaInfo.getState() == ReplicaState.TEMPORARY) {
        // remove from volumeMap
        volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());

        // delete the on-disk temp file
        if (delBlockFromDisk(replicaInfo)) {
          LOG.warn("Block " + b + " unfinalized and removed. ");
        }
        if (replicaInfo.getVolume().isTransientStorage()) {
          ramDiskReplicaTracker.discardReplica(b.getBlockPoolId(),
              b.getBlockId(), true);
        }
      }
    }
  }

  /**
   * Remove a block from disk
   * @param info the replica that needs to be deleted
   * @return true if data for the replica are deleted; false otherwise
   */
  private boolean delBlockFromDisk(ReplicaInfo info) {
    
    if (!info.deleteBlockData()) {
      LOG.warn("Not able to delete the block data for replica " + info);
      return false;
    } else { // remove the meta file
      if (!info.deleteMetadata()) {
        LOG.warn("Not able to delete the meta data for replica " + info);
        return false;
      }
    }
    return true;
  }

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    return cacheManager.getCachedBlocks(bpid);
  }

  @Override
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReportsMap =
        new HashMap<DatanodeStorage, BlockListAsLongs>();

    Map<String, BlockListAsLongs.Builder> builders =
        new HashMap<String, BlockListAsLongs.Builder>();

    List<FsVolumeImpl> curVolumes = null;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      curVolumes = volumes.getVolumes();
      for (FsVolumeSpi v : curVolumes) {
        builders.put(v.getStorageID(), BlockListAsLongs.builder(maxDataLength));
      }

      Set<String> missingVolumesReported = new HashSet<>();
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        String volStorageID = b.getVolume().getStorageID();
        if (!builders.containsKey(volStorageID)) {
          if (!missingVolumesReported.contains(volStorageID)) {
            LOG.warn("Storage volume: " + volStorageID + " missing for the"
                + " replica block: " + b + ". Probably being removed!");
            missingVolumesReported.add(volStorageID);
          }
          continue;
        }
        switch(b.getState()) {
        case FINALIZED:
        case RBW:
        case RWR:
          builders.get(b.getVolume().getStorageID()).add(b);
          break;
        case RUR:
          ReplicaInfo orig = b.getOriginalReplica();
          builders.get(b.getVolume().getStorageID()).add(orig);
          break;
        case TEMPORARY:
          break;
        default:
          assert false : "Illegal ReplicaInfo state.";
        }
      }
    }

    for (FsVolumeImpl v : curVolumes) {
      blockReportsMap.put(v.toDatanodeStorage(),
                          builders.get(v.getStorageID()).build());
    }

    return blockReportsMap;
  }

  /**
   * Gets a list of references to the finalized blocks for the given block pool.
   * <p>
   * Callers of this function should call
   * {@link FsDatasetSpi#acquireDatasetLock} to avoid blocks' status being
   * changed during list iteration.
   * </p>
   * @return a list of references to the finalized blocks for the given block
   *         pool.
   */
  @Override
  public List<ReplicaInfo> getFinalizedBlocks(String bpid) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final List<ReplicaInfo> finalized = new ArrayList<ReplicaInfo>(
          volumeMap.size(bpid));
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        if (b.getState() == ReplicaState.FINALIZED) {
          finalized.add(b);
        }
      }
      return finalized;
    }
  }

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
  @Override // FsDatasetSpi
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException,
      FileNotFoundException, EOFException, IOException {
    final ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(b);
    }
    if (replicaInfo.getState() != state) {
      throw new UnexpectedReplicaStateException(b,state);
    }
    if (!replicaInfo.blockDataExists()) {
      throw new FileNotFoundException(replicaInfo.getBlockURI().toString());
    }
    long onDiskLength = getLength(b);
    if (onDiskLength < minLength) {
      throw new EOFException(b + "'s on-disk length " + onDiskLength
          + " is shorter than minLength " + minLength);
    }
  }

  /**
   * Check whether the given block is a valid one.
   * valid means finalized
   */
  @Override // FsDatasetSpi
  public boolean isValidBlock(ExtendedBlock b) {
    return isValid(b, ReplicaState.FINALIZED);
  }
  
  /**
   * Check whether the given block is a valid RBW.
   */
  @Override // {@link FsDatasetSpi}
  public boolean isValidRbw(final ExtendedBlock b) {
    return isValid(b, ReplicaState.RBW);
  }

  /** Does the block exist and have the given state? */
  private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
    try {
      checkBlock(b, 0, state);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  ReplicaInfo validateBlockFile(String bpid, long blockId) {
    //Should we check for metadata file too?
    final ReplicaInfo r;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      r = volumeMap.get(bpid, blockId);
    }

    if (r != null) {
      if (r.blockDataExists()) {
        return r;
      }
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskErrorAsync();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("blockId=" + blockId + ", replica=" + r);
    }
    return null;
  }

  /** Check the files of a replica. */
  static void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //check replica's data exists
    if (!r.blockDataExists()) {
      throw new FileNotFoundException("Block data not found, r=" + r);
    }
    if (r.getBytesOnDisk() != r.getBlockDataLength()) {
      throw new IOException("Block length mismatch, len="
          + r.getBlockDataLength() + " but r=" + r);
    }

    //check replica's meta file
    if (!r.metadataExists()) {
      throw new IOException(r.getMetadataURI() + " does not exist, r=" + r);
    }
    if (r.getMetadataLength() == 0) {
      throw new IOException("Metafile is empty, r=" + r);
    }
  }

  /**
   * We're informed that a block is no longer valid. Delete it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      final ReplicaInfo removing;
      final FsVolumeImpl v;
      try (AutoCloseableLock lock = datasetLock.acquire()) {
        final ReplicaInfo info = volumeMap.get(bpid, invalidBlks[i]);
        if (info == null) {
          ReplicaInfo infoByBlockId =
              volumeMap.get(bpid, invalidBlks[i].getBlockId());
          if (infoByBlockId == null) {
            // It is okay if the block is not found -- it
            // may be deleted earlier.
            LOG.info("Failed to delete replica " + invalidBlks[i]
                + ": ReplicaInfo not found.");
          } else {
            errors.add("Failed to delete replica " + invalidBlks[i]
                + ": GenerationStamp not matched, existing replica is "
                + Block.toString(infoByBlockId));
          }
          continue;
        }

        v = (FsVolumeImpl)info.getVolume();
        if (v == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". No volume for replica " + info);
          continue;
        }
        try {
          File blockFile = new File(info.getBlockURI());
          if (blockFile != null && blockFile.getParentFile() == null) {
            errors.add("Failed to delete replica " + invalidBlks[i]
                +  ". Parent not found for block file: " + blockFile);
            continue;
          }
        } catch(IllegalArgumentException e) {
          LOG.warn("Parent directory check failed; replica " + info
              + " is not backed by a local file");
        }
        removing = volumeMap.remove(bpid, invalidBlks[i]);
        addDeletingBlock(bpid, removing.getBlockId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Block file " + removing.getBlockURI()
              + " is to be deleted");
        }
        if (removing instanceof ReplicaInPipeline) {
          ((ReplicaInPipeline) removing).releaseAllBytesReserved();
        }
      }

      if (v.isTransientStorage()) {
        RamDiskReplica replicaInfo =
          ramDiskReplicaTracker.getReplica(bpid, invalidBlks[i].getBlockId());
        if (replicaInfo != null) {
          if (!replicaInfo.getIsPersisted()) {
            datanode.getMetrics().incrRamDiskBlocksDeletedBeforeLazyPersisted();
          }
          ramDiskReplicaTracker.discardReplica(replicaInfo.getBlockPoolId(),
            replicaInfo.getBlockId(), true);
        }
      }

      // If a DFSClient has the replica in its cache of short-circuit file
      // descriptors (and the client is using ShortCircuitShm), invalidate it.
      datanode.getShortCircuitRegistry().processBlockInvalidation(
                new ExtendedBlockId(invalidBlks[i].getBlockId(), bpid));

      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, invalidBlks[i].getBlockId());

      // Delete the block asynchronously to make sure we can do it fast enough.
      // It's ok to unlink the block file before the uncache operation
      // finishes.
      try {
        asyncDiskService.deleteAsync(v.obtainReference(), removing,
            new ExtendedBlock(bpid, invalidBlks[i]),
            dataStorage.getTrashDirectoryForReplica(bpid, removing));
      } catch (ClosedChannelException e) {
        LOG.warn("Volume " + v + " is closed, ignore the deletion task for " +
            "block " + invalidBlks[i]);
      }
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
        .append(errors.size()).append(" (out of ").append(invalidBlks.length)
        .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }

  /**
   * Invalidate a block but does not delete the actual on-disk block file.
   *
   * It should only be used when deactivating disks.
   *
   * @param bpid the block pool ID.
   * @param block The block to be invalidated.
   */
  public void invalidate(String bpid, ReplicaInfo block) {
    // If a DFSClient has the replica in its cache of short-circuit file
    // descriptors (and the client is using ShortCircuitShm), invalidate it.
    datanode.getShortCircuitRegistry().processBlockInvalidation(
        new ExtendedBlockId(block.getBlockId(), bpid));

    // If the block is cached, start uncaching it.
    cacheManager.uncacheBlock(bpid, block.getBlockId());

    datanode.notifyNamenodeDeletedBlock(new ExtendedBlock(bpid, block),
        block.getStorageUuid());
  }

  /**
   * Asynchronously attempts to cache a single block via {@link FsDatasetCache}.
   */
  private void cacheBlock(String bpid, long blockId) {
    FsVolumeImpl volume;
    String blockFileName;
    long length, genstamp;
    Executor volumeExecutor;

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo info = volumeMap.get(bpid, blockId);
      boolean success = false;
      try {
        if (info == null) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": ReplicaInfo not found.");
          return;
        }
        if (info.getState() != ReplicaState.FINALIZED) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": replica is not finalized; it is in state " +
              info.getState());
          return;
        }
        try {
          volume = (FsVolumeImpl)info.getVolume();
          if (volume == null) {
            LOG.warn("Failed to cache block with id " + blockId + ", pool " +
                bpid + ": volume not found.");
            return;
          }
        } catch (ClassCastException e) {
          LOG.warn("Failed to cache block with id " + blockId +
              ": volume was not an instance of FsVolumeImpl.");
          return;
        }
        if (volume.isTransientStorage()) {
          LOG.warn("Caching not supported on block with id " + blockId +
              " since the volume is backed by RAM.");
          return;
        }
        success = true;
      } finally {
        if (!success) {
          cacheManager.numBlocksFailedToCache.incrementAndGet();
        }
      }
      blockFileName = info.getBlockURI().toString();
      length = info.getVisibleLength();
      genstamp = info.getGenerationStamp();
      volumeExecutor = volume.getCacheExecutor();
    }
    cacheManager.cacheBlock(blockId, bpid, 
        blockFileName, length, genstamp, volumeExecutor);
  }

  @Override // FsDatasetSpi
  public void cache(String bpid, long[] blockIds) {
    for (int i=0; i < blockIds.length; i++) {
      cacheBlock(bpid, blockIds[i]);
    }
  }

  @Override // FsDatasetSpi
  public void uncache(String bpid, long[] blockIds) {
    for (int i=0; i < blockIds.length; i++) {
      cacheManager.uncacheBlock(bpid, blockIds[i]);
    }
  }

  @Override
  public boolean isCached(String bpid, long blockId) {
    return cacheManager.isCached(bpid, blockId);
  }

  @Override // FsDatasetSpi
  public boolean contains(final ExtendedBlock block) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final long blockId = block.getLocalBlock().getBlockId();
      final String bpid = block.getBlockPoolId();
      final ReplicaInfo r = volumeMap.get(bpid, blockId);
      return (r != null && r.blockDataExists());
    }
  }

  /**
   * check if a data directory is healthy
   *
   * if some volumes failed - the caller must emove all the blocks that belong
   * to these failed volumes.
   * @return the failed volumes. Returns null if no volume failed.
   */
  @Override // FsDatasetSpi
  public Set<StorageLocation> checkDataDir() {
   return volumes.checkDirs();
  }
    

  @Override // FsDatasetSpi
  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
   */
  void registerMBean(final String datanodeUuid) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    try {
      StandardMBean bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + datanodeUuid, bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering FSDatasetState MBean", e);
    }
    LOG.info("Registered FSDatasetState MBean");
  }

  @Override // FsDatasetSpi
  public void shutdown() {
    fsRunning = false;

    if (lazyWriter != null) {
      ((LazyWriter) lazyWriter.getRunnable()).stop();
      lazyWriter.interrupt();
    }

    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
    }
    
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }

    if (asyncLazyPersistService != null) {
      asyncLazyPersistService.shutdown();
    }
    
    if(volumes != null) {
      volumes.shutdown();
    }

    if (lazyWriter != null) {
      try {
        lazyWriter.join();
      } catch (InterruptedException ie) {
        LOG.warn("FsDatasetImpl.shutdown ignoring InterruptedException " +
                     "from LazyWriter.join");
      }
    }
  }

  @Override // FSDatasetMBean
  public String getStorageInfo() {
    return toString();
  }

  /**
   * Reconcile the difference between blocks on the disk and blocks in
   * volumeMap
   *
   * Check the given block for inconsistencies. Look at the
   * current state of the block and reconcile the differences as follows:
   * <ul>
   * <li>If the block file is missing, delete the block from volumeMap</li>
   * <li>If the block file exists and the block is missing in volumeMap,
   * add the block to volumeMap <li>
   * <li>If generation stamp does not match, then update the block with right
   * generation stamp</li>
   * <li>If the block length in memory does not match the actual block file length
   * then mark the block as corrupt and update the block length in memory</li>
   * <li>If the file in {@link ReplicaInfo} does not match the file on
   * the disk, update {@link ReplicaInfo} with the correct file</li>
   * </ul>
   *
   * @param blockId Block that differs
   * @param diskFile Block file on the disk
   * @param diskMetaFile Metadata file from on the disk
   * @param vol Volume of the block file
   */
  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) throws IOException {
    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      memBlockInfo = volumeMap.get(bpid, blockId);
      if (memBlockInfo != null && memBlockInfo.getState() != ReplicaState.FINALIZED) {
        // Block is not finalized - ignore the difference
        return;
      }

      final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
            HdfsConstants.GRANDFATHER_GENERATION_STAMP;

      if (diskFile == null || !diskFile.exists()) {
        if (memBlockInfo == null) {
          // Block file does not exist and block does not exist in memory
          // If metadata file exists then delete it
          if (diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.delete()) {
            LOG.warn("Deleted a metadata file without a block "
                + diskMetaFile.getAbsolutePath());
          }
          return;
        }
        if (!memBlockInfo.blockDataExists()) {
          // Block is in memory and not on the disk
          // Remove the block from volumeMap
          volumeMap.remove(bpid, blockId);
          if (vol.isTransientStorage()) {
            ramDiskReplicaTracker.discardReplica(bpid, blockId, true);
          }
          LOG.warn("Removed block " + blockId
              + " from memory with missing block file on the disk");
          // Finally remove the metadata file
          if (diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.delete()) {
            LOG.warn("Deleted a metadata file for the deleted block "
                + diskMetaFile.getAbsolutePath());
          }
        }
        return;
      }
      /*
       * Block file exists on the disk
       */
      if (memBlockInfo == null) {
        // Block is missing in memory - add the block to volumeMap
        ReplicaInfo diskBlockInfo = new ReplicaBuilder(ReplicaState.FINALIZED)
            .setBlockId(blockId)
            .setLength(diskFile.length())
            .setGenerationStamp(diskGS)
            .setFsVolume(vol)
            .setDirectoryToUse(diskFile.getParentFile())
            .build();
        volumeMap.add(bpid, diskBlockInfo);
        if (vol.isTransientStorage()) {
          long lockedBytesReserved =
              cacheManager.reserve(diskBlockInfo.getNumBytes()) > 0 ?
                  diskBlockInfo.getNumBytes() : 0;
          ramDiskReplicaTracker.addReplica(
              bpid, blockId, (FsVolumeImpl) vol, lockedBytesReserved);
        }
        LOG.warn("Added missing block to memory " + diskBlockInfo);
        return;
      }
      /*
       * Block exists in volumeMap and the block file exists on the disk
       */
      // Compare block files
      if (memBlockInfo.blockDataExists()) {
        if (memBlockInfo.getBlockURI().compareTo(diskFile.toURI()) != 0) {
          if (diskMetaFile.exists()) {
            if (memBlockInfo.metadataExists()) {
              // We have two sets of block+meta files. Decide which one to
              // keep.
              ReplicaInfo diskBlockInfo =
                  new ReplicaBuilder(ReplicaState.FINALIZED)
                    .setBlockId(blockId)
                    .setLength(diskFile.length())
                    .setGenerationStamp(diskGS)
                    .setFsVolume(vol)
                    .setDirectoryToUse(diskFile.getParentFile())
                    .build();
              ((FsVolumeImpl) vol).resolveDuplicateReplicas(bpid,
                  memBlockInfo, diskBlockInfo, volumeMap);
            }
          } else {
            if (!diskFile.delete()) {
              LOG.warn("Failed to delete " + diskFile);
            }
          }
        }
      } else {
        // Block refers to a block file that does not exist.
        // Update the block with the file found on the disk. Since the block
        // file and metadata file are found as a pair on the disk, update
        // the block based on the metadata file found on the disk
        LOG.warn("Block file in replica "
            + memBlockInfo.getBlockURI()
            + " does not exist. Updating it to the file found during scan "
            + diskFile.getAbsolutePath());
        memBlockInfo.updateWithReplica(
            StorageLocation.parse(diskFile.toString()));

        LOG.warn("Updating generation stamp for block " + blockId
            + " from " + memBlockInfo.getGenerationStamp() + " to " + diskGS);
        memBlockInfo.setGenerationStamp(diskGS);
      }

      // Compare generation stamp
      if (memBlockInfo.getGenerationStamp() != diskGS) {
        File memMetaFile = FsDatasetUtil.getMetaFile(diskFile, 
            memBlockInfo.getGenerationStamp());
        if (memMetaFile.exists()) {
          if (memMetaFile.compareTo(diskMetaFile) != 0) {
            LOG.warn("Metadata file in memory "
                + memMetaFile.getAbsolutePath()
                + " does not match file found by scan "
                + (diskMetaFile == null? null: diskMetaFile.getAbsolutePath()));
          }
        } else {
          // Metadata file corresponding to block in memory is missing
          // If metadata file found during the scan is on the same directory
          // as the block file, then use the generation stamp from it
          try {
            File memFile = new File(memBlockInfo.getBlockURI());
            long gs = diskMetaFile != null && diskMetaFile.exists()
                && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
                : HdfsConstants.GRANDFATHER_GENERATION_STAMP;

            LOG.warn("Updating generation stamp for block " + blockId
                + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

            memBlockInfo.setGenerationStamp(gs);
          } catch (IllegalArgumentException e) {
            //exception arises because the URI cannot be converted to a file
            LOG.warn("Block URI could not be resolved to a file", e);
          }
        }
      }

      // Compare block size
      if (memBlockInfo.getNumBytes() != memBlockInfo.getBlockDataLength()) {
        // Update the length based on the block file
        corruptBlock = new Block(memBlockInfo);
        LOG.warn("Updating size of block " + blockId + " from "
            + memBlockInfo.getNumBytes() + " to "
            + memBlockInfo.getBlockDataLength());
        memBlockInfo.setNumBytes(memBlockInfo.getBlockDataLength());
      }
    }

    // Send corrupt block report outside the lock
    if (corruptBlock != null) {
      LOG.warn("Reporting the block " + corruptBlock
          + " as corrupt due to length mismatch");
      try {
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock),
            memBlockInfo.getVolume());
      } catch (IOException e) {
        LOG.warn("Failed to repot bad block " + corruptBlock, e);
      }
    }
  }

  /**
   * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
   */
  @Override // FsDatasetSpi
  @Deprecated
  public ReplicaInfo getReplica(String bpid, long blockId) {
    return volumeMap.get(bpid, blockId);
  }

  @Override 
  public String getReplicaString(String bpid, long blockId) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica r = volumeMap.get(bpid, blockId);
      return r == null ? "null" : r.toString();
    }
  }

  @Override // FsDatasetSpi
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), volumeMap,
        rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(),
        datanode.getDnConf().getXceiverStopTimeout());
  }

  /** static version of {@link #initReplicaRecovery(RecoveringBlock)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid, ReplicaMap map,
      Block block, long recoveryId, long xceiverStopTimeout) throws IOException {
    while (true) {
      try {
        try (AutoCloseableLock lock = map.getLock().acquire()) {
          return initReplicaRecoveryImpl(bpid, map, block, recoveryId);
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline().stopWriter(xceiverStopTimeout);
      }
    }
  }

  static ReplicaRecoveryInfo initReplicaRecoveryImpl(String bpid, ReplicaMap map,
      Block block, long recoveryId)
          throws IOException, MustStopExistingWriter {
    final ReplicaInfo replica = map.get(bpid, block.getBlockId());
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      return null;
    }

    //stop writer if there is any
    if (replica.getState() == ReplicaState.TEMPORARY ||
        replica.getState() == ReplicaState.RBW) {
      final ReplicaInPipeline rip = (ReplicaInPipeline)replica;
      if (!rip.attemptToSetWriter(null, Thread.currentThread())) {
        throw new MustStopExistingWriter(rip);
      }

      //check replica bytes on disk.
      if (replica.getBytesOnDisk() < replica.getVisibleLength()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " getBytesOnDisk() < getVisibleLength(), rip=" + replica);
      }

      //check the replica's files
      checkReplicaFiles(replica);
    }

    //check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }

    //check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getGenerationStamp() >= recoveryId = " + recoveryId
          + ", block=" + block + ", replica=" + replica);
    }

    //check RUR
    final ReplicaInfo rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = replica;
      if (rur.getRecoveryID() >= recoveryId) {
        throw new RecoveryInProgressException(
            "rur.getRecoveryID() >= recoveryId = " + recoveryId
            + ", block=" + block + ", rur=" + rur);
      }
      final long oldRecoveryID = rur.getRecoveryID();
      rur.setRecoveryID(recoveryId);
      LOG.info("initReplicaRecovery: update recovery id for " + block
          + " from " + oldRecoveryID + " to " + recoveryId);
    }
    else {
      rur = new ReplicaBuilder(ReplicaState.RUR)
          .from(replica).setRecoveryId(recoveryId).build();
      map.add(bpid, rur);
      LOG.info("initReplicaRecovery: changing replica state for "
          + block + " from " + replica.getState()
          + " to " + rur.getState());
    }
    return rur.createInfo();
  }

  @Override // FsDatasetSpi
  public Replica updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
                                    final long newBlockId,
                                    final long newlength) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      //get replica
      final String bpid = oldBlock.getBlockPoolId();
      final ReplicaInfo replica = volumeMap.get(bpid, oldBlock.getBlockId());
      LOG.info("updateReplica: " + oldBlock
          + ", recoveryId=" + recoveryId
          + ", length=" + newlength
          + ", replica=" + replica);

      //check replica
      if (replica == null) {
        throw new ReplicaNotFoundException(oldBlock);
      }

      //check replica state
      if (replica.getState() != ReplicaState.RUR) {
        throw new IOException("replica.getState() != " + ReplicaState.RUR
            + ", replica=" + replica);
      }

      //check replica's byte on disk
      if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " replica.getBytesOnDisk() != block.getNumBytes(), block="
            + oldBlock + ", replica=" + replica);
      }

      //check replica files before update
      checkReplicaFiles(replica);

      //update replica
      final ReplicaInfo finalized = updateReplicaUnderRecovery(oldBlock
          .getBlockPoolId(), replica, recoveryId,
          newBlockId, newlength);

      boolean copyTruncate = newBlockId != oldBlock.getBlockId();
      if (!copyTruncate) {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == recoveryId
            && finalized.getNumBytes() == newlength
            : "Replica information mismatched: oldBlock=" + oldBlock
            + ", recoveryId=" + recoveryId + ", newlength=" + newlength
            + ", newBlockId=" + newBlockId + ", finalized=" + finalized;
      } else {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == oldBlock.getGenerationStamp()
            && finalized.getNumBytes() == oldBlock.getNumBytes()
            : "Finalized and old information mismatched: oldBlock=" + oldBlock
            + ", genStamp=" + oldBlock.getGenerationStamp()
            + ", len=" + oldBlock.getNumBytes()
            + ", finalized=" + finalized;
      }
      //check replica files after update
      checkReplicaFiles(finalized);

      return finalized;
    }
  }

  private ReplicaInfo updateReplicaUnderRecovery(
                                          String bpid,
                                          ReplicaInfo rur,
                                          long recoveryId,
                                          long newBlockId,
                                          long newlength) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId
          + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    // bump rur's GS to be recovery id
    if(!copyOnTruncate) {
      rur.bumpReplicaGS(recoveryId);
    }

    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }

    if (rur.getNumBytes() > newlength) {
      if(!copyOnTruncate) {
        rur.breakHardLinksIfNeeded();
        rur.truncateBlock(newlength);
        // update RUR with the new length
        rur.setNumBytes(newlength);
      } else {
        // Copying block to a new block with new blockId.
        // Not truncating original block.
        FsVolumeImpl volume = (FsVolumeImpl) rur.getVolume();
        ReplicaInPipeline newReplicaInfo = volume.updateRURCopyOnTruncate(
            rur, bpid, newBlockId, recoveryId, newlength);
        if (newReplicaInfo.getState() != ReplicaState.RBW) {
          throw new IOException("Append on block " + rur.getBlockId()
              + " returned a replica of state " + newReplicaInfo.getState()
              + "; expected RBW");
        }

        newReplicaInfo.setNumBytes(newlength);
        volumeMap.add(bpid, newReplicaInfo.getReplicaInfo());
        finalizeReplica(bpid, newReplicaInfo.getReplicaInfo());
      }
    }
    // finalize the block
    return finalizeReplica(bpid, rur);
  }

  @Override // FsDatasetSpi
  public long getReplicaVisibleLength(final ExtendedBlock block)
  throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica replica = getReplicaInfo(block.getBlockPoolId(),
          block.getBlockId());
      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException(
            "replica.getGenerationStamp() < block.getGenerationStamp(), block="
                + block + ", replica=" + replica);
      }
      return replica.getVisibleLength();
    }
  }
  
  @Override
  public void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    LOG.info("Adding block pool " + bpid);
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      volumes.addBlockPool(bpid, conf);
      volumeMap.initBlockPool(bpid);
    }
    volumes.getAllVolumesMap(bpid, volumeMap, ramDiskReplicaTracker);
  }

  @Override
  public void shutdownBlockPool(String bpid) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      LOG.info("Removing block pool " + bpid);
      Map<DatanodeStorage, BlockListAsLongs> blocksPerVolume
          = getBlockReports(bpid);
      volumeMap.cleanUpBlockPool(bpid);
      volumes.removeBlockPool(bpid, blocksPerVolume);
    }
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS
    final long reservedSpaceForReplicas; // size of space reserved RBW or
                                    // re-replication
    final long numBlocks;

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
      this.reservedSpaceForReplicas = v.getReservedForReplicas();
      this.numBlocks = v.getNumBlocks();
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FsVolumeImpl volume : volumes.getVolumes()) {
      long used = 0;
      long free = 0;
      try (FsVolumeReference ref = volume.obtainReference()) {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
      } catch (ClosedChannelException e) {
        continue;
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        used = 0;
        free = 0;
      }
      
      info.add(new VolumeInfo(volume, used, free));
    }
    return info;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      innerInfo.put("reservedSpaceForReplicas", v.reservedSpaceForReplicas);
      innerInfo.put("numBlocks", v.numBlocks);
      info.put(v.directory, innerInfo);
    }
    return info;
  }

  @Override //FsDatasetSpi
  public void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      List<FsVolumeImpl> curVolumes = volumes.getVolumes();
      if (!force) {
        for (FsVolumeImpl volume : curVolumes) {
          try (FsVolumeReference ref = volume.obtainReference()) {
            if (!volume.isBPDirEmpty(bpid)) {
              LOG.warn(bpid
                  + " has some block files, cannot delete unless forced");
              throw new IOException("Cannot delete block pool, "
                  + "it contains some block files");
            }
          } catch (ClosedChannelException e) {
            // ignore.
          }
        }
      }
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
          volume.deleteBPDirectories(bpid, force);
        } catch (ClosedChannelException e) {
          // ignore.
        }
      }
    }
  }
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica replica = volumeMap.get(block.getBlockPoolId(),
          block.getBlockId());
      if (replica == null) {
        throw new ReplicaNotFoundException(block);
      }
      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException(
            "Replica generation stamp < block generation stamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        block.setGenerationStamp(replica.getGenerationStamp());
      }
    }

    ReplicaInfo r = getBlockReplica(block);
    File blockFile = new File(r.getBlockURI());
    File metaFile = new File(r.getMetadataURI());
    BlockLocalPathInfo info = new BlockLocalPathInfo(block,
        blockFile.getAbsolutePath(), metaFile.toString());
    return info;
  }

  @Override
  public void enableTrash(String bpid) {
    dataStorage.enableTrash(bpid);
  }

  @Override
  public void clearTrash(String bpid) {
    dataStorage.clearTrash(bpid);
  }

  @Override
  public boolean trashEnabled(String bpid) {
    return dataStorage.trashEnabled(bpid);
  }

  @Override
  public void setRollingUpgradeMarker(String bpid) throws IOException {
    dataStorage.setRollingUpgradeMarker(bpid);
  }

  @Override
  public void clearRollingUpgradeMarker(String bpid) throws IOException {
    dataStorage.clearRollingUpgradeMarker(bpid);
  }


  @Override
  public void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, FsVolumeImpl targetVolume) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ramDiskReplicaTracker.recordEndLazyPersist(bpId, blockId, savedFiles);

      targetVolume.incDfsUsedAndNumBlocks(bpId, savedFiles[0].length()
          + savedFiles[1].length());

      // Update metrics (ignore the metadata file size)
      datanode.getMetrics().incrRamDiskBlocksLazyPersisted();
      datanode.getMetrics().incrRamDiskBytesLazyPersisted(savedFiles[1].length());
      datanode.getMetrics().addRamDiskBlocksLazyPersistWindowMs(
          Time.monotonicNow() - creationTime);

      if (LOG.isDebugEnabled()) {
        LOG.debug("LazyWriter: Finish persisting RamDisk block: "
            + " block pool Id: " + bpId + " block id: " + blockId
            + " to block file " + savedFiles[1] + " and meta file " + savedFiles[0]
            + " on target volume " + targetVolume);
      }
    }
  }

  @Override
  public void onFailLazyPersist(String bpId, long blockId) {
    RamDiskReplica block = null;
    block = ramDiskReplicaTracker.getReplica(bpId, blockId);
    if (block != null) {
      LOG.warn("Failed to save replica " + block + ". re-enqueueing it.");
      ramDiskReplicaTracker.reenqueueReplicaNotPersisted(block);
    }
  }

  @Override
  public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
      FileDescriptor fd, long offset, long nbytes, int flags) {
    FsVolumeImpl fsVolumeImpl = this.getVolume(block);
    asyncDiskService.submitSyncFileRangeRequest(fsVolumeImpl, fd, offset,
        nbytes, flags);
  }

  private boolean ramDiskConfigured() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      if (v.isTransientStorage()) {
        return true;
      }
    }
    return false;
  }

  // Add/Remove per DISK volume async lazy persist thread when RamDisk volume is
  // added or removed.
  // This should only be called when the FsDataSetImpl#volumes list is finalized.
  private void setupAsyncLazyPersistThreads() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      setupAsyncLazyPersistThread(v);
    }
  }

  private void setupAsyncLazyPersistThread(final FsVolumeImpl v) {
    // Skip transient volumes
    if (v.isTransientStorage()) {
      return;
    }
    boolean ramDiskConfigured = ramDiskConfigured();
    // Add thread for DISK volume if RamDisk is configured
    if (ramDiskConfigured &&
        asyncLazyPersistService != null &&
        !asyncLazyPersistService.queryVolume(v)) {
      asyncLazyPersistService.addVolume(v);
    }

    // Remove thread for DISK volume if RamDisk is not configured
    if (!ramDiskConfigured &&
        asyncLazyPersistService != null &&
        asyncLazyPersistService.queryVolume(v)) {
      asyncLazyPersistService.removeVolume(v);
    }
  }

  private void removeOldReplica(ReplicaInfo replicaInfo,
      ReplicaInfo newReplicaInfo, final String bpid) {
    // Before deleting the files from old storage we must notify the
    // NN that the files are on the new storage. Else a blockReport from
    // the transient storage might cause the NN to think the blocks are lost.
    // Replicas must be evicted from client short-circuit caches, because the
    // storage will no longer be same, and thus will require validating
    // checksum.  This also stops a client from holding file descriptors,
    // which would prevent the OS from reclaiming the memory.
    ExtendedBlock extendedBlock =
        new ExtendedBlock(bpid, newReplicaInfo);
    datanode.getShortCircuitRegistry().processBlockInvalidation(
        ExtendedBlockId.fromExtendedBlock(extendedBlock));
    datanode.notifyNamenodeReceivedBlock(
        extendedBlock, null, newReplicaInfo.getStorageUuid(),
        newReplicaInfo.isOnTransientStorage());

    // Remove the old replicas
    if (replicaInfo.deleteBlockData() || !replicaInfo.blockDataExists()) {
      FsVolumeImpl volume = (FsVolumeImpl) replicaInfo.getVolume();
      volume.onBlockFileDeletion(bpid, replicaInfo.getBytesOnDisk());
      if (replicaInfo.deleteMetadata() || !replicaInfo.metadataExists()) {
        volume.onMetaFileDeletion(bpid, replicaInfo.getMetadataLength());
      }
    }

    // If deletion failed then the directory scanner will cleanup the blocks
    // eventually.
  }

  class LazyWriter implements Runnable {
    private volatile boolean shouldRun = true;
    final int checkpointerInterval;

    public LazyWriter(Configuration conf) {
      this.checkpointerInterval = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC);
    }

    /**
     * Checkpoint a pending replica to persistent storage now.
     * If we fail then move the replica to the end of the queue.
     * @return true if there is more work to be done, false otherwise.
     */
    private boolean saveNextReplica() {
      RamDiskReplica block = null;
      FsVolumeReference targetReference;
      FsVolumeImpl targetVolume;
      ReplicaInfo replicaInfo;
      boolean succeeded = false;

      try {
        block = ramDiskReplicaTracker.dequeueNextReplicaToPersist();
        if (block != null) {
          try (AutoCloseableLock lock = datasetLock.acquire()) {
            replicaInfo = volumeMap.get(block.getBlockPoolId(), block.getBlockId());

            // If replicaInfo is null, the block was either deleted before
            // it could be checkpointed or it is already on persistent storage.
            // This can occur if a second replica on persistent storage was found
            // after the lazy write was scheduled.
            if (replicaInfo != null &&
                replicaInfo.getVolume().isTransientStorage()) {
              // Pick a target volume to persist the block.
              targetReference = volumes.getNextVolume(
                  StorageType.DEFAULT, replicaInfo.getNumBytes());
              targetVolume = (FsVolumeImpl) targetReference.getVolume();

              ramDiskReplicaTracker.recordStartLazyPersist(
                  block.getBlockPoolId(), block.getBlockId(), targetVolume);

              if (LOG.isDebugEnabled()) {
                LOG.debug("LazyWriter: Start persisting RamDisk block:"
                    + " block pool Id: " + block.getBlockPoolId()
                    + " block id: " + block.getBlockId()
                    + " on target volume " + targetVolume);
              }

              asyncLazyPersistService.submitLazyPersistTask(
                  block.getBlockPoolId(), block.getBlockId(),
                  replicaInfo.getGenerationStamp(), block.getCreationTime(),
                  replicaInfo, targetReference);
            }
          }
        }
        succeeded = true;
      } catch(IOException ioe) {
        LOG.warn("Exception saving replica " + block, ioe);
      } finally {
        if (!succeeded && block != null) {
          LOG.warn("Failed to save replica " + block + ". re-enqueueing it.");
          onFailLazyPersist(block.getBlockPoolId(), block.getBlockId());
        }
      }
      return succeeded;
    }

    /**
     * Attempt to evict one or more transient block replicas until we
     * have at least bytesNeeded bytes free.
     */
    public void evictBlocks(long bytesNeeded) throws IOException {
      int iterations = 0;

      final long cacheCapacity = cacheManager.getCacheCapacity();

      while (iterations++ < MAX_BLOCK_EVICTIONS_PER_ITERATION &&
             (cacheCapacity - cacheManager.getCacheUsed()) < bytesNeeded) {
        RamDiskReplica replicaState = ramDiskReplicaTracker.getNextCandidateForEviction();

        if (replicaState == null) {
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Evicting block " + replicaState);
        }

        ReplicaInfo replicaInfo, newReplicaInfo;
        final String bpid = replicaState.getBlockPoolId();

        try (AutoCloseableLock lock = datasetLock.acquire()) {
          replicaInfo = getReplicaInfo(replicaState.getBlockPoolId(),
                                       replicaState.getBlockId());
          Preconditions.checkState(replicaInfo.getVolume().isTransientStorage());
          ramDiskReplicaTracker.discardReplica(replicaState.getBlockPoolId(),
              replicaState.getBlockId(), false);

          // Move the replica from lazyPersist/ to finalized/ on
          // the target volume
          newReplicaInfo =
              replicaState.getLazyPersistVolume().activateSavedReplica(bpid,
                  replicaInfo, replicaState);

          // Update the volumeMap entry.
          volumeMap.add(bpid, newReplicaInfo);

          // Update metrics
          datanode.getMetrics().incrRamDiskBlocksEvicted();
          datanode.getMetrics().addRamDiskBlocksEvictionWindowMs(
              Time.monotonicNow() - replicaState.getCreationTime());
          if (replicaState.getNumReads() == 0) {
            datanode.getMetrics().incrRamDiskBlocksEvictedWithoutRead();
          }

          // Delete the block+meta files from RAM disk and release locked
          // memory.
          removeOldReplica(replicaInfo, newReplicaInfo, bpid);
        }
      }
    }

    @Override
    public void run() {
      int numSuccessiveFailures = 0;

      while (fsRunning && shouldRun) {
        try {
          numSuccessiveFailures = saveNextReplica() ? 0 : (numSuccessiveFailures + 1);

          // Sleep if we have no more work to do or if it looks like we are not
          // making any forward progress. This is to ensure that if all persist
          // operations are failing we don't keep retrying them in a tight loop.
          if (numSuccessiveFailures >= ramDiskReplicaTracker.numReplicasNotPersisted()) {
            Thread.sleep(checkpointerInterval * 1000);
            numSuccessiveFailures = 0;
          }
        } catch (InterruptedException e) {
          LOG.info("LazyWriter was interrupted, exiting");
          break;
        } catch (Exception e) {
          LOG.warn("Ignoring exception in LazyWriter:", e);
        }
      }
    }

    public void stop() {
      shouldRun = false;
    }
  }
  
  @Override
  public void setPinning(ExtendedBlock block) throws IOException {
    if (!blockPinningEnabled) {
      return;
    }
    
    ReplicaInfo r = getBlockReplica(block);
    r.setPinning(localFS);
  }
  
  @Override
  public boolean getPinning(ExtendedBlock block) throws IOException {
    if (!blockPinningEnabled) {
      return  false;
    }
    ReplicaInfo r = getBlockReplica(block);
    return r.getPinning(localFS);
  }
  
  @Override
  public boolean isDeletingBlock(String bpid, long blockId) {
    synchronized(deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      return s != null ? s.contains(blockId) : false;
    }
  }
  
  public void removeDeletedBlocks(String bpid, Set<Long> blockIds) {
    synchronized (deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      if (s != null) {
        for (Long id : blockIds) {
          s.remove(id);
        }
      }
    }
  }
  
  private void addDeletingBlock(String bpid, Long blockId) {
    synchronized(deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      if (s == null) {
        s = new HashSet<Long>();
        deletingBlock.put(bpid, s);
      }
      s.add(blockId);
    }
  }

  void releaseLockedMemory(long count, boolean roundup) {
    if (roundup) {
      cacheManager.release(count);
    } else {
      cacheManager.releaseRoundDown(count);
    }
  }

  /**
   * Attempt to evict blocks from cache Manager to free the requested
   * bytes.
   *
   * @param bytesNeeded
   */
  @VisibleForTesting
  public void evictLazyPersistBlocks(long bytesNeeded) {
    try {
      ((LazyWriter) lazyWriter.getRunnable()).evictBlocks(bytesNeeded);
    } catch(IOException ioe) {
      LOG.info("Ignoring exception ", ioe);
    }
  }

  /**
   * Attempt to reserve the given amount of memory with the cache Manager.
   * @param bytesNeeded
   * @return
   */
  boolean reserveLockedMemory(long bytesNeeded) {
    if (cacheManager.reserve(bytesNeeded) > 0) {
      return true;
    }

    // Round up bytes needed to osPageSize and attempt to evict
    // one more more blocks to free up the reservation.
    bytesNeeded = cacheManager.roundUpPageSize(bytesNeeded);
    evictLazyPersistBlocks(bytesNeeded);
    return cacheManager.reserve(bytesNeeded) > 0;
  }

  @VisibleForTesting
  public void setTimer(Timer newTimer) {
    this.timer = newTimer;
  }

  void stopAllDataxceiverThreads(FsVolumeImpl volume) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for (String blockPoolId : volumeMap.getBlockPoolList()) {
        Collection<ReplicaInfo> replicas = volumeMap.replicas(blockPoolId);
        for (ReplicaInfo replicaInfo : replicas) {
          if ((replicaInfo.getState() == ReplicaState.TEMPORARY
              || replicaInfo.getState() == ReplicaState.RBW)
              && replicaInfo.getVolume().equals(volume)) {
            ReplicaInPipeline replicaInPipeline =
                (ReplicaInPipeline) replicaInfo;
            replicaInPipeline.interruptThread();
          }
        }
      }
    }
  }
}

