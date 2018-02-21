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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
import java.util.*;
import java.util.concurrent.Executor;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
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
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

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
  static {
    isNativeIOAvailable = NativeIO.isAvailable();
    if (Path.WINDOWS && !isNativeIOAvailable) {
      LOG.warn("Data node cannot fully support concurrent reading"
          + " and writing without native code extensions on Windows.");
    }
  }

  @Override // FsDatasetSpi
  public List<FsVolumeImpl> getVolumes() {
    return volumes.getVolumes();
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
      List<FsVolumeImpl> curVolumes = getVolumes();
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
  public synchronized FsVolumeImpl getVolume(final ExtendedBlock b) {
    final ReplicaInfo r =  volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    return r != null? (FsVolumeImpl)r.getVolume(): null;
  }

  @Override // FsDatasetSpi
  public synchronized Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    File blockfile = getFile(bpid, blkid, false);
    if (blockfile == null) {
      return null;
    }
    final File metafile = FsDatasetUtil.findMetaFile(blockfile);
    final long gs = FsDatasetUtil.parseGenerationStamp(blockfile, metafile);
    return new Block(blkid, blockfile.length(), gs);
  }


  /**
   * This should be primarily used for testing.
   * @return clone of replica store in datanode memory
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
    if(r == null)
      return null;
    switch(r.getState()) {
    case FINALIZED:
      return new FinalizedReplica((FinalizedReplica)r);
    case RBW:
      return new ReplicaBeingWritten((ReplicaBeingWritten)r);
    case RWR:
      return new ReplicaWaitingToBeRecovered((ReplicaWaitingToBeRecovered)r);
    case RUR:
      return new ReplicaUnderRecovery((ReplicaUnderRecovery)r);
    case TEMPORARY:
      return new ReplicaInPipeline((ReplicaInPipeline)r);
    }
    return null;
  }
  
  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    File meta = FsDatasetUtil.getMetaFile(getBlockFile(b), b.getGenerationStamp());
    if (meta == null || !meta.exists()) {
      return null;
    }
    if (isNativeIOAvailable) {
      return new LengthInputStream(
          NativeIO.getShareDeleteFileInputStream(meta),
          meta.length());
    }
    return new LengthInputStream(new FileInputStream(meta), meta.length());
  }
    
  final DataNode datanode;
  final DataStorage dataStorage;
  final FsVolumeList volumes;
  final Map<String, DatanodeStorage> storageMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  final Daemon lazyWriter;
  final FsDatasetCache cacheManager;
  private final Configuration conf;
  private final int validVolsRequired;
  private volatile boolean fsRunning;

  final ReplicaMap volumeMap;
  final Map<String, Set<Long>> deletingBlock;
  final RamDiskReplicaTracker ramDiskReplicaTracker;
  final RamDiskAsyncLazyPersistService asyncLazyPersistService;

  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;


  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

  final LocalFileSystem localFS;

  private boolean blockPinningEnabled;
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {
    this.fsRunning = true;
    this.datanode = datanode;
    this.dataStorage = storage;
    this.conf = conf;
    // The number of volumes required for operation is the total number 
    // of volumes minus the number of failed volumes we can tolerate.
    final int volFailuresTolerated =
      conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
                  DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    String[] dataDirs = conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    Collection<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);
    List<VolumeFailureInfo> volumeFailureInfos = getInitialVolumeFailureInfos(
        dataLocations, storage);

    int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
    int volsFailed = volumeFailureInfos.size();
    this.validVolsRequired = volsConfigured - volFailuresTolerated;

    if (volFailuresTolerated < 0 || volFailuresTolerated >= volsConfigured) {
      throw new DiskErrorException("Invalid volume failure "
          + " config value: " + volFailuresTolerated);
    }
    if (volsFailed > volFailuresTolerated) {
      throw new DiskErrorException("Too many failed volumes - "
          + "current valid volumes: " + storage.getNumStorageDirs() 
          + ", volumes configured: " + volsConfigured 
          + ", volumes failed: " + volsFailed
          + ", volume failures tolerated: " + volFailuresTolerated);
    }

    storageMap = new ConcurrentHashMap<String, DatanodeStorage>();
    volumeMap = new ReplicaMap(this);
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
    asyncLazyPersistService = new RamDiskAsyncLazyPersistService(datanode);
    deletingBlock = new HashMap<String, Set<Long>>();

    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      addVolume(dataLocations, storage.getStorageDir(idx));
    }
    setupAsyncLazyPersistThreads();

    cacheManager = new FsDatasetCache(this);

    // Start the lazy writer once we have built the replica maps.
    lazyWriter = new Daemon(new LazyWriter(conf));
    lazyWriter.start();
    registerMBean(datanode.getDatanodeUuid());
    localFS = FileSystem.getLocal(conf);
    blockPinningEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED,
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT);
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
    Set<String> failedLocationSet = Sets.newHashSetWithExpectedSize(
        dataLocations.size());
    for (StorageLocation sl: dataLocations) {
      failedLocationSet.add(sl.getFile().getAbsolutePath());
    }
    for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
         it.hasNext(); ) {
      Storage.StorageDirectory sd = it.next();
      failedLocationSet.remove(sd.getRoot().getAbsolutePath());
    }
    List<VolumeFailureInfo> volumeFailureInfos = Lists.newArrayListWithCapacity(
        failedLocationSet.size());
    long failureDate = Time.now();
    for (String failedStorageLocation: failedLocationSet) {
      volumeFailureInfos.add(new VolumeFailureInfo(failedStorageLocation,
          failureDate));
    }
    return volumeFailureInfos;
  }

  private void addVolume(Collection<StorageLocation> dataLocations,
      Storage.StorageDirectory sd) throws IOException {
    final File dir = sd.getCurrentDir();
    final StorageType storageType =
        getStorageTypeFromLocations(dataLocations, sd.getRoot());

    // If IOException raises from FsVolumeImpl() or getVolumeMap(), there is
    // nothing needed to be rolled back to make various data structures, e.g.,
    // storageMap and asyncDiskService, consistent.
    FsVolumeImpl fsVolume = new FsVolumeImpl(
        this, sd.getStorageUuid(), dir, this.conf, storageType);
    FsVolumeReference ref = fsVolume.obtainReference();
    ReplicaMap tempVolumeMap = new ReplicaMap(this);
    fsVolume.getVolumeMap(tempVolumeMap, ramDiskReplicaTracker);

    synchronized (this) {
      volumeMap.addAll(tempVolumeMap);
      storageMap.put(sd.getStorageUuid(),
          new DatanodeStorage(sd.getStorageUuid(),
              DatanodeStorage.State.NORMAL,
              storageType));
      asyncDiskService.addVolume(sd.getCurrentDir());
      volumes.addVolume(ref);
    }

    LOG.info("Added volume - " + dir + ", StorageType: " + storageType);
  }

  @VisibleForTesting
  public FsVolumeImpl createFsVolume(String storageUuid, File currentDir,
      StorageType storageType) throws IOException {
    return new FsVolumeImpl(this, storageUuid, currentDir, conf, storageType);
  }

  @Override
  public void addVolume(final StorageLocation location,
      final List<NamespaceInfo> nsInfos)
      throws IOException {
    final File dir = location.getFile();

    // Prepare volume in DataStorage
    final DataStorage.VolumeBuilder builder;
    try {
      builder = dataStorage.prepareVolume(datanode, location.getFile(), nsInfos);
    } catch (IOException e) {
      volumes.addVolumeFailureInfo(new VolumeFailureInfo(
          location.getFile().getAbsolutePath(), Time.now()));
      throw e;
    }

    final Storage.StorageDirectory sd = builder.getStorageDirectory();

    StorageType storageType = location.getStorageType();
    final FsVolumeImpl fsVolume =
        createFsVolume(sd.getStorageUuid(), sd.getCurrentDir(), storageType);
    final ReplicaMap tempVolumeMap = new ReplicaMap(fsVolume);
    ArrayList<IOException> exceptions = Lists.newArrayList();

    for (final NamespaceInfo nsInfo : nsInfos) {
      String bpid = nsInfo.getBlockPoolID();
      try {
        fsVolume.addBlockPool(bpid, this.conf);
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
    synchronized (this) {
      volumeMap.addAll(tempVolumeMap);
      storageMap.put(sd.getStorageUuid(),
          new DatanodeStorage(sd.getStorageUuid(),
              DatanodeStorage.State.NORMAL,
              storageType));
      asyncDiskService.addVolume(sd.getCurrentDir());
      volumes.addVolume(ref);
    }
    LOG.info("Added volume - " + dir + ", StorageType: " + storageType);
  }

  /**
   * Removes a set of volumes from FsDataset.
   * @param volumesToRemove a set of absolute root path of each volume.
   * @param clearFailure set true to clear failure information.
   */
  @Override
  public void removeVolumes(Set<File> volumesToRemove, boolean clearFailure) {
    // Make sure that all volumes are absolute path.
    for (File vol : volumesToRemove) {
      Preconditions.checkArgument(vol.isAbsolute(),
          String.format("%s is not absolute path.", vol.getPath()));
    }

    Map<String, List<ReplicaInfo>> blkToInvalidate = new HashMap<>();
    List<String> storageToRemove = new ArrayList<>();
    synchronized (this) {
      for (int idx = 0; idx < dataStorage.getNumStorageDirs(); idx++) {
        Storage.StorageDirectory sd = dataStorage.getStorageDir(idx);
        final File absRoot = sd.getRoot().getAbsoluteFile();
        if (volumesToRemove.contains(absRoot)) {
          LOG.info("Removing " + absRoot + " from FsDataset.");

          // Disable the volume from the service.
          asyncDiskService.removeVolume(sd.getCurrentDir());
          volumes.removeVolume(absRoot, clearFailure);

          // Removed all replica information for the blocks on the volume.
          // Unlike updating the volumeMap in addVolume(), this operation does
          // not scan disks.
          for (String bpid : volumeMap.getBlockPoolList()) {
            List<ReplicaInfo> blocks = new ArrayList<>();
            for (Iterator<ReplicaInfo> it = volumeMap.replicas(bpid).iterator();
                 it.hasNext(); ) {
              ReplicaInfo block = it.next();
              final File absBasePath =
                  new File(block.getVolume().getBasePath()).getAbsoluteFile();
              if (absBasePath.equals(absRoot)) {
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

    synchronized (this) {
      for(String storageUuid : storageToRemove) {
        storageMap.remove(storageUuid);
      }
    }
  }

  private StorageType getStorageTypeFromLocations(
      Collection<StorageLocation> dataLocations, File dir) {
    for (StorageLocation dataLocation : dataLocations) {
      if (dataLocation.getFile().equals(dir)) {
        return dataLocation.getStorageType();
      }
    }
    return StorageType.DEFAULT;
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
    return getVolumes().size() >= validVolsRequired; 
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() {
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
      failedStorageLocations.add(info.getFailedStorageLocation());
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
      failedStorageLocations.add(info.getFailedStorageLocation());
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

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return cacheManager.getNumBlocksCached();
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FsDatasetSpi
  public long getLength(ExtendedBlock b) throws IOException {
    return getBlockFile(b).length();
  }

  /**
   * Get File name for a given block.
   */
  private File getBlockFile(ExtendedBlock b) throws IOException {
    return getBlockFile(b.getBlockPoolId(), b.getBlockId());
  }
  
  /**
   * Get File name for a given block.
   */
  File getBlockFile(String bpid, long blockId) throws IOException {
    File f = validateBlockFile(bpid, blockId);
    if(f == null) {
      throw new IOException("BlockId " + blockId + " is not valid.");
    }
    return f;
  }
  
  /**
   * Return the File associated with a block, without first
   * checking that it exists. This should be used when the
   * next operation is going to open the file for read anyway,
   * and thus the exists check is redundant.
   *
   * @param touch if true then update the last access timestamp of the
   *              block. Currently used for blocks on transient storage.
   */
  private File getBlockFileNoExistsCheck(ExtendedBlock b,
                                         boolean touch)
      throws IOException {
    final File f;
    synchronized(this) {
      f = getFile(b.getBlockPoolId(), b.getLocalBlock().getBlockId(), touch);
    }
    if (f == null) {
      throw new IOException("Block " + b + " is not valid");
    }
    return f;
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    File blockFile = getBlockFileNoExistsCheck(b, true);
    if (isNativeIOAvailable) {
      return NativeIO.getShareDeleteFileInputStream(blockFile, seekOffset);
    } else {
      try {
        return openAndSeek(blockFile, seekOffset);
      } catch (FileNotFoundException fnfe) {
        throw new IOException("Block " + b + " is not valid. " +
            "Expected block file at " + blockFile + " does not exist.");
      }
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
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
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
  private ReplicaInfo getReplicaInfo(String bpid, long blkid)
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
  public synchronized ReplicaInputStreams getTmpInputStreams(ExtendedBlock b,
      long blkOffset, long metaOffset) throws IOException {
    ReplicaInfo info = getReplicaInfo(b);
    FsVolumeReference ref = info.getVolume().obtainReference();
    try {
      InputStream blockInStream = openAndSeek(info.getBlockFile(), blkOffset);
      try {
        InputStream metaInStream = openAndSeek(info.getMetaFile(), metaOffset);
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

  private static FileInputStream openAndSeek(File file, long offset)
      throws IOException {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(file, "r");
      if (offset > 0) {
        raf.seek(offset);
      }
      return new FileInputStream(raf.getFD());
    } catch(IOException ioe) {
      IOUtils.cleanup(null, raf);
      throw ioe;
    }
  }

  static File moveBlockFiles(Block b, File srcfile, File destdir)
      throws IOException {
    final File dstfile = new File(destdir, b.getBlockName());
    final File srcmeta = FsDatasetUtil.getMetaFile(srcfile, b.getGenerationStamp());
    final File dstmeta = FsDatasetUtil.getMetaFile(dstfile, b.getGenerationStamp());
    try {
      NativeIO.renameTo(srcmeta, dstmeta);
    } catch (IOException e) {
      throw new IOException("Failed to move meta file for " + b
          + " from " + srcmeta + " to " + dstmeta, e);
    }
    try {
      NativeIO.renameTo(srcfile, dstfile);
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + b
          + " from " + srcfile + " to " + dstfile.getAbsolutePath(), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("addFinalizedBlock: Moved " + srcmeta + " to " + dstmeta
          + " and " + srcfile + " to " + dstfile);
    }
    return dstfile;
  }

  private void fsyncDirectory(File... dirs)
      throws IOException {
    for (File dir : dirs) {
      try {
        IOUtils.fsync(dir);
      } catch (IOException e) {
        throw new IOException("Failed to sync " + dir, e);
      }
    }
  }

  /**
   * Copy the block and meta files for the given block to the given destination.
   * @return the new meta and block files.
   * @throws IOException
   */
  static File[] copyBlockFiles(long blockId, long genStamp, File srcMeta,
      File srcFile, File destRoot, boolean calculateChecksum)
      throws IOException {
    final File destDir = DatanodeUtil.idToBlockDir(destRoot, blockId);
    final File dstFile = new File(destDir, srcFile.getName());
    final File dstMeta = FsDatasetUtil.getMetaFile(dstFile, genStamp);
    return copyBlockFiles(srcMeta, srcFile, dstMeta, dstFile, calculateChecksum);
  }

  static File[] copyBlockFiles(File srcMeta, File srcFile, File dstMeta,
                               File dstFile, boolean calculateChecksum)
      throws IOException {
    if (calculateChecksum) {
      computeChecksum(srcMeta, dstMeta, srcFile);
    } else {
      try {
        Storage.nativeCopyFileUnbuffered(srcMeta, dstMeta, true);
      } catch (IOException e) {
        throw new IOException("Failed to copy " + srcMeta + " to " + dstMeta, e);
      }
    }

    try {
      Storage.nativeCopyFileUnbuffered(srcFile, dstFile, true);
    } catch (IOException e) {
      throw new IOException("Failed to copy " + srcFile + " to " + dstFile, e);
    }
    if (LOG.isDebugEnabled()) {
      if (calculateChecksum) {
        LOG.debug("Copied " + srcMeta + " to " + dstMeta
            + " and calculated checksum");
      } else {
        LOG.debug("Copied " + srcFile + " to " + dstFile);
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
    synchronized (this) {
      volumeRef = volumes.getNextVolume(targetStorageType, block.getNumBytes());
    }
    try {
      File oldBlockFile = replicaInfo.getBlockFile();
      File oldMetaFile = replicaInfo.getMetaFile();
      FsVolumeImpl targetVolume = (FsVolumeImpl) volumeRef.getVolume();
      // Copy files to temp dir first
      File[] blockFiles = copyBlockFiles(block.getBlockId(),
          block.getGenerationStamp(), oldMetaFile, oldBlockFile,
          targetVolume.getTmpDir(block.getBlockPoolId()),
          replicaInfo.isOnTransientStorage());

      ReplicaInfo newReplicaInfo = new ReplicaInPipeline(
          replicaInfo.getBlockId(), replicaInfo.getGenerationStamp(),
          targetVolume, blockFiles[0].getParentFile(), 0);
      newReplicaInfo.setNumBytes(blockFiles[1].length());
      // Finalize the copied files
      newReplicaInfo = finalizeReplica(block.getBlockPoolId(), newReplicaInfo);

      removeOldReplica(replicaInfo, newReplicaInfo, oldBlockFile, oldMetaFile,
          oldBlockFile.length(), oldMetaFile.length(), block.getBlockPoolId());
    } finally {
      if (volumeRef != null) {
        volumeRef.close();
      }
    }

    // Replace the old block if any to reschedule the scanning.
    return replicaInfo;
  }

  /**
   * Compute and store the checksum for a block file that does not already have
   * its checksum computed.
   *
   * @param srcMeta source meta file, containing only the checksum header, not a
   *     calculated checksum
   * @param dstMeta destination meta file, into which this method will write a
   *     full computed checksum
   * @param blockFile block file for which the checksum will be computed
   * @throws IOException
   */
  static void computeChecksum(File srcMeta, File dstMeta, File blockFile)
      throws IOException {
    final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(srcMeta);
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
          new FileOutputStream(dstMeta), HdfsConstants.SMALL_BUFFER_SIZE));
      BlockMetadataHeader.writeHeader(metaOut, checksum);

      int offset = 0;
      try (InputStream dataIn = isNativeIOAvailable ?
          NativeIO.getShareDeleteFileInputStream(blockFile) :
          new FileInputStream(blockFile)) {

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
      metaOut.close();
      metaOut = null;
    } finally {
      IOUtils.closeStream(metaOut);
    }
  }

  static private void truncateBlock(File blockFile, File metaFile,
      long oldlen, long newlen) throws IOException {
    LOG.info("truncateBlock: blockFile=" + blockFile
        + ", metaFile=" + metaFile
        + ", oldlen=" + oldlen
        + ", newlen=" + newlen);

    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen
          + ") to newlen (=" + newlen + ")");
    }

    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum(); 
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1)/bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n*checksumsize;
    long lastchunkoffset = (n - 1)*bpc;
    int lastchunksize = (int)(newlen - lastchunkoffset); 
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)]; 

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile 
      blockRAF.setLength(newlen);
 
      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile 
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }


  @Override  // FsDatasetSpi
  public synchronized ReplicaHandler append(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
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
    ReplicaBeingWritten replica = null;
    try {
      replica = append(b.getBlockPoolId(), (FinalizedReplica)replicaInfo, newGS,
          b.getNumBytes());
    } catch (IOException e) {
      IOUtils.cleanup(null, ref);
      throw e;
    }
    return new ReplicaHandler(replica, ref);
  }

  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param bpid block pool Id
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate generation stamp
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private synchronized ReplicaBeingWritten append(String bpid,
      FinalizedReplica replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {
    // If the block is cached, start uncaching it.
    cacheManager.uncacheBlock(bpid, replicaInfo.getBlockId());
    // unlink the finalized replica
    replicaInfo.unlinkBlock(1);
    
    // construct a RBW replica with the new GS
    File blkfile = replicaInfo.getBlockFile();
    FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
    long bytesReserved = estimateBlockLen - replicaInfo.getNumBytes();
    if (v.getAvailable() < bytesReserved) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }
    File newBlkFile = new File(v.getRbwDir(bpid), replicaInfo.getBlockName());
    File oldmeta = replicaInfo.getMetaFile();
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
        replicaInfo.getBlockId(), replicaInfo.getNumBytes(), newGS,
        v, newBlkFile.getParentFile(), Thread.currentThread(), bytesReserved);

    // load last checksum and datalen
    newReplicaInfo.setLastChecksumAndDataLen(
        replicaInfo.getNumBytes(), replicaInfo.getLastPartialChunkChecksum());

    File newmeta = newReplicaInfo.getMetaFile();

    // rename meta file to rbw directory
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      NativeIO.renameTo(oldmeta, newmeta);
    } catch (IOException e) {
      throw new IOException("Block " + replicaInfo + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to rbw dir " + newmeta, e);
    }

    // rename block file to rbw directory
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
      throw new IOException("Block " + replicaInfo + " reopen failed. " +
                              " Unable to move block file " + blkfile +
                              " to rbw dir " + newBlkFile, e);
    }
    
    // Replace finalized replica by a RBW replica in replicas map
    volumeMap.add(bpid, newReplicaInfo);
    v.reserveSpaceForRbw(bytesReserved);
    return newReplicaInfo;
  }

  private ReplicaInfo recoverCheck(ExtendedBlock b, long newGS, 
      long expectedBlockLen) throws IOException {
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
      ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
      // kill the previous writer
      rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      rbw.setWriter(Thread.currentThread());
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
  public synchronized ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
    ReplicaBeingWritten replica;
    try {
      // change the replica's state/gs etc.
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        replica = append(b.getBlockPoolId(), (FinalizedReplica) replicaInfo,
                         newGS, b.getNumBytes());
      } else { //RBW
        bumpReplicaGS(replicaInfo, newGS);
        replica = (ReplicaBeingWritten) replicaInfo;
      }
    } catch (IOException e) {
      IOUtils.cleanup(null, ref);
      throw e;
    }
    return new ReplicaHandler(replica, ref);
  }

  @Override // FsDatasetSpi
  public synchronized Replica recoverClose(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    LOG.info("Recover failed close " + b);
    // check replica's state
    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
    // bump the replica's GS
    bumpReplicaGS(replicaInfo, newGS);
    // finalize the replica if RBW
    if (replicaInfo.getState() == ReplicaState.RBW) {
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
    return replicaInfo;
  }
  
  /**
   * Bump a replica's generation stamp to a new one.
   * Its on-disk meta file name is renamed to be the new one too.
   * 
   * @param replicaInfo a replica
   * @param newGS new generation stamp
   * @throws IOException if rename fails
   */
  private void bumpReplicaGS(ReplicaInfo replicaInfo, 
      long newGS) throws IOException { 
    long oldGS = replicaInfo.getGenerationStamp();
    File oldmeta = replicaInfo.getMetaFile();
    replicaInfo.setGenerationStamp(newGS);
    File newmeta = replicaInfo.getMetaFile();

    // rename meta file to new GS
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming " + oldmeta + " to " + newmeta);
    }
    try {
      NativeIO.renameTo(oldmeta, newmeta);
    } catch (IOException e) {
      replicaInfo.setGenerationStamp(oldGS); // restore old GS
      throw new IOException("Block " + replicaInfo + " reopen failed. " +
                            " Unable to move meta file  " + oldmeta +
                            " to " + newmeta, e);
    }
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createRbw(
      StorageType storageType, ExtendedBlock b, boolean allowLazyPersist)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
        b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists in state " + replicaInfo.getState() +
      " and thus cannot be created.");
    }
    // create a new block
    FsVolumeReference ref;
    while (true) {
      try {
        if (allowLazyPersist) {
          // First try to place the block on a transient volume.
          ref = volumes.getNextTransientVolume(b.getNumBytes());
          datanode.getMetrics().incrRamDiskBlocksWrite();
        } else {
          ref = volumes.getNextVolume(storageType, b.getNumBytes());
        }
      } catch (DiskOutOfSpaceException de) {
        if (allowLazyPersist) {
          datanode.getMetrics().incrRamDiskBlocksWriteFallback();
          allowLazyPersist = false;
          continue;
        }
        throw de;
      }
      break;
    }
    FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
    // create an rbw file to hold block in the designated volume
    File f;
    try {
      f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
    } catch (IOException e) {
      IOUtils.cleanup(null, ref);
      throw e;
    }

    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile(), b.getNumBytes());
    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
    return new ReplicaHandler(newReplicaInfo, ref);
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler recoverRbw(
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    LOG.info("Recover RBW replica " + b);

    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
    
    // check the replica's state
    if (replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
    }
    ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
    
    LOG.info("Recovering " + rbw);

    // Stop the previous writer
    rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
    rbw.setWriter(Thread.currentThread());

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
    if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd){
      throw new ReplicaNotFoundException("Unmatched length replica " + 
          replicaInfo + ": BytesAcked = " + bytesAcked + 
          " BytesRcvd = " + numBytes + " are not in the range of [" + 
          minBytesRcvd + ", " + maxBytesRcvd + "].");
    }

    long bytesOnDisk = rbw.getBytesOnDisk();
    long blockDataLength = rbw.getBlockFile().length();
    if (bytesOnDisk != blockDataLength) {
      LOG.info("Resetting bytesOnDisk to match blockDataLength (=" +
          blockDataLength + ") for replica " + rbw);
      bytesOnDisk = blockDataLength;
      rbw.setLastChecksumAndDataLen(bytesOnDisk, null);
    }

    if (bytesOnDisk < bytesAcked) {
      throw new ReplicaNotFoundException("Found fewer bytesOnDisk than " +
          "bytesAcked for replica " + rbw);
    }

    FsVolumeReference ref = rbw.getVolume().obtainReference();
    try {
      // Truncate the potentially corrupt portion.
      // If the source was client and the last node in the pipeline was lost,
      // any corrupt data written after the acked length can go unnoticed.
      if (bytesOnDisk > bytesAcked) {
        final File replicafile = rbw.getBlockFile();
        truncateBlock(replicafile, rbw.getMetaFile(), bytesOnDisk, bytesAcked);
        rbw.setNumBytes(bytesAcked);
        rbw.setLastChecksumAndDataLen(bytesAcked, null);
      }

      // bump the replica's generation stamp to newGS
      bumpReplicaGS(rbw, newGS);
    } catch (IOException e) {
      IOUtils.cleanup(null, ref);
      throw e;
    }
    return new ReplicaHandler(rbw, ref);
  }
  
  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {
    final long blockId = b.getBlockId();
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    LOG.info("Convert " + b + " from Temporary to RBW, visible length="
        + visible);

    final ReplicaInPipeline temp;
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
      temp = (ReplicaInPipeline)r;
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
    final FsVolumeImpl v = (FsVolumeImpl)temp.getVolume();
    if (v == null) {
      throw new IOException("r.getVolume() = null, temp="  + temp);
    }
    
    // move block files to the rbw directory
    BlockPoolSlice bpslice = v.getBlockPoolSlice(b.getBlockPoolId());
    final File dest = moveBlockFiles(b.getLocalBlock(), temp.getBlockFile(), 
        bpslice.getRbwDir());
    // create RBW
    final ReplicaBeingWritten rbw = new ReplicaBeingWritten(
        blockId, numBytes, expectedGs,
        v, dest.getParentFile(), Thread.currentThread(), 0);
    rbw.setBytesAcked(visible);

    // load last checksum and datalen
    final File destMeta = FsDatasetUtil.getMetaFile(dest,
        b.getGenerationStamp());
    byte[] lastChunkChecksum = v.loadLastPartialChunkChecksum(dest, destMeta);
    rbw.setLastChecksumAndDataLen(numBytes, lastChunkChecksum);
    // overwrite the RBW in the volume map
    volumeMap.add(b.getBlockPoolId(), rbw);
    return rbw;
  }

  @Override // FsDatasetSpi
  public ReplicaHandler createTemporary(StorageType storageType,
      ExtendedBlock b, boolean isTransfer) throws IOException {
    long startTimeMs = Time.monotonicNow();
    long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();
    ReplicaInfo lastFoundReplicaInfo = null;
    boolean isInPipeline = false;
    do {
      synchronized (this) {
        ReplicaInfo currentReplicaInfo =
            volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        if (currentReplicaInfo == lastFoundReplicaInfo) {
          break;
        } else {
          isInPipeline = currentReplicaInfo.getState() == ReplicaState.TEMPORARY
              || currentReplicaInfo.getState() == ReplicaState.RBW;
          /*
           * If the current block is old, reject.
           * else If transfer request, then accept it.
           * else if state is not RBW/Temporary, then reject
           */
          if ((currentReplicaInfo.getGenerationStamp() >= b.getGenerationStamp())
              || (!isTransfer && !isInPipeline)) {
            throw new ReplicaAlreadyExistsException("Block " + b
                + " already exists in state " + currentReplicaInfo.getState()
                + " and thus cannot be created.");
          }
          lastFoundReplicaInfo = currentReplicaInfo;
        }
      }

      if (!isInPipeline) {
        continue;
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
      ((ReplicaInPipeline) lastFoundReplicaInfo)
          .stopWriter(writerStopTimeoutMs);
    } while (true);

    if (lastFoundReplicaInfo != null) {
      // Old blockfile should be deleted synchronously as it might collide
      // with the new block if allocated in same volume.
      // Do the deletion outside of lock as its DISK IO.
      invalidate(b.getBlockPoolId(), new Block[] { lastFoundReplicaInfo },
          false);
    }
    synchronized (this) {
      FsVolumeReference ref = volumes.getNextVolume(storageType, b
          .getNumBytes());
      FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
      // create a temporary file to hold block in the designated volume
      File f;
      try {
        f = v.createTmpFile(b.getBlockPoolId(), b.getLocalBlock());
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.getBlockId(), b
          .getGenerationStamp(), v, f.getParentFile(), 0);
      volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
      return new ReplicaHandler(newReplicaInfo, ref);
    }
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
  public void finalizeBlock(ExtendedBlock b, boolean fsyncDir)
      throws IOException {
    ReplicaInfo replicaInfo = null;
    ReplicaInfo finalizedReplicaInfo = null;
    synchronized (this) {
      if (Thread.interrupted()) {
        // Don't allow data modifications from interrupted threads
        throw new IOException("Cannot finalize block from Interrupted Thread");
      }
      replicaInfo = getReplicaInfo(b);
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        // this is legal, when recovery happens on a file that has
        // been opened for append but never modified
        return;
      }
      finalizedReplicaInfo = finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
    /*
     * Sync the directory after rename from tmp/rbw to Finalized if
     * configured. Though rename should be atomic operation, sync on both
     * dest and src directories are done because IOUtils.fsync() calls
     * directory's channel sync, not the journal itself.
     */
    if (fsyncDir) {
      File f = replicaInfo.getBlockFile();
      File dest = finalizedReplicaInfo.getBlockFile();
      fsyncDirectory(dest.getParentFile(), f.getParentFile());
    }
  }

  private synchronized FinalizedReplica finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    FinalizedReplica newReplicaInfo = null;
    if (replicaInfo.getState() == ReplicaState.RUR &&
       ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica().getState() == 
         ReplicaState.FINALIZED) {
      newReplicaInfo = (FinalizedReplica)
             ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica();
      newReplicaInfo.loadLastPartialChunkChecksum();
    } else {
      FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
      File f = replicaInfo.getBlockFile();
      if (v == null) {
        throw new IOException("No volume for temporary file " + f +
            " for block " + replicaInfo);
      }

      File dest = v.addFinalizedBlock(
          bpid, replicaInfo, f, replicaInfo.getBytesReserved());
      newReplicaInfo = new FinalizedReplica(replicaInfo, v, dest.getParentFile());

      byte[] checksum = null;
      // copy the last partial checksum if the replica is originally
      // in finalized or rbw state.
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        FinalizedReplica finalized = (FinalizedReplica)replicaInfo;
        checksum = finalized.getLastPartialChunkChecksum();
      } else if (replicaInfo.getState() == ReplicaState.RBW) {
        ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;
        checksum = rbw.getLastChecksumAndDataLen().getChecksum();
      }
      newReplicaInfo.setLastPartialChunkChecksum(checksum);

      if (v.isTransientStorage()) {
        ramDiskReplicaTracker.addReplica(bpid, replicaInfo.getBlockId(), v);
        datanode.getMetrics().addRamDiskBytesWrite(replicaInfo.getNumBytes());
      }
    }
    volumeMap.add(bpid, newReplicaInfo);

    return newReplicaInfo;
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FsDatasetSpi
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    if (replicaInfo != null && replicaInfo.getState() == ReplicaState.TEMPORARY) {
      // remove from volumeMap
      volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());
      
      // delete the on-disk temp file
      if (delBlockFromDisk(replicaInfo.getBlockFile(), 
          replicaInfo.getMetaFile(), b.getLocalBlock())) {
        LOG.warn("Block " + b + " unfinalized and removed. " );
      }
      if (replicaInfo.getVolume().isTransientStorage()) {
        ramDiskReplicaTracker.discardReplica(b.getBlockPoolId(), b.getBlockId(), true);
      }
    }
  }

  /**
   * Remove a block from disk
   * @param blockFile block file
   * @param metaFile block meta file
   * @param b a block
   * @return true if on-disk files are deleted; false otherwise
   */
  private boolean delBlockFromDisk(File blockFile, File metaFile, Block b) {
    if (blockFile == null) {
      LOG.warn("No file exists for block: " + b);
      return true;
    }
    
    if (!blockFile.delete()) {
      LOG.warn("Not able to delete the block file: " + blockFile);
      return false;
    } else { // remove the meta file
      if (metaFile != null && !metaFile.delete()) {
        LOG.warn("Not able to delete the meta block file: " + metaFile);
        return false;
      }
    }
    return true;
  }

  @Override
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReportsMap =
        new HashMap<DatanodeStorage, BlockListAsLongs>();

    Map<String, BlockListAsLongs.Builder> builders =
        new HashMap<String, BlockListAsLongs.Builder>();

    List<FsVolumeImpl> curVolumes = getVolumes();
    for (FsVolumeSpi v : curVolumes) {
      builders.put(v.getStorageID(), BlockListAsLongs.builder());
    }

    synchronized(this) {
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        switch(b.getState()) {
          case FINALIZED:
          case RBW:
          case RWR:
            builders.get(b.getVolume().getStorageID()).add(b);
            break;
          case RUR:
            ReplicaUnderRecovery rur = (ReplicaUnderRecovery)b;
            builders.get(rur.getVolume().getStorageID())
                .add(rur.getOriginalReplica());
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

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    return cacheManager.getCachedBlocks(bpid);
  }

  /**
   * Get the list of finalized blocks from in-memory blockmap for a block pool.
   */
  @Override
  public synchronized List<FinalizedReplica> getFinalizedBlocks(String bpid) {
    ArrayList<FinalizedReplica> finalized =
        new ArrayList<FinalizedReplica>(volumeMap.size(bpid));
    for (ReplicaInfo b : volumeMap.replicas(bpid)) {
      if(b.getState() == ReplicaState.FINALIZED) {
        finalized.add(new FinalizedReplica((FinalizedReplica)b));
      }
    }
    return finalized;
  }

  /**
   * Get the list of finalized blocks from in-memory blockmap for a block pool.
   */
  @Override
  public synchronized List<FinalizedReplica> getFinalizedBlocksOnPersistentStorage(String bpid) {
    ArrayList<FinalizedReplica> finalized =
        new ArrayList<FinalizedReplica>(volumeMap.size(bpid));
    for (ReplicaInfo b : volumeMap.replicas(bpid)) {
      if(!b.getVolume().isTransientStorage() &&
         b.getState() == ReplicaState.FINALIZED) {
        finalized.add(new FinalizedReplica((FinalizedReplica)b));
      }
    }
    return finalized;
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
    if (!replicaInfo.getBlockFile().exists()) {
      throw new FileNotFoundException(replicaInfo.getBlockFile().getPath());
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
  File validateBlockFile(String bpid, long blockId) {
    //Should we check for metadata file too?
    final File f;
    synchronized(this) {
      f = getFile(bpid, blockId, false);
    }
    
    if(f != null ) {
      if(f.exists())
        return f;
   
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskErrorAsync();
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("blockId=" + blockId + ", f=" + f);
    }
    return null;
  }

  /** Check the files of a replica. */
  static void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //check replica's file
    final File f = r.getBlockFile();
    if (!f.exists()) {
      throw new FileNotFoundException("File " + f + " not found, r=" + r);
    }
    if (r.getBytesOnDisk() != f.length()) {
      throw new IOException("File length mismatched.  The length of "
          + f + " is " + f.length() + " but r=" + r);
    }

    //check replica's meta file
    final File metafile = FsDatasetUtil.getMetaFile(f, r.getGenerationStamp());
    if (!metafile.exists()) {
      throw new IOException("Metafile " + metafile + " does not exist, r=" + r);
    }
    if (metafile.length() == 0) {
      throw new IOException("Metafile " + metafile + " is empty, r=" + r);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    invalidate(bpid, invalidBlks, true);
  }

  private void invalidate(String bpid, Block[] invalidBlks, boolean async)
      throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      final File f;
      final FsVolumeImpl v;
      synchronized (this) {
        final ReplicaInfo info = volumeMap.get(bpid, invalidBlks[i]);
        if (info == null) {
          // It is okay if the block is not found -- it may be deleted earlier.
          LOG.info("Failed to delete replica " + invalidBlks[i]
              + ": ReplicaInfo not found.");
          continue;
        }
        if (info.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              + ": GenerationStamp not matched, info=" + info);
          continue;
        }
        f = info.getBlockFile();
        v = (FsVolumeImpl)info.getVolume();
        if (v == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". No volume for this replica, file=" + f);
          continue;
        }
        File parent = f.getParentFile();
        if (parent == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". Parent not found for file " + f);
          continue;
        }
        ReplicaInfo removing = volumeMap.remove(bpid, invalidBlks[i]);
        addDeletingBlock(bpid, removing.getBlockId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Block file " + removing.getBlockFile().getName()
              + " is to be deleted");
        }
        if (removing instanceof ReplicaInPipelineInterface) {
          ((ReplicaInPipelineInterface) removing).releaseAllBytesReserved();
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

      try {
        // Delete the block asynchronously to make sure we can do it fast
        // enough.
        // It's ok to unlink the block file before the uncache operation
        // finishes.
        if (async) {
          asyncDiskService.deleteAsync(v.obtainReference(), f,
              FsDatasetUtil.getMetaFile(f, invalidBlks[i].getGenerationStamp()),
              new ExtendedBlock(bpid, invalidBlks[i]),
              dataStorage.getTrashDirectoryForBlockFile(bpid, f));
        } else {
          asyncDiskService.deleteSync(v.obtainReference(), f,
              FsDatasetUtil.getMetaFile(f, invalidBlks[i].getGenerationStamp()),
              new ExtendedBlock(bpid, invalidBlks[i]),
              dataStorage.getTrashDirectoryForBlockFile(bpid, f));
        }
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

    synchronized (this) {
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
      blockFileName = info.getBlockFile().getAbsolutePath();
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
  public synchronized boolean contains(final ExtendedBlock block) {
    final long blockId = block.getLocalBlock().getBlockId();
    return getFile(block.getBlockPoolId(), blockId, false) != null;
  }

  /**
   * Turn the block identifier into a filename
   * @param bpid Block pool Id
   * @param blockId a block's id
   * @return on disk data file path; null if the replica does not exist
   */
  File getFile(final String bpid, final long blockId, boolean touch) {
    ReplicaInfo info = volumeMap.get(bpid, blockId);
    if (info != null) {
      if (touch && info.getVolume().isTransientStorage()) {
        ramDiskReplicaTracker.touch(bpid, blockId);
        datanode.getMetrics().incrRamDiskBlocksReadHits();
      }
      return info.getBlockFile();
    }
    return null;    
  }

  /**
   * check if a data directory is healthy
   *
   * if some volumes failed - the caller must emove all the blocks that belong
   * to these failed volumes.
   * @return the failed volumes. Returns null if no volume failed.
   */
  @Override // FsDatasetSpi
  public Set<File> checkDataDir() {
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

    ((LazyWriter) lazyWriter.getRunnable()).stop();
    lazyWriter.interrupt();

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

    try {
      lazyWriter.join();
    } catch (InterruptedException ie) {
      LOG.warn("FsDatasetImpl.shutdown ignoring InterruptedException " +
               "from LazyWriter.join");
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
    synchronized (this) {
      memBlockInfo = volumeMap.get(bpid, blockId);
      if (memBlockInfo != null && memBlockInfo.getState() != ReplicaState.FINALIZED) {
        // Block is not finalized - ignore the difference
        return;
      }

      final long diskGS = diskMetaFile != null && diskMetaFile.exists() ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
            GenerationStamp.GRANDFATHER_GENERATION_STAMP;

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
        if (!memBlockInfo.getBlockFile().exists()) {
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
        ReplicaInfo diskBlockInfo = new FinalizedReplica(blockId, 
            diskFile.length(), diskGS, vol, diskFile.getParentFile());
        volumeMap.add(bpid, diskBlockInfo);
        if (vol.isTransientStorage()) {
          ramDiskReplicaTracker.addReplica(bpid, blockId, (FsVolumeImpl) vol);
        }
        LOG.warn("Added missing block to memory " + diskBlockInfo);
        return;
      }
      /*
       * Block exists in volumeMap and the block file exists on the disk
       */
      // Compare block files
      File memFile = memBlockInfo.getBlockFile();
      if (memFile.exists()) {
        if (memFile.compareTo(diskFile) != 0) {
          if (diskMetaFile.exists()) {
            if (memBlockInfo.getMetaFile().exists()) {
              // We have two sets of block+meta files. Decide which one to
              // keep.
              ReplicaInfo diskBlockInfo = new FinalizedReplica(
                  blockId, diskFile.length(), diskGS, vol, diskFile.getParentFile());
              ((FsVolumeImpl) vol).getBlockPoolSlice(bpid).resolveDuplicateReplicas(
                  memBlockInfo, diskBlockInfo, volumeMap);
            }
          } else {
            if (!diskFile.delete()) {
              LOG.warn("Failed to delete " + diskFile + ". Will retry on next scan");
            }
          }
        }
      } else {
        // Block refers to a block file that does not exist.
        // Update the block with the file found on the disk. Since the block
        // file and metadata file are found as a pair on the disk, update
        // the block based on the metadata file found on the disk
        LOG.warn("Block file in volumeMap "
            + memFile.getAbsolutePath()
            + " does not exist. Updating it to the file found during scan "
            + diskFile.getAbsolutePath());
        memBlockInfo.setDir(diskFile.getParentFile());
        memFile = diskFile;

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
          long gs = diskMetaFile != null && diskMetaFile.exists()
              && diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
              : GenerationStamp.GRANDFATHER_GENERATION_STAMP;

          LOG.warn("Updating generation stamp for block " + blockId
              + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

          memBlockInfo.setGenerationStamp(gs);
        }
      }

      // Compare block size
      if (memBlockInfo.getNumBytes() != memFile.length()) {
        // Update the length based on the block file
        corruptBlock = new Block(memBlockInfo);
        LOG.warn("Updating size of block " + blockId + " from "
            + memBlockInfo.getNumBytes() + " to " + memFile.length());
        memBlockInfo.setNumBytes(memFile.length());
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
  public synchronized String getReplicaString(String bpid, long blockId) {
    final Replica r = volumeMap.get(bpid, blockId);
    return r == null? "null": r.toString();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaRecoveryInfo initReplicaRecovery(
      RecoveringBlock rBlock) throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), volumeMap,
        rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(),
        datanode.getDnConf().getXceiverStopTimeout());
  }

  /** static version of {@link #initReplicaRecovery(RecoveringBlock)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid, ReplicaMap map,
      Block block, long recoveryId, long xceiverStopTimeout) throws IOException {
    final ReplicaInfo replica = map.get(bpid, block.getBlockId());
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      return null;
    }

    //stop writer if there is any
    if (replica instanceof ReplicaInPipeline) {
      final ReplicaInPipeline rip = (ReplicaInPipeline)replica;
      rip.stopWriter(xceiverStopTimeout);

      //check replica bytes on disk.
      if (rip.getBytesOnDisk() < rip.getVisibleLength()) {
        throw new IOException("getBytesOnDisk() < getVisibleLength(), rip="
            + rip);
      }

      //check the replica's files
      checkReplicaFiles(rip);
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
    final ReplicaUnderRecovery rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = (ReplicaUnderRecovery)replica;
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
      rur = new ReplicaUnderRecovery(replica, recoveryId);
      map.add(bpid, rur);
      LOG.info("initReplicaRecovery: changing replica state for "
          + block + " from " + replica.getState()
          + " to " + rur.getState());
      if (replica.getState() == ReplicaState.TEMPORARY || replica
          .getState() == ReplicaState.RBW) {
        ((ReplicaInPipeline) replica).releaseAllBytesReserved();
      }
    }
    return rur.createInfo();
  }

  @Override // FsDatasetSpi
  public synchronized Replica updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
                                    final long newBlockId,
                                    final long newlength) throws IOException {
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
    final FinalizedReplica finalized = updateReplicaUnderRecovery(oldBlock
        .getBlockPoolId(), (ReplicaUnderRecovery) replica, recoveryId,
        newBlockId, newlength);

    boolean copyTruncate = newBlockId != oldBlock.getBlockId();
    if(!copyTruncate) {
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

  private FinalizedReplica updateReplicaUnderRecovery(
                                          String bpid,
                                          ReplicaUnderRecovery rur,
                                          long recoveryId,
                                          long newBlockId,
                                          long newlength) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId
          + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    File blockFile;
    File metaFile;
    // bump rur's GS to be recovery id
    if(!copyOnTruncate) {
      bumpReplicaGS(rur, recoveryId);
      blockFile = rur.getBlockFile();
      metaFile = rur.getMetaFile();
    } else {
      File[] copiedReplicaFiles =
          copyReplicaWithNewBlockIdAndGS(rur, bpid, newBlockId, recoveryId);
      blockFile = copiedReplicaFiles[1];
      metaFile = copiedReplicaFiles[0];
    }

    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {
      rur.unlinkBlock(1);
      truncateBlock(blockFile, metaFile, rur.getNumBytes(), newlength);
      if(!copyOnTruncate) {
        // update RUR with the new length
        rur.setNumBytes(newlength);
      } else {
        // Copying block to a new block with new blockId.
        // Not truncating original block.
        FsVolumeSpi volume = rur.getVolume();
        String blockPath = blockFile.getAbsolutePath();
        String volumePath = volume.getBasePath();
        assert blockPath.startsWith(volumePath) :
            "New block file: " + blockPath + " must be on " +
                "same volume as recovery replica: " + volumePath;
        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
            newBlockId, recoveryId, volume, blockFile.getParentFile(),
            newlength);
        newReplicaInfo.setNumBytes(newlength);
        // In theory, this rbw replica needs to reload last chunk checksum,
        // but it is immediately converted to finalized state within the same
        // lock, so no need to update it.
        volumeMap.add(bpid, newReplicaInfo);
        finalizeReplica(bpid, newReplicaInfo);
      }
   }

    // finalize the block
    return finalizeReplica(bpid, rur);
  }

  private File[] copyReplicaWithNewBlockIdAndGS(
      ReplicaUnderRecovery replicaInfo, String bpid, long newBlkId, long newGS)
      throws IOException {
    String blockFileName = Block.BLOCK_FILE_PREFIX + newBlkId;
    FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
    final File tmpDir = v.getBlockPoolSlice(bpid).getTmpDir();
    final File destDir = DatanodeUtil.idToBlockDir(tmpDir, newBlkId);
    final File dstBlockFile = new File(destDir, blockFileName);
    final File dstMetaFile = FsDatasetUtil.getMetaFile(dstBlockFile, newGS);
    return copyBlockFiles(replicaInfo.getMetaFile(), replicaInfo.getBlockFile(),
        dstMetaFile, dstBlockFile, true);
  }

  @Override // FsDatasetSpi
  public synchronized long getReplicaVisibleLength(final ExtendedBlock block)
  throws IOException {
    final Replica replica = getReplicaInfo(block.getBlockPoolId(), 
        block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }
  
  @Override
  public void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    LOG.info("Adding block pool " + bpid);
    synchronized(this) {
      volumes.addBlockPool(bpid, conf);
      volumeMap.initBlockPool(bpid);
    }
    volumes.getAllVolumesMap(bpid, volumeMap, ramDiskReplicaTracker);
  }

  @Override
  public synchronized void shutdownBlockPool(String bpid) {
    LOG.info("Removing block pool " + bpid);
    volumeMap.cleanUpBlockPool(bpid);
    volumes.removeBlockPool(bpid);
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS and RBW

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FsVolumeImpl volume : getVolumes()) {
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
      info.put(v.directory, innerInfo);
    }
    return info;
  }

  @Override //FsDatasetSpi
  public synchronized void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    List<FsVolumeImpl> curVolumes = getVolumes();
    if (!force) {
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
          if (!volume.isBPDirEmpty(bpid)) {
            LOG.warn(bpid + " has some block files, cannot delete unless forced");
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
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    synchronized(this) {
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

    File datafile = getBlockFile(block);
    File metafile = FsDatasetUtil.getMetaFile(datafile, block.getGenerationStamp());
    BlockLocalPathInfo info = new BlockLocalPathInfo(block,
        datafile.getAbsolutePath(), metafile.getAbsolutePath());
    return info;
  }

  @Override // FsDatasetSpi
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String poolId,
      long[] blockIds) throws IOException {
    List<FsVolumeImpl> curVolumes = getVolumes();
    // List of VolumeIds, one per volume on the datanode
    List<byte[]> blocksVolumeIds = new ArrayList<>(curVolumes.size());
    // List of indexes into the list of VolumeIds, pointing at the VolumeId of
    // the volume that the block is on
    List<Integer> blocksVolumeIndexes = new ArrayList<Integer>(blockIds.length);
    // Initialize the list of VolumeIds simply by enumerating the volumes
    for (int i = 0; i < curVolumes.size(); i++) {
      blocksVolumeIds.add(ByteBuffer.allocate(4).putInt(i).array());
    }
    // Determine the index of the VolumeId of each block's volume, by comparing 
    // the block's volume against the enumerated volumes
    for (int i = 0; i < blockIds.length; i++) {
      long blockId = blockIds[i];
      boolean isValid = false;

      ReplicaInfo info = volumeMap.get(poolId, blockId);
      int volumeIndex = 0;
      if (info != null) {
        FsVolumeSpi blockVolume = info.getVolume();
        for (FsVolumeImpl volume : curVolumes) {
          // This comparison of references should be safe
          if (blockVolume == volume) {
            isValid = true;
            break;
          }
          volumeIndex++;
        }
      }
      // Indicates that the block is not present, or not found in a data dir
      if (!isValid) {
        volumeIndex = Integer.MAX_VALUE;
      }
      blocksVolumeIndexes.add(volumeIndex);
    }
    return new HdfsBlocksMetadata(poolId, blockIds,
        blocksVolumeIds, blocksVolumeIndexes);
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
    synchronized (FsDatasetImpl.this) {
      ramDiskReplicaTracker.recordEndLazyPersist(bpId, blockId, savedFiles);

      targetVolume.incDfsUsed(bpId,
          savedFiles[0].length() + savedFiles[1].length());

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
    for (FsVolumeImpl v: getVolumes()){
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
    for (FsVolumeImpl v: getVolumes()){
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
        !asyncLazyPersistService.queryVolume(v.getCurrentDir())) {
      asyncLazyPersistService.addVolume(v.getCurrentDir());
    }

    // Remove thread for DISK volume if RamDisk is not configured
    if (!ramDiskConfigured &&
        asyncLazyPersistService.queryVolume(v.getCurrentDir())) {
      asyncLazyPersistService.removeVolume(v.getCurrentDir());
    }
  }

  private void removeOldReplica(ReplicaInfo replicaInfo,
      ReplicaInfo newReplicaInfo, File blockFile, File metaFile,
      long blockFileUsed, long metaFileUsed, final String bpid) {
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
    if (blockFile.delete() || !blockFile.exists()) {
      ((FsVolumeImpl) replicaInfo.getVolume()).decDfsUsed(bpid, blockFileUsed);
      if (metaFile.delete() || !metaFile.exists()) {
        ((FsVolumeImpl) replicaInfo.getVolume()).decDfsUsed(bpid, metaFileUsed);
      }
    }

    // If deletion failed then the directory scanner will cleanup the blocks
    // eventually.
  }

  class LazyWriter implements Runnable {
    private volatile boolean shouldRun = true;
    final int checkpointerInterval;
    final float lowWatermarkFreeSpacePercentage;
    final long lowWatermarkFreeSpaceBytes;


    public LazyWriter(Configuration conf) {
      this.checkpointerInterval = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC);
      this.lowWatermarkFreeSpacePercentage = conf.getFloat(
          DFSConfigKeys.DFS_DATANODE_RAM_DISK_LOW_WATERMARK_PERCENT,
          DFSConfigKeys.DFS_DATANODE_RAM_DISK_LOW_WATERMARK_PERCENT_DEFAULT);
      this.lowWatermarkFreeSpaceBytes = conf.getLong(
          DFSConfigKeys.DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES,
          DFSConfigKeys.DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES_DEFAULT);
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
          synchronized (FsDatasetImpl.this) {
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
                  replicaInfo.getMetaFile(), replicaInfo.getBlockFile(),
                  targetReference);
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

    private boolean transientFreeSpaceBelowThreshold() throws IOException {
      long free = 0;
      long capacity = 0;
      float percentFree = 0.0f;

      // Don't worry about fragmentation for now. We don't expect more than one
      // transient volume per DN.
      for (FsVolumeImpl v : getVolumes()) {
        try (FsVolumeReference ref = v.obtainReference()) {
          if (v.isTransientStorage()) {
            capacity += v.getCapacity();
            free += v.getAvailable();
          }
        } catch (ClosedChannelException e) {
          // ignore.
        }
      }

      if (capacity == 0) {
        return false;
      }

      percentFree = (float) ((double)free * 100 / capacity);
      return (percentFree < lowWatermarkFreeSpacePercentage) ||
          (free < lowWatermarkFreeSpaceBytes);
    }

    /**
     * Attempt to evict one or more transient block replicas we have at least
     * spaceNeeded bytes free.
     */
    private void evictBlocks() throws IOException {
      int iterations = 0;

      while (iterations++ < MAX_BLOCK_EVICTIONS_PER_ITERATION &&
             transientFreeSpaceBelowThreshold()) {
        RamDiskReplica replicaState = ramDiskReplicaTracker.getNextCandidateForEviction();

        if (replicaState == null) {
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Evicting block " + replicaState);
        }

        ReplicaInfo replicaInfo, newReplicaInfo;
        File blockFile, metaFile;
        long blockFileUsed, metaFileUsed;
        final String bpid = replicaState.getBlockPoolId();

        synchronized (FsDatasetImpl.this) {
          replicaInfo = getReplicaInfo(replicaState.getBlockPoolId(), replicaState.getBlockId());
          Preconditions.checkState(replicaInfo.getVolume().isTransientStorage());
          blockFile = replicaInfo.getBlockFile();
          metaFile = replicaInfo.getMetaFile();
          blockFileUsed = blockFile.length();
          metaFileUsed = metaFile.length();
          ramDiskReplicaTracker.discardReplica(replicaState.getBlockPoolId(),
              replicaState.getBlockId(), false);

          // Move the replica from lazyPersist/ to finalized/ on target volume
          BlockPoolSlice bpSlice =
              replicaState.getLazyPersistVolume().getBlockPoolSlice(bpid);
          File newBlockFile = bpSlice.activateSavedReplica(
              replicaInfo, replicaState.getSavedMetaFile(),
              replicaState.getSavedBlockFile());

          newReplicaInfo =
              new FinalizedReplica(replicaInfo.getBlockId(),
                                   replicaInfo.getBytesOnDisk(),
                                   replicaInfo.getGenerationStamp(),
                                   replicaState.getLazyPersistVolume(),
                                   newBlockFile.getParentFile());

          // Update the volumeMap entry.
          volumeMap.add(bpid, newReplicaInfo);

          // Update metrics
          datanode.getMetrics().incrRamDiskBlocksEvicted();
          datanode.getMetrics().addRamDiskBlocksEvictionWindowMs(
              Time.monotonicNow() - replicaState.getCreationTime());
          if (replicaState.getNumReads() == 0) {
            datanode.getMetrics().incrRamDiskBlocksEvictedWithoutRead();
          }
        }

        removeOldReplica(replicaInfo, newReplicaInfo, blockFile, metaFile,
            blockFileUsed, metaFileUsed, bpid);
      }
    }

    @Override
    public void run() {
      int numSuccessiveFailures = 0;

      while (fsRunning && shouldRun) {
        try {
          numSuccessiveFailures = saveNextReplica() ? 0 : (numSuccessiveFailures + 1);
          evictBlocks();

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

    File f = getBlockFile(block);
    Path p = new Path(f.getAbsolutePath());
    
    FsPermission oldPermission = localFS.getFileStatus(
        new Path(f.getAbsolutePath())).getPermission();
    //sticky bit is used for pinning purpose
    FsPermission permission = new FsPermission(oldPermission.getUserAction(),
        oldPermission.getGroupAction(), oldPermission.getOtherAction(), true);
    localFS.setPermission(p, permission);
  }
  
  @Override
  public boolean getPinning(ExtendedBlock block) throws IOException {
    if (!blockPinningEnabled) {
      return  false;
    }
    File f = getBlockFile(block);
        
    FileStatus fss = localFS.getFileStatus(new Path(f.getAbsolutePath()));
    return fss.getPermission().getStickyBit();
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

  synchronized void stopAllDataxceiverThreads(FsVolumeImpl volume) {
    for (String blockPoolId : volumeMap.getBlockPoolList()) {
      Collection<ReplicaInfo> replicas = volumeMap.replicas(blockPoolId);
      for (ReplicaInfo replicaInfo : replicas) {
        if (replicaInfo instanceof ReplicaInPipeline
            && replicaInfo.getVolume().equals(volume)) {
          ReplicaInPipeline replicaInPipeline = (ReplicaInPipeline) replicaInfo;
          replicaInPipeline.interruptThread();
        }
      }
    }
  }
}

