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
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
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
import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.ReplicaWaitingToBeRecovered;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
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
    return volumes.volumes;
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override // FsDatasetSpi
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    StorageReport[] reports;
    synchronized (statsLock) {
      reports = new StorageReport[volumes.volumes.size()];
      int i = 0;
      for (FsVolumeImpl volume : volumes.volumes) {
        reports[i++] = new StorageReport(volume.toDatanodeStorage(),
                                         false,
                                         volume.getCapacity(),
                                         volume.getDfsUsed(),
                                         volume.getAvailable(),
                                         volume.getBlockPoolUsed(bpid));
      }
    }

    return reports;
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
  final RamDiskReplicaTracker ramDiskReplicaTracker;
  final RamDiskAsyncLazyPersistService asyncLazyPersistService;

  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;


  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

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

    int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
    int volsFailed = volsConfigured - storage.getNumStorageDirs();
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
    volumes = new FsVolumeList(volsFailed, blockChooserImpl);
    asyncDiskService = new FsDatasetAsyncDiskService(datanode);
    asyncLazyPersistService = new RamDiskAsyncLazyPersistService(datanode);

    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      addVolume(dataLocations, storage.getStorageDir(idx));
    }
    setupAsyncLazyPersistThreads();

    cacheManager = new FsDatasetCache(this);

    // Start the lazy writer once we have built the replica maps.
    lazyWriter = new Daemon(new LazyWriter(conf));
    lazyWriter.start();
    registerMBean(datanode.getDatanodeUuid());
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
    ReplicaMap tempVolumeMap = new ReplicaMap(this);
    fsVolume.getVolumeMap(tempVolumeMap, ramDiskReplicaTracker);

    volumeMap.addAll(tempVolumeMap);
    volumes.addVolume(fsVolume);
    storageMap.put(sd.getStorageUuid(),
        new DatanodeStorage(sd.getStorageUuid(),
            DatanodeStorage.State.NORMAL,
            storageType));
    asyncDiskService.addVolume(sd.getCurrentDir());

    LOG.info("Added volume - " + dir + ", StorageType: " + storageType);
  }

  private void addVolumeAndBlockPool(Collection<StorageLocation> dataLocations,
      Storage.StorageDirectory sd, final Collection<String> bpids)
      throws IOException {
    final File dir = sd.getCurrentDir();
    final StorageType storageType =
        getStorageTypeFromLocations(dataLocations, sd.getRoot());

    final FsVolumeImpl fsVolume = new FsVolumeImpl(
        this, sd.getStorageUuid(), dir, this.conf, storageType);
    final ReplicaMap tempVolumeMap = new ReplicaMap(fsVolume);

    List<IOException> exceptions = Lists.newArrayList();
    for (final String bpid : bpids) {
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
      // The states of FsDatasteImpl are not modified, thus no need to rolled back.
      throw MultipleIOException.createIOException(exceptions);
    }

    volumeMap.addAll(tempVolumeMap);
    storageMap.put(sd.getStorageUuid(),
        new DatanodeStorage(sd.getStorageUuid(),
            DatanodeStorage.State.NORMAL,
            storageType));
    asyncDiskService.addVolume(sd.getCurrentDir());
    volumes.addVolume(fsVolume);

    LOG.info("Added volume - " + dir + ", StorageType: " + storageType);
  }

  /**
   * Add an array of StorageLocation to FsDataset.
   *
   * @pre dataStorage must have these volumes.
   * @param volumes an array of storage locations for adding volumes.
   * @param bpids block pool IDs.
   * @return an array of successfully loaded volumes.
   */
  @Override
  public synchronized List<StorageLocation> addVolumes(
      final List<StorageLocation> volumes, final Collection<String> bpids) {
    final Collection<StorageLocation> dataLocations =
        DataNode.getStorageLocations(this.conf);
    final Map<String, Storage.StorageDirectory> allStorageDirs =
        new HashMap<String, Storage.StorageDirectory>();
    List<StorageLocation> succeedVolumes = Lists.newArrayList();
    try {
      for (int idx = 0; idx < dataStorage.getNumStorageDirs(); idx++) {
        Storage.StorageDirectory sd = dataStorage.getStorageDir(idx);
        allStorageDirs.put(sd.getRoot().getCanonicalPath(), sd);
      }
    } catch (IOException ioe) {
      LOG.warn("Caught exception when parsing storage URL.", ioe);
      return succeedVolumes;
    }

    final boolean[] successFlags = new boolean[volumes.size()];
    Arrays.fill(successFlags, false);
    List<Thread> volumeAddingThreads = Lists.newArrayList();
    for (int i = 0; i < volumes.size(); i++) {
      final int idx = i;
      Thread t = new Thread() {
        public void run() {
          StorageLocation vol = volumes.get(idx);
          try {
            String key = vol.getFile().getCanonicalPath();
            if (!allStorageDirs.containsKey(key)) {
              LOG.warn("Attempt to add an invalid volume: " + vol.getFile());
            } else {
              addVolumeAndBlockPool(dataLocations, allStorageDirs.get(key),
                  bpids);
              successFlags[idx] = true;
            }
          } catch (IOException e) {
            LOG.warn("Caught exception when adding volume " + vol, e);
          }
        }
      };
      volumeAddingThreads.add(t);
      t.start();
    }

    for (Thread t : volumeAddingThreads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        LOG.warn("Caught InterruptedException when adding volume.", e);
      }
    }

    setupAsyncLazyPersistThreads();

    for (int i = 0; i < volumes.size(); i++) {
      if (successFlags[i]) {
        succeedVolumes.add(volumes.get(i));
      }
    }
    return succeedVolumes;
  }

  /**
   * Removes a collection of volumes from FsDataset.
   * @param volumes the root directories of the volumes.
   *
   * DataNode should call this function before calling
   * {@link DataStorage#removeVolumes(java.util.Collection)}.
   */
  @Override
  public synchronized void removeVolumes(Collection<StorageLocation> volumes) {
    Set<File> volumeSet = new HashSet<File>();
    for (StorageLocation sl : volumes) {
      volumeSet.add(sl.getFile());
    }
    for (int idx = 0; idx < dataStorage.getNumStorageDirs(); idx++) {
      Storage.StorageDirectory sd = dataStorage.getStorageDir(idx);
      if (volumeSet.contains(sd.getRoot())) {
        String volume = sd.getRoot().toString();
        LOG.info("Removing " + volume + " from FsDataset.");

        // Disable the volume from the service.
        asyncDiskService.removeVolume(sd.getCurrentDir());
        this.volumes.removeVolume(volume);

        // Removed all replica information for the blocks on the volume. Unlike
        // updating the volumeMap in addVolume(), this operation does not scan
        // disks.
        for (String bpid : volumeMap.getBlockPoolList()) {
          List<Block> blocks = new ArrayList<Block>();
          for (Iterator<ReplicaInfo> it = volumeMap.replicas(bpid).iterator();
              it.hasNext(); ) {
            ReplicaInfo block = it.next();
            if (block.getVolume().getBasePath().equals(volume)) {
              invalidate(bpid, block);
              blocks.add(block);
              it.remove();
            }
          }
          // Delete blocks from the block scanner in batch.
          datanode.getBlockScanner().deleteBlocks(bpid,
              blocks.toArray(new Block[blocks.size()]));
        }

        storageMap.remove(sd.getStorageUuid());
      }
    }
    setupAsyncLazyPersistThreads();
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
  @Override
  public int getNumFailedVolumes() {
    return volumes.numberOfFailedVolumes();
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
      RandomAccessFile blockInFile;
      try {
        blockInFile = new RandomAccessFile(blockFile, "r");
      } catch (FileNotFoundException fnfe) {
        throw new IOException("Block " + b + " is not valid. " +
            "Expected block file at " + blockFile + " does not exist.");
      }

      if (seekOffset > 0) {
        blockInFile.seek(seekOffset);
      }
      return new FileInputStream(blockInFile.getFD());
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
                          long blkOffset, long ckoff) throws IOException {
    ReplicaInfo info = getReplicaInfo(b);
    File blockFile = info.getBlockFile();
    RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
    if (blkOffset > 0) {
      blockInFile.seek(blkOffset);
    }
    File metaFile = info.getMetaFile();
    RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
    if (ckoff > 0) {
      metaInFile.seek(ckoff);
    }
    return new ReplicaInputStreams(blockInFile.getFD(), metaInFile.getFD());
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

  /**
   * Copy the block and meta files for the given block to the given destination.
   * @return the new meta and block files.
   * @throws IOException
   */
  static File[] copyBlockFiles(long blockId, long genStamp,
                               File srcMeta, File srcFile, File destRoot)
      throws IOException {
    final File destDir = DatanodeUtil.idToBlockDir(destRoot, blockId);
    final File dstFile = new File(destDir, srcFile.getName());
    final File dstMeta = FsDatasetUtil.getMetaFile(dstFile, genStamp);
    computeChecksum(srcMeta, dstMeta, srcFile);

    try {
      Storage.nativeCopyFileUnbuffered(srcFile, dstFile, true);
    } catch (IOException e) {
      throw new IOException("Failed to copy " + srcFile + " to " + dstFile, e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Copied " + srcMeta + " to " + dstMeta +
          " and calculated checksum");
      LOG.debug("Copied " + srcFile + " to " + dstFile);
    }
    return new File[] {dstMeta, dstFile};
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
  private static void computeChecksum(File srcMeta, File dstMeta, File blockFile)
      throws IOException {
    final DataChecksum checksum = BlockMetadataHeader.readDataChecksum(srcMeta);
    final byte[] data = new byte[1 << 16];
    final byte[] crcs = new byte[checksum.getChecksumSize(data.length)];

    DataOutputStream metaOut = null;
    InputStream dataIn = null;
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

      dataIn = isNativeIOAvailable ?
          NativeIO.getShareDeleteFileInputStream(blockFile) :
          new FileInputStream(blockFile);

      int offset = 0;
      for(int n; (n = dataIn.read(data, offset, data.length - offset)) != -1; ) {
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

      // calculate and write the last crc
      checksum.calculateChunkedSums(data, 0, offset, crcs, 0);
      metaOut.write(crcs, 0, 4);
    } finally {
      IOUtils.cleanup(LOG, dataIn, metaOut);
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
  public synchronized ReplicaInPipeline append(ExtendedBlock b,
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

    return append(b.getBlockPoolId(), (FinalizedReplica)replicaInfo, newGS,
        b.getNumBytes());
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
    if (v.getAvailable() < estimateBlockLen - replicaInfo.getNumBytes()) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }
    File newBlkFile = new File(v.getRbwDir(bpid), replicaInfo.getBlockName());
    File oldmeta = replicaInfo.getMetaFile();
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
        replicaInfo.getBlockId(), replicaInfo.getNumBytes(), newGS,
        v, newBlkFile.getParentFile(), Thread.currentThread(), estimateBlockLen);
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
    v.reserveSpaceForRbw(estimateBlockLen - replicaInfo.getNumBytes());
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
  public synchronized ReplicaInPipeline recoverAppend(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    // change the replica's state/gs etc.
    if (replicaInfo.getState() == ReplicaState.FINALIZED ) {
      return append(b.getBlockPoolId(), (FinalizedReplica) replicaInfo, newGS, 
          b.getNumBytes());
    } else { //RBW
      bumpReplicaGS(replicaInfo, newGS);
      return (ReplicaBeingWritten)replicaInfo;
    }
  }

  @Override // FsDatasetSpi
  public synchronized String recoverClose(ExtendedBlock b, long newGS,
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
    return replicaInfo.getStorageUuid();
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
  public synchronized ReplicaInPipeline createRbw(StorageType storageType,
      ExtendedBlock b, boolean allowLazyPersist) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
        b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists in state " + replicaInfo.getState() +
      " and thus cannot be created.");
    }
    // create a new block
    FsVolumeImpl v;
    while (true) {
      try {
        if (allowLazyPersist) {
          // First try to place the block on a transient volume.
          v = volumes.getNextTransientVolume(b.getNumBytes());
          datanode.getMetrics().incrRamDiskBlocksWrite();
        } else {
          v = volumes.getNextVolume(storageType, b.getNumBytes());
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
    // create an rbw file to hold block in the designated volume
    File f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
    ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile(), b.getNumBytes());
    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);

    return newReplicaInfo;
  }
  
  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd)
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

    // Truncate the potentially corrupt portion.
    // If the source was client and the last node in the pipeline was lost,
    // any corrupt data written after the acked length can go unnoticed. 
    if (numBytes > bytesAcked) {
      final File replicafile = rbw.getBlockFile();
      truncateBlock(replicafile, rbw.getMetaFile(), numBytes, bytesAcked);
      rbw.setNumBytes(bytesAcked);
      rbw.setLastChecksumAndDataLen(bytesAcked, null);
    }

    // bump the replica's generation stamp to newGS
    bumpReplicaGS(rbw, newGS);
    
    return rbw;
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
    // overwrite the RBW in the volume map
    volumeMap.add(b.getBlockPoolId(), rbw);
    return rbw;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline createTemporary(StorageType storageType,
      ExtendedBlock b) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
    if (replicaInfo != null) {
      if (replicaInfo.getGenerationStamp() < b.getGenerationStamp()
          && replicaInfo instanceof ReplicaInPipeline) {
        // Stop the previous writer
        ((ReplicaInPipeline)replicaInfo)
                      .stopWriter(datanode.getDnConf().getXceiverStopTimeout());
        invalidate(b.getBlockPoolId(), new Block[]{replicaInfo});
      } else {
        throw new ReplicaAlreadyExistsException("Block " + b +
            " already exists in state " + replicaInfo.getState() +
            " and thus cannot be created.");
      }
    }
    
    FsVolumeImpl v = volumes.getNextVolume(storageType, b.getNumBytes());
    // create a temporary file to hold block in the designated volume
    File f = v.createTmpFile(b.getBlockPoolId(), b.getLocalBlock());
    ReplicaInPipeline newReplicaInfo = new ReplicaInPipeline(b.getBlockId(), 
        b.getGenerationStamp(), v, f.getParentFile(), 0);
    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
    return newReplicaInfo;
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
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
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
  
  private synchronized FinalizedReplica finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    FinalizedReplica newReplicaInfo = null;
    if (replicaInfo.getState() == ReplicaState.RUR &&
       ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica().getState() == 
         ReplicaState.FINALIZED) {
      newReplicaInfo = (FinalizedReplica)
             ((ReplicaUnderRecovery)replicaInfo).getOriginalReplica();
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

    Map<String, ArrayList<ReplicaInfo>> finalized =
        new HashMap<String, ArrayList<ReplicaInfo>>();
    Map<String, ArrayList<ReplicaInfo>> uc =
        new HashMap<String, ArrayList<ReplicaInfo>>();

    for (FsVolumeSpi v : volumes.volumes) {
      finalized.put(v.getStorageID(), new ArrayList<ReplicaInfo>());
      uc.put(v.getStorageID(), new ArrayList<ReplicaInfo>());
    }

    synchronized(this) {
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        switch(b.getState()) {
          case FINALIZED:
            finalized.get(b.getVolume().getStorageID()).add(b);
            break;
          case RBW:
          case RWR:
            uc.get(b.getVolume().getStorageID()).add(b);
            break;
          case RUR:
            ReplicaUnderRecovery rur = (ReplicaUnderRecovery)b;
            uc.get(rur.getVolume().getStorageID()).add(rur.getOriginalReplica());
            break;
          case TEMPORARY:
            break;
          default:
            assert false : "Illegal ReplicaInfo state.";
        }
      }
    }

    for (FsVolumeSpi v : volumes.volumes) {
      ArrayList<ReplicaInfo> finalizedList = finalized.get(v.getStorageID());
      ArrayList<ReplicaInfo> ucList = uc.get(v.getStorageID());
      blockReportsMap.put(((FsVolumeImpl) v).toDatanodeStorage(),
                          new BlockListAsLongs(finalizedList, ucList));
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
    final ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    return replicaInfo != null
        && replicaInfo.getState() == state
        && replicaInfo.getBlockFile().exists();
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
        if (f == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ": File not found, volume=" + v);
          continue;
        }
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
        volumeMap.remove(bpid, invalidBlks[i]);
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
      asyncDiskService.deleteAsync(v, f,
          FsDatasetUtil.getMetaFile(f, invalidBlks[i].getGenerationStamp()),
          new ExtendedBlock(bpid, invalidBlks[i]),
          dataStorage.getTrashDirectoryForBlockFile(bpid, f));
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
    // The short-circuit registry is null in the unit tests, because the
    // datanode is mock object.
    if (datanode.getShortCircuitRegistry() != null) {
      datanode.getShortCircuitRegistry().processBlockInvalidation(
          new ExtendedBlockId(block.getBlockId(), bpid));

      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, block.getBlockId());
    }

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
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  @Override // FsDatasetSpi
  public void checkDataDir() throws DiskErrorException {
    long totalBlocks=0, removedBlocks=0;
    List<FsVolumeImpl> failedVols =  volumes.checkDirs();
    
    // If there no failed volumes return
    if (failedVols == null) { 
      return;
    }
    
    // Otherwise remove blocks for the failed volumes
    long mlsec = Time.now();
    synchronized (this) {
      for (FsVolumeImpl fv: failedVols) {
        for (String bpid : fv.getBlockPoolList()) {
          Iterator<ReplicaInfo> ib = volumeMap.replicas(bpid).iterator();
          while(ib.hasNext()) {
            ReplicaInfo b = ib.next();
            totalBlocks++;
            // check if the volume block belongs to still valid
            if(b.getVolume() == fv) {
              LOG.warn("Removing replica " + bpid + ":" + b.getBlockId()
                  + " on failed volume " + fv.getCurrentDir().getAbsolutePath());
              ib.remove();
              removedBlocks++;
            }
          }
        }
      }
    } // end of sync
    mlsec = Time.now() - mlsec;
    LOG.warn("Removed " + removedBlocks + " out of " + totalBlocks +
        "(took " + mlsec + " millisecs)");

    // report the error
    StringBuilder sb = new StringBuilder();
    for (FsVolumeImpl fv : failedVols) {
      sb.append(fv.getCurrentDir().getAbsolutePath() + ";");
    }
    throw new DiskErrorException("DataNode failed volumes:" + sb);
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
          final DataBlockScanner blockScanner = datanode.getBlockScanner();
          if (blockScanner != null) {
            blockScanner.deleteBlock(bpid, new Block(blockId));
          }
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
        final DataBlockScanner blockScanner = datanode.getBlockScanner();
        if (!vol.isTransientStorage()) {
          if (blockScanner != null) {
            blockScanner.addBlock(new ExtendedBlock(bpid, diskBlockInfo));
          }
        } else {
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
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock));  
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
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " getBytesOnDisk() < getVisibleLength(), rip=" + rip);
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
    }
    return rur.createInfo();
  }

  @Override // FsDatasetSpi
  public synchronized String updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
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
        .getBlockPoolId(), (ReplicaUnderRecovery) replica, recoveryId, newlength);
    assert finalized.getBlockId() == oldBlock.getBlockId()
        && finalized.getGenerationStamp() == recoveryId
        && finalized.getNumBytes() == newlength
        : "Replica information mismatched: oldBlock=" + oldBlock
            + ", recoveryId=" + recoveryId + ", newlength=" + newlength
            + ", finalized=" + finalized;

    //check replica files after update
    checkReplicaFiles(finalized);

    //return storage ID
    return getVolume(new ExtendedBlock(bpid, finalized)).getStorageID();
  }

  private FinalizedReplica updateReplicaUnderRecovery(
                                          String bpid,
                                          ReplicaUnderRecovery rur,
                                          long recoveryId,
                                          long newlength) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId
          + ", rur=" + rur);
    }

    // bump rur's GS to be recovery id
    bumpReplicaGS(rur, recoveryId);

    //update length
    final File replicafile = rur.getBlockFile();
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {
      rur.unlinkBlock(1);
      truncateBlock(replicafile, rur.getMetaFile(), rur.getNumBytes(), newlength);
      // update RUR with the new length
      rur.setNumBytes(newlength);
   }

    // finalize the block
    return finalizeReplica(bpid, rur);
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
    final long usedSpace;
    final long freeSpace;
    final long reservedSpace;

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FsVolumeImpl volume : volumes.volumes) {
      long used = 0;
      long free = 0;
      try {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
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
    if (!force) {
      for (FsVolumeImpl volume : volumes.volumes) {
        if (!volume.isBPDirEmpty(bpid)) {
          LOG.warn(bpid + " has some block files, cannot delete unless forced");
          throw new IOException("Cannot delete block pool, "
              + "it contains some block files");
        }
      }
    }
    for (FsVolumeImpl volume : volumes.volumes) {
      volume.deleteBPDirectories(bpid, force);
    }
  }
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    File datafile = getBlockFile(block);
    File metafile = FsDatasetUtil.getMetaFile(datafile, block.getGenerationStamp());
    BlockLocalPathInfo info = new BlockLocalPathInfo(block,
        datafile.getAbsolutePath(), metafile.getAbsolutePath());
    return info;
  }

  @Override // FsDatasetSpi
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String poolId,
      long[] blockIds) throws IOException {
    // List of VolumeIds, one per volume on the datanode
    List<byte[]> blocksVolumeIds = new ArrayList<byte[]>(volumes.volumes.size());
    // List of indexes into the list of VolumeIds, pointing at the VolumeId of
    // the volume that the block is on
    List<Integer> blocksVolumeIndexes = new ArrayList<Integer>(blockIds.length);
    // Initialize the list of VolumeIds simply by enumerating the volumes
    for (int i = 0; i < volumes.volumes.size(); i++) {
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
        for (FsVolumeImpl volume : volumes.volumes) {
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
  public void restoreTrash(String bpid) {
    dataStorage.restoreTrash(bpid);
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
  public RollingLogs createRollingLogs(String bpid, String prefix
      ) throws IOException {
    String dir = null;
    final List<FsVolumeImpl> volumes = getVolumes();
    for (FsVolumeImpl vol : volumes) {
      String bpDir = vol.getPath(bpid);
      if (RollingLogsImpl.isFilePresent(bpDir, prefix)) {
        dir = bpDir;
        break;
      }
    }
    if (dir == null) {
      dir = volumes.get(0).getPath(bpid);
    }
    return new RollingLogsImpl(dir, prefix);
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
    boolean ramDiskConfigured = ramDiskConfigured();
    for (FsVolumeImpl v: getVolumes()){
      // Skip transient volumes
      if (v.isTransientStorage()) {
        continue;
      }

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
              targetVolume = volumes.getNextVolume(
                  StorageType.DEFAULT, replicaInfo.getNumBytes());

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
                  targetVolume);
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
      for (FsVolumeImpl v : volumes.volumes) {
        if (v.isTransientStorage()) {
          capacity += v.getCapacity();
          free += v.getAvailable();
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

        // Before deleting the files from transient storage we must notify the
        // NN that the files are on the new storage. Else a blockReport from
        // the transient storage might cause the NN to think the blocks are lost.
        // Replicas must be evicted from client short-circuit caches, because the
        // storage will no longer be transient, and thus will require validating
        // checksum.  This also stops a client from holding file descriptors,
        // which would prevent the OS from reclaiming the memory.
        ExtendedBlock extendedBlock =
            new ExtendedBlock(bpid, newReplicaInfo);
        datanode.getShortCircuitRegistry().processBlockInvalidation(
            ExtendedBlockId.fromExtendedBlock(extendedBlock));
        datanode.notifyNamenodeReceivedBlock(
            extendedBlock, null, newReplicaInfo.getStorageUuid());

        // Remove the old replicas from transient storage.
        if (blockFile.delete() || !blockFile.exists()) {
          ((FsVolumeImpl) replicaInfo.getVolume()).decDfsUsed(bpid, blockFileUsed);
          if (metaFile.delete() || !metaFile.exists()) {
            ((FsVolumeImpl) replicaInfo.getVolume()).decDfsUsed(bpid, metaFileUsed);
          }
        }

        // If deletion failed then the directory scanner will cleanup the blocks
        // eventually.
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
}

