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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.Shell.getMemlockLimit;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.slf4j.event.Level;

public abstract class LazyPersistTestCase {
  static final byte LAZY_PERSIST_POLICY_ID = (byte) 15;

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.DEBUG);
    GenericTestUtils.setLogLevel(FsDatasetImpl.LOG, Level.DEBUG);
  }

  protected static final Logger LOG =
      LoggerFactory.getLogger(LazyPersistTestCase.class);
  protected static final int BLOCK_SIZE = 5 * 1024 * 1024;
  protected static final int BUFFER_LENGTH = 4096;
  protected static final int LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC = 3;
  protected static final int LAZY_WRITER_INTERVAL_SEC = 1;
  protected static final short REPL_FACTOR = 1;
  private static final String JMX_RAM_DISK_METRICS_PATTERN = "^RamDisk";
  private static final String JMX_SERVICE_NAME = "DataNode";
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MS = 500;
  private static final long WAIT_FOR_FBR_MS =
      TimeUnit.SECONDS.toMillis(10);
  private static final long WAIT_FOR_STORAGE_TYPES_MS =
      TimeUnit.SECONDS.toMillis(30);
  private static final long WAIT_FOR_ASYNC_DELETE_MS =
      TimeUnit.SECONDS.toMillis(10);
  private static final long WAIT_FOR_DN_SHUTDOWN_MS =
      TimeUnit.SECONDS.toMillis(30);
  private static final long WAIT_FOR_REDUNDANCY_MS =
      TimeUnit.SECONDS
          .toMillis(2 * DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT);
  private static final long WAIT_FOR_LAZY_SCRUBBER_MS =
      TimeUnit.SECONDS.toMillis(2 * LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
  private static final long WAIT_POLL_INTERVAL_MS = 10;
  private static final long WAIT_POLL_INTERVAL_LARGE_MS = 20;

  protected final long osPageSize =
      NativeIO.POSIX.getCacheManipulator().getOperatingSystemPageSize();

  protected MiniDFSCluster cluster;
  protected DistributedFileSystem fs;
  protected DFSClient client;
  protected JMXGet jmx;
  protected TemporarySocketDirectory sockDir;

  @After
  public void shutDownCluster() throws Exception {

    // Dump all RamDisk JMX metrics before shutdown the cluster
    printRamDiskJMXMetrics();

    if (fs != null) {
      fs.close();
      fs = null;
      client = null;
    }

    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
      cluster = null;
    }

    if (jmx != null) {
      jmx = null;
    }

    IOUtils.closeQuietly(sockDir);
    sockDir = null;
  }

  @Rule
  public Timeout timeout = Timeout.seconds(300);

  protected final LocatedBlocks ensureFileReplicasOnStorageType(
      Path path, StorageType storageType)
      throws IOException, TimeoutException, InterruptedException {
    // Ensure that returned block locations returned are correct!
    LOG.info("Ensure path: {} is on StorageType: {}", path, storageType);
    assertThat(fs.exists(path), is(true));
    long fileLength = client.getFileInfo(path.toString()).getLen();

    GenericTestUtils.waitFor(() -> {
      try {
        LocatedBlocks locatedBlocks =
            client.getLocatedBlocks(path.toString(), 0, fileLength);
        for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
          if (locatedBlock.getStorageTypes()[0] != storageType) {
            return false;
          }
        }
        return true;
      } catch (IOException ioe) {
        LOG.warn("Exception got in ensureFileReplicasOnStorageType()", ioe);
        return false;
      }
    }, WAIT_POLL_INTERVAL_MS, WAIT_FOR_STORAGE_TYPES_MS);

    return client.getLocatedBlocks(path.toString(), 0, fileLength);
  }

  /**
   * Make sure at least one non-transient volume has a saved copy of the
   * replica. An infinite loop is used to ensure the async lazy persist tasks
   * are completely done before verification.
   * Caller of this method expects either a successful pass or timeout failure.
   *
   * @param locatedBlocks the collection of blocks and their locations.
   * @throws IOException for aut-closeable resources.
   * @throws InterruptedException if the thread is interrupted.
   * @throws TimeoutException if {@link #WAIT_FOR_STORAGE_TYPES_MS} expires
   *                          before we find a persisted copy for each located
   *                          block.
   */
  protected final void ensureLazyPersistBlocksAreSaved(
      final LocatedBlocks locatedBlocks)
      throws IOException, InterruptedException, TimeoutException {
    final String bpid = cluster.getNamesystem().getBlockPoolId();

    final Set<Long> persistedBlockIds = new HashSet<Long>();
    // We should find a persisted copy for each located block.
    try (FsDatasetSpi.FsVolumeReferences volumes =
        cluster.getDataNodes().get(0).getFSDataset().getFsVolumeReferences()) {
      GenericTestUtils.waitFor(() -> {
        for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
          for (FsVolumeSpi v : volumes) {
            if (v.isTransientStorage()) {
              continue;
            }
            FsVolumeImpl volume = (FsVolumeImpl) v;
            File lazyPersistDir;
            try {
              lazyPersistDir =
                  volume.getBlockPoolSlice(bpid).getLazypersistDir();
            } catch (IOException ioe) {
              return false;
            }
            long blockId = lb.getBlock().getBlockId();
            File targetDir =
                DatanodeUtil.idToBlockDir(lazyPersistDir, blockId);
            File blockFile = new File(targetDir, lb.getBlock().getBlockName());
            if (blockFile.exists()) {
              // Found a persisted copy for this block and added to the Set.
              persistedBlockIds.add(blockId);
            }
          }
        }
        return (persistedBlockIds.size() ==
            locatedBlocks.getLocatedBlocks().size());
      }, WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_STORAGE_TYPES_MS);
    }
  }

  protected final void makeRandomTestFile(Path path, long length,
      boolean isLazyPersist, long seed) throws IOException {
    DFSTestUtil.createFile(fs, path, isLazyPersist, BUFFER_LENGTH, length,
                           BLOCK_SIZE, REPL_FACTOR, seed, true);
  }

  protected final void makeTestFile(Path path, long length,
      boolean isLazyPersist) throws IOException {

    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);

    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    FSDataOutputStream fos = null;
    try {
      fos =
          fs.create(path,
              FsPermission.getFileDefault(),
              createFlags,
              BUFFER_LENGTH,
              REPL_FACTOR,
              BLOCK_SIZE,
              null);

      // Allocate a block.
      byte[] buffer = new byte[BUFFER_LENGTH];
      for (int bytesWritten = 0; bytesWritten < length; ) {
        fos.write(buffer, 0, buffer.length);
        bytesWritten += buffer.length;
      }
      if (length > 0) {
        fos.hsync();
      }
    } finally {
      IOUtils.closeQuietly(fos);
    }
  }

  /**
   * If ramDiskStorageLimit is >=0, then RAM_DISK capacity is artificially
   * capped. If ramDiskStorageLimit < 0 then it is ignored.
   */
  protected final void startUpCluster(
      int numDatanodes,
      boolean hasTransientStorage,
      StorageType[] storageTypes,
      int ramDiskReplicaCapacity,
      long ramDiskStorageLimit,
      long maxLockedMemory,
      boolean useSCR,
      boolean useLegacyBlockReaderLocal,
      boolean disableScrubber) throws IOException {

    initCacheManipulator();
    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    if (disableScrubber) {
      conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC, 0);
    } else {
      conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
          LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    }
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        HEARTBEAT_RECHECK_INTERVAL_MS);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY, 1);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, maxLockedMemory);

    if (useSCR) {
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
      // Do not share a client context across tests.
      conf.set(HdfsClientConfigKeys.DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      conf.set(DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
          UserGroupInformation.getCurrentUser().getShortUserName());
      if (useLegacyBlockReaderLocal) {
        conf.setBoolean(
            HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
      } else {
        sockDir = new TemporarySocketDirectory();
        conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
            this.getClass().getSimpleName() + "._PORT.sock").getAbsolutePath());
      }
    }

    Preconditions.checkState(
        ramDiskReplicaCapacity < 0 || ramDiskStorageLimit < 0,
        "Cannot specify non-default values for both ramDiskReplicaCapacity "
            + "and ramDiskStorageLimit");

    long[] capacities;
    if (hasTransientStorage && ramDiskReplicaCapacity >= 0) {
      // Convert replica count to byte count, add some delta for .meta and
      // VERSION files.
      ramDiskStorageLimit = ((long) ramDiskReplicaCapacity * BLOCK_SIZE) +
          (BLOCK_SIZE - 1);
    }
    capacities = new long[] { ramDiskStorageLimit, -1 };

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(numDatanodes)
        .storageCapacities(capacities)
        .storageTypes(storageTypes != null ? storageTypes :
                          (hasTransientStorage ? new StorageType[]{RAM_DISK, DEFAULT} : null))
        .build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    client = fs.getClient();
    try {
      jmx = initJMX();
    } catch (Exception e) {
      fail("Failed initialize JMX for testing: " + e);
    }
    LOG.info("Cluster startup complete");
  }

  /**
   * Use a dummy cache manipulator for testing.
   */
  public static void initCacheManipulator() {
    NativeIO.POSIX.setCacheManipulator(new NativeIO.POSIX.CacheManipulator() {
      @Override
      public void mlock(String identifier,
                        ByteBuffer mmap, long length) throws IOException {
        LOG.info("LazyPersistTestCase: faking mlock of {} bytes.", identifier);
      }

      @Override
      public long getMemlockLimit() {
        LOG.info("LazyPersistTestCase: fake return {}", Long.MAX_VALUE);
        return Long.MAX_VALUE;
      }

      @Override
      public boolean verifyCanMlock() {
        LOG.info("LazyPersistTestCase: fake return {}", true);
        return true;
      }
    });
  }

  ClusterWithRamDiskBuilder getClusterBuilder() {
    return new ClusterWithRamDiskBuilder();
  }

  /**
   * Builder class that allows controlling RAM disk-specific properties for a
   * MiniDFSCluster.
   */
  class ClusterWithRamDiskBuilder {
    public ClusterWithRamDiskBuilder setNumDatanodes(
        int numDatanodes) {
      this.numDatanodes = numDatanodes;
      return this;
    }

    public ClusterWithRamDiskBuilder setStorageTypes(
        StorageType[] storageTypes) {
      this.storageTypes = storageTypes;
      return this;
    }

    public ClusterWithRamDiskBuilder setRamDiskReplicaCapacity(
        int ramDiskReplicaCapacity) {
      this.ramDiskReplicaCapacity = ramDiskReplicaCapacity;
      return this;
    }

    public ClusterWithRamDiskBuilder setRamDiskStorageLimit(
        long ramDiskStorageLimit) {
      this.ramDiskStorageLimit = ramDiskStorageLimit;
      return this;
    }

    public ClusterWithRamDiskBuilder setMaxLockedMemory(long maxLockedMemory) {
      this.maxLockedMemory = maxLockedMemory;
      return this;
    }

    public ClusterWithRamDiskBuilder setUseScr(boolean useScr) {
      this.useScr = useScr;
      return this;
    }

    public ClusterWithRamDiskBuilder setHasTransientStorage(
        boolean hasTransientStorage) {
      this.hasTransientStorage = hasTransientStorage;
      return this;
    }

    public ClusterWithRamDiskBuilder setUseLegacyBlockReaderLocal(
        boolean useLegacyBlockReaderLocal) {
      this.useLegacyBlockReaderLocal = useLegacyBlockReaderLocal;
      return this;
    }

    public ClusterWithRamDiskBuilder disableScrubber() {
      this.disableScrubber = true;
      return this;
    }

    public void build() throws IOException {
      LazyPersistTestCase.this.startUpCluster(
          numDatanodes, hasTransientStorage, storageTypes,
          ramDiskReplicaCapacity,
          ramDiskStorageLimit, maxLockedMemory, useScr,
          useLegacyBlockReaderLocal,
          disableScrubber);
    }

    private int numDatanodes = REPL_FACTOR;
    private StorageType[] storageTypes = null;
    private int ramDiskReplicaCapacity = -1;
    private long ramDiskStorageLimit = -1;
    private long maxLockedMemory = getMemlockLimit(Long.MAX_VALUE);
    private boolean hasTransientStorage = true;
    private boolean useScr = false;
    private boolean useLegacyBlockReaderLocal = false;
    private boolean disableScrubber=false;
  }

  /**
   * Forces a full blockreport on all the datatanodes. The call blocks waiting
   * for all blockreports to be received by the namenode.
   *
   * @throws IOException if an exception is thrown while getting the datanode
   *                     descriptors or triggering the blockreports.
   * @throws InterruptedException if the thread receives an interrupt.
   * @throws TimeoutException if the reports are not received by
   *                          {@link #WAIT_FOR_FBR_MS}.
   */
  protected final void triggerBlockReport()
      throws InterruptedException, TimeoutException, IOException {
    // Trigger block report to NN
    final Map<DatanodeStorageInfo, Integer> reportCountsBefore =
        new HashMap<>();
    final FSNamesystem fsn = cluster.getNamesystem();
    for (DataNode dn : cluster.getDataNodes()) {
      final DatanodeDescriptor dnd =
          NameNodeAdapter.getDatanode(fsn, dn.getDatanodeId());
      final DatanodeStorageInfo storage = dnd.getStorageInfos()[0];
      reportCountsBefore.put(storage, storage.getBlockReportCount());
      DataNodeTestUtils.triggerBlockReport(dn);
    }
    // wait for block reports to be received.
    GenericTestUtils.waitFor(() -> {
      for (Entry<DatanodeStorageInfo, Integer> reportEntry :
          reportCountsBefore.entrySet()) {
        final DatanodeStorageInfo dnStorageInfo = reportEntry.getKey();
        final int cntBefore = reportEntry.getValue();
        final int currentCnt = dnStorageInfo.getBlockReportCount();
        if (cntBefore == currentCnt) {
          // Same count means no report has been received.
          return false;
        }
      }
      // If we reach here, then all the block reports have been received.
      return true;
    }, WAIT_POLL_INTERVAL_MS, WAIT_FOR_FBR_MS);
  }

  protected final boolean verifyBlockDeletedFromDir(File dir,
      LocatedBlocks locatedBlocks) {

    for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
      File targetDir =
        DatanodeUtil.idToBlockDir(dir, lb.getBlock().getBlockId());

      File blockFile = new File(targetDir, lb.getBlock().getBlockName());
      if (blockFile.exists()) {
        return false;
      }
      File metaFile = new File(targetDir,
        DatanodeUtil.getMetaName(lb.getBlock().getBlockName(),
          lb.getBlock().getGenerationStamp()));
      if (metaFile.exists()) {
        return false;
      }
    }
    return true;
  }

  protected final boolean verifyDeletedBlocks(final LocatedBlocks locatedBlocks)
      throws Exception {

    LOG.info("Verifying replica has no saved copy after deletion.");
    triggerBlockReport();
    final DataNode dn = cluster.getDataNodes().get(0);

    GenericTestUtils.waitFor(() -> {
      for (DataNode dn1 : cluster.getDataNodes()) {
        if (cluster.getFsDatasetTestUtils(dn1).getPendingAsyncDeletions()
            > 0) {
          return false;
        }
      }
      return true;
    }, WAIT_POLL_INTERVAL_MS, WAIT_FOR_ASYNC_DELETE_MS);

    final String bpid = cluster.getNamesystem().getBlockPoolId();
    final FsDatasetSpi<?> dataset = dn.getFSDataset();
    // Make sure deleted replica does not have a copy on either finalized dir of
    // transient volume or finalized dir of non-transient volume.
    // We need to wait until the asyn deletion is scheduled.
    try (FsDatasetSpi.FsVolumeReferences volumes =
        dataset.getFsVolumeReferences()) {
      GenericTestUtils.waitFor(() -> {
        try {
          for (FsVolumeSpi vol : volumes) {
            FsVolumeImpl volume = (FsVolumeImpl) vol;
            File targetDir = (volume.isTransientStorage()) ?
                volume.getBlockPoolSlice(bpid).getFinalizedDir() :
                volume.getBlockPoolSlice(bpid).getLazypersistDir();
            if (!verifyBlockDeletedFromDir(targetDir, locatedBlocks)) {
              return false;
            }
          }
          return true;
        } catch (IOException ie) {
          return false;
        }
      }, WAIT_POLL_INTERVAL_MS, WAIT_FOR_ASYNC_DELETE_MS);
    }
    return true;
  }

  protected final void verifyRamDiskJMXMetric(String metricName,
      long expectedValue) throws Exception {
    waitForMetric(metricName, (int)expectedValue);
    assertEquals(expectedValue, Integer.parseInt(jmx.getValue(metricName)));
  }

  protected final boolean verifyReadRandomFile(
      Path path, int fileLength, int seed) throws IOException {
    byte contents[] = DFSTestUtil.readFileBuffer(fs, path);
    byte expected[] = DFSTestUtil.
      calculateFileContentsFromSeed(seed, fileLength);
    return Arrays.equals(contents, expected);
  }

  private JMXGet initJMX() throws Exception {
    JMXGet jmx = new JMXGet();
    jmx.setService(JMX_SERVICE_NAME);
    jmx.init();
    return jmx;
  }

  private void printRamDiskJMXMetrics() {
    try {
      if (jmx != null) {
        jmx.printAllMatchedAttributes(JMX_RAM_DISK_METRICS_PATTERN);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void waitForMetric(final String metricName, final int expectedValue)
      throws TimeoutException, InterruptedException {
    DFSTestUtil.waitForMetric(jmx, metricName, expectedValue);
  }

  protected void triggerEviction(final DataNode dn) {
    FsDatasetImpl fsDataset = (FsDatasetImpl) dn.getFSDataset();
    fsDataset.evictLazyPersistBlocks(Long.MAX_VALUE); // Run one eviction cycle.
  }

  /**
   * Shutdown all datanodes in {@link #cluster}. The call blocks for
   * {@link #WAIT_FOR_DN_SHUTDOWN_MS} until client report has no datanode
   * labeled as live.
   *
   * @throws TimeoutException if {@link #WAIT_FOR_DN_SHUTDOWN_MS} expires with
   * at least one datanode still alive.
   * @throws InterruptedException if the thread receives an interrupt.
   */
  protected void shutdownDataNodes()
      throws TimeoutException, InterruptedException {
    cluster.shutdownDataNodes();
    GenericTestUtils.waitFor(() -> {
      try {
        DatanodeInfo[] info = client.datanodeReport(
            HdfsConstants.DatanodeReportType.LIVE);
        return info.length == 0;
      } catch (IOException e) {
        return false;
      }
    }, WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_DN_SHUTDOWN_MS);
  }

  /**
   * Blocks for {@link #WAIT_FOR_REDUNDANCY_MS}  waiting for corrupt block count
   * to reach a certain count.
   *
   * @param corruptCnt representing the number of corrupt blocks before
   *                   resuming.
   * @throws TimeoutException if {@link #WAIT_FOR_REDUNDANCY_MS} expires with
   *                          corrupt count does not meet the criteria.
   * @throws InterruptedException if the thread receives an interrupt.
   */
  protected void waitForCorruptBlock(final long corruptCnt)
      throws TimeoutException, InterruptedException {
    // wait for the redundancy monitor to mark the file as corrupt.
    GenericTestUtils.waitFor(() -> {
      Iterator<BlockInfo> bInfoIter = cluster.getNameNode()
          .getNamesystem().getBlockManager().getCorruptReplicaBlockIterator();
      int count = 0;
      while (bInfoIter.hasNext()) {
        bInfoIter.next();
        count++;
      }
      return corruptCnt == count;
    }, 2 * WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_REDUNDANCY_MS);
  }

  /**
   * Blocks until {@link FSNamesystem#lazyPersistFileScrubber} daemon completes
   * a full iteration.
   *
   * @throws InterruptedException if the thread receives an interrupt.
   * @throws TimeoutException
   *                         {@link FSNamesystem#getLazyPersistFileScrubberTS()}
   *                         does not update the timestamp by
   *                         {@link #WAIT_FOR_LAZY_SCRUBBER_MS}.
   */
  protected void waitForScrubberCycle()
      throws TimeoutException, InterruptedException {
    // wait for the redundancy monitor to mark the file as corrupt.
    final FSNamesystem fsn = cluster.getNamesystem();
    final long lastTimeStamp = fsn.getLazyPersistFileScrubberTS();
    if (lastTimeStamp == -1) { // scrubber is disabled
      return;
    }
    GenericTestUtils.waitFor(
        () -> lastTimeStamp != fsn.getLazyPersistFileScrubberTS(),
        2 * WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_LAZY_SCRUBBER_MS);
  }

  /**
   * Blocks until {@link BlockManager#RedundancyMonitor} daemon completes
   * a full iteration.
   *
   * @throws InterruptedException if the thread receives an interrupt.
   * @throws TimeoutException {@link BlockManager#getLastRedundancyMonitorTS()}
   *                          does not update the timestamp by
   *                          {@link #WAIT_FOR_REDUNDANCY_MS}.
   */
  protected void waitForRedundancyMonitorCycle()
      throws TimeoutException, InterruptedException {
    // wait for the redundancy monitor to mark the file as corrupt.
    final BlockManager bm = cluster.getNamesystem().getBlockManager();
    final long lastRedundancyTS =
        bm.getLastRedundancyMonitorTS();

    GenericTestUtils.waitFor(
        () -> lastRedundancyTS != bm.getLastRedundancyMonitorTS(),
        2 * WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_REDUNDANCY_MS);
  }

  /**
   * Blocks until {@link BlockManager#lowRedundancyBlocksCount} reaches a
   * certain value.
   *
   * @throws InterruptedException if the thread receives an interrupt.
   * @throws TimeoutException {@link BlockManager#getLowRedundancyBlocksCount()}
   *                          does not update the count by
   *                          {@link #WAIT_FOR_REDUNDANCY_MS}.
   */
  protected void waitForLowRedundancyCount(final long cnt)
      throws TimeoutException, InterruptedException {
    final BlockManager bm = cluster.getNamesystem().getBlockManager();

    GenericTestUtils.waitFor(() -> cnt == bm.getLowRedundancyBlocksCount(),
        2 * WAIT_POLL_INTERVAL_LARGE_MS, WAIT_FOR_REDUNDANCY_MS);
  }

  /**
   * Blocks until the file status changes on the filesystem.
   *
   * @param path of the file to be checked.
   * @param expected whether a file should exist or not.
   * @throws TimeoutException if the file status does not meet the expected by
   *                          {@link #WAIT_FOR_STORAGE_TYPES_MS}.
   * @throws InterruptedException if the thread receives an interrupt.
   */
  protected void waitForFile(final Path path, final boolean expected)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        return expected == fs.exists(path);
      } catch (IOException e) {
        return false;
      }
    }, WAIT_POLL_INTERVAL_MS, WAIT_FOR_STORAGE_TYPES_MS);
  }
}
