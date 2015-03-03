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

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class LazyPersistTestCase {

  static {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(FsDatasetImpl.LOG, Level.ALL);
  }

  protected static final int BLOCK_SIZE = 5 * 1024 * 1024;
  protected static final int BUFFER_LENGTH = 4096;
  protected static final int EVICTION_LOW_WATERMARK = 1;
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MSEC = 500;
  private static final String JMX_RAM_DISK_METRICS_PATTERN = "^RamDisk";
  private static final String JMX_SERVICE_NAME = "DataNode";
  protected static final int LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC = 3;
  protected static final int LAZY_WRITER_INTERVAL_SEC = 1;
  protected static final Log LOG = LogFactory.getLog(LazyPersistTestCase.class);
  protected static final short REPL_FACTOR = 1;

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
  public Timeout timeout = new Timeout(300000);

  protected final LocatedBlocks ensureFileReplicasOnStorageType(
      Path path, StorageType storageType) throws IOException {
    // Ensure that returned block locations returned are correct!
    LOG.info("Ensure path: " + path + " is on StorageType: " + storageType);
    assertThat(fs.exists(path), is(true));
    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
        client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      assertThat(locatedBlock.getStorageTypes()[0], is(storageType));
    }
    return locatedBlocks;
  }

  /**
   * Make sure at least one non-transient volume has a saved copy of the replica.
   * An infinite loop is used to ensure the async lazy persist tasks are completely
   * done before verification. Caller of ensureLazyPersistBlocksAreSaved expects
   * either a successful pass or timeout failure.
   */
  protected final void ensureLazyPersistBlocksAreSaved(
      LocatedBlocks locatedBlocks) throws IOException, InterruptedException {
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    List<? extends FsVolumeSpi> volumes =
      cluster.getDataNodes().get(0).getFSDataset().getVolumes();
    final Set<Long> persistedBlockIds = new HashSet<Long>();

    while (persistedBlockIds.size() < locatedBlocks.getLocatedBlocks().size()) {
      // Take 1 second sleep before each verification iteration
      Thread.sleep(1000);

      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        for (FsVolumeSpi v : volumes) {
          if (v.isTransientStorage()) {
            continue;
          }

          FsVolumeImpl volume = (FsVolumeImpl) v;
          File lazyPersistDir = volume.getBlockPoolSlice(bpid).getLazypersistDir();

          long blockId = lb.getBlock().getBlockId();
          File targetDir =
            DatanodeUtil.idToBlockDir(lazyPersistDir, blockId);
          File blockFile = new File(targetDir, lb.getBlock().getBlockName());
          if (blockFile.exists()) {
            // Found a persisted copy for this block and added to the Set
            persistedBlockIds.add(blockId);
          }
        }
      }
    }

    // We should have found a persisted copy for each located block.
    assertThat(persistedBlockIds.size(), is(locatedBlocks.getLocatedBlocks().size()));
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
  protected final void startUpCluster(boolean hasTransientStorage,
                                      final int ramDiskReplicaCapacity,
                                      final boolean useSCR,
                                      final boolean useLegacyBlockReaderLocal)
      throws IOException {

    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
                LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);
    conf.setInt(DFS_DATANODE_RAM_DISK_LOW_WATERMARK_BYTES,
                EVICTION_LOW_WATERMARK * BLOCK_SIZE);

    if (useSCR) {
      conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
      // Do not share a client context across tests.
      conf.set(DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      if (useLegacyBlockReaderLocal) {
        conf.setBoolean(DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
        conf.set(DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
            UserGroupInformation.getCurrentUser().getShortUserName());
      } else {
        sockDir = new TemporarySocketDirectory();
        conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
            this.getClass().getSimpleName() + "._PORT.sock").getAbsolutePath());
      }
    }

    long[] capacities = null;
    if (hasTransientStorage && ramDiskReplicaCapacity >= 0) {
      // Convert replica count to byte count, add some delta for .meta and
      // VERSION files.
      long ramDiskStorageLimit = ((long) ramDiskReplicaCapacity * BLOCK_SIZE) +
          (BLOCK_SIZE - 1);
      capacities = new long[] { ramDiskStorageLimit, -1 };
    }

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(REPL_FACTOR)
        .storageCapacities(capacities)
        .storageTypes(hasTransientStorage ?
            new StorageType[]{ RAM_DISK, DEFAULT } : null)
        .build();
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
   * If ramDiskStorageLimit is >=0, then RAM_DISK capacity is artificially
   * capped. If ramDiskStorageLimit < 0 then it is ignored.
   */
  protected final void startUpCluster(final int numDataNodes,
                                      final StorageType[] storageTypes,
                                      final long ramDiskStorageLimit,
                                      final boolean useSCR)
    throws IOException {

    Configuration conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
      LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
      HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
      LAZY_WRITER_INTERVAL_SEC);

    if (useSCR)
    {
      conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY,useSCR);
      conf.set(DFS_CLIENT_CONTEXT, UUID.randomUUID().toString());
      sockDir = new TemporarySocketDirectory();
      conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
          this.getClass().getSimpleName() + "._PORT.sock").getAbsolutePath());
      conf.set(DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    }

    cluster = new MiniDFSCluster
      .Builder(conf)
      .numDataNodes(numDataNodes)
      .storageTypes(storageTypes != null ?
          storageTypes : new StorageType[] { DEFAULT, DEFAULT })
      .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();

    // Artificially cap the storage capacity of the RAM_DISK volume.
    if (ramDiskStorageLimit >= 0) {
      List<? extends FsVolumeSpi> volumes =
        cluster.getDataNodes().get(0).getFSDataset().getVolumes();

      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageType() == RAM_DISK) {
          ((FsVolumeImpl) volume).setCapacityForTesting(ramDiskStorageLimit);
        }
      }
    }

    LOG.info("Cluster startup complete");
  }

  protected final void startUpCluster(boolean hasTransientStorage,
                                      final int ramDiskReplicaCapacity)
      throws IOException {
    startUpCluster(hasTransientStorage, ramDiskReplicaCapacity, false, false);
  }

  protected final void triggerBlockReport()
      throws IOException, InterruptedException {
    // Trigger block report to NN
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    Thread.sleep(10 * 1000);
  }

  protected final boolean verifyBlockDeletedFromDir(File dir,
      LocatedBlocks locatedBlocks) {

    for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
      File targetDir =
        DatanodeUtil.idToBlockDir(dir, lb.getBlock().getBlockId());

      File blockFile = new File(targetDir, lb.getBlock().getBlockName());
      if (blockFile.exists()) {
        LOG.warn("blockFile: " + blockFile.getAbsolutePath() +
          " exists after deletion.");
        return false;
      }
      File metaFile = new File(targetDir,
        DatanodeUtil.getMetaName(lb.getBlock().getBlockName(),
          lb.getBlock().getGenerationStamp()));
      if (metaFile.exists()) {
        LOG.warn("metaFile: " + metaFile.getAbsolutePath() +
          " exists after deletion.");
        return false;
      }
    }
    return true;
  }

  protected final boolean verifyDeletedBlocks(LocatedBlocks locatedBlocks)
      throws IOException, InterruptedException {

    LOG.info("Verifying replica has no saved copy after deletion.");
    triggerBlockReport();

    while(
      DataNodeTestUtils.getPendingAsyncDeletions(cluster.getDataNodes().get(0))
        > 0L){
      Thread.sleep(1000);
    }

    final String bpid = cluster.getNamesystem().getBlockPoolId();
    List<? extends FsVolumeSpi> volumes =
      cluster.getDataNodes().get(0).getFSDataset().getVolumes();

    // Make sure deleted replica does not have a copy on either finalized dir of
    // transient volume or finalized dir of non-transient volume
    for (FsVolumeSpi v : volumes) {
      FsVolumeImpl volume = (FsVolumeImpl) v;
      File targetDir = (v.isTransientStorage()) ?
          volume.getBlockPoolSlice(bpid).getFinalizedDir() :
          volume.getBlockPoolSlice(bpid).getLazypersistDir();
      if (verifyBlockDeletedFromDir(targetDir, locatedBlocks) == false) {
        return false;
      }
    }
    return true;
  }

  protected final void verifyRamDiskJMXMetric(String metricName,
      long expectedValue) throws Exception {
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
}
