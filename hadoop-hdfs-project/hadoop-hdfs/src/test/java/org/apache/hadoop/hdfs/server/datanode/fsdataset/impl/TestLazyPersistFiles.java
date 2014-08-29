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
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.hdfs.StorageType.DEFAULT;
import static org.apache.hadoop.hdfs.StorageType.RAM_DISK;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;

public class TestLazyPersistFiles {
  public static final Log LOG = LogFactory.getLog(TestLazyPersistFiles.class);

  static {
    ((Log4JLogger) NameNode.blockStateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FsDatasetImpl.LOG).getLogger().setLevel(Level.ALL);
  }

  private static short REPL_FACTOR = 1;
  private static final long BLOCK_SIZE = 10485760;   // 10 MB
  private static final int LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC = 3;
  private static final long HEARTBEAT_INTERVAL_SEC = 1;
  private static final int HEARTBEAT_RECHECK_INTERVAL_MSEC = 500;
  private static final int LAZY_WRITER_INTERVAL_SEC = 1;
  private static final int BUFFER_LENGTH = 4096;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;
  private Configuration conf;

  @After
  public void shutDownCluster() throws IOException {
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
  }

  @Test (timeout=300000)
  public void testFlagNotSetByDefault() throws IOException {
    startUpCluster(REPL_FACTOR, null, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, false);
    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(false));
  }

  @Test (timeout=300000)
  public void testFlagPropagation() throws IOException {
    startUpCluster(REPL_FACTOR, null, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testFlagPersistenceInEditLog() throws IOException {
    startUpCluster(REPL_FACTOR, null, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, 0, true);
    cluster.restartNameNode(true);

    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testFlagPersistenceInFsImage() throws IOException {
    startUpCluster(REPL_FACTOR, null, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");
    FSDataOutputStream fos = null;

    makeTestFile(path, 0, true);
    // checkpoint
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);

    // Stat the file and check that the lazyPersist flag is returned back.
    HdfsFileStatus status = client.getFileInfo(path.toString());
    assertThat(status.isLazyPersist(), is(true));
  }

  @Test (timeout=300000)
  public void testPlacementOnRamDisk() throws IOException {
    startUpCluster(REPL_FACTOR, new StorageType[] { DEFAULT, RAM_DISK}, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, RAM_DISK);
  }

  @Test (timeout=300000)
  public void testFallbackToDisk() throws IOException {
    startUpCluster(REPL_FACTOR, null, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path, DEFAULT);
  }

  /**
   * If the only available storage is RAM_DISK and the LAZY_PERSIST flag is not
   * specified, then block placement should fail.
   *
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testRamDiskNotChosenByDefault() throws IOException {
    startUpCluster(REPL_FACTOR, new StorageType[] {RAM_DISK, RAM_DISK}, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    try {
      makeTestFile(path, BLOCK_SIZE, false);
      fail("Block placement to RAM_DISK should have failed without lazyPersist flag");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  @Test (timeout=300000)
  public void testAppendIsDenied() throws IOException {
    startUpCluster(REPL_FACTOR, new StorageType[] {RAM_DISK, DEFAULT }, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    makeTestFile(path, BLOCK_SIZE, true);

    try {
      client.append(path.toString(), BUFFER_LENGTH, null, null).close();
      fail("Append to LazyPersist file did not fail as expected");
    } catch (Throwable t) {
      LOG.info("Got expected exception ", t);
    }
  }

  /**
   * If one or more replicas of a lazyPersist file are lost, then the file
   * must be discarded by the NN, instead of being kept around as a
   * 'corrupt' file.
   */
  @Test (timeout=300000)
  public void testLazyPersistFilesAreDiscarded()
      throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR,
                   new StorageType[] {RAM_DISK, DEFAULT },
                   (2 * BLOCK_SIZE - 1));   // 1 replica + delta.
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    makeTestFile(path2, BLOCK_SIZE, false);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
    ensureFileReplicasOnStorageType(path2, DEFAULT);

    // Stop the DataNode and sleep for the time it takes the NN to
    // detect the DN as being dead.
    cluster.shutdownDataNodes();
    Thread.sleep(30000L);
    assertThat(cluster.getNamesystem().getNumDeadDataNodes(), is(1));

    // Next, wait for the replication monitor to mark the file as
    // corrupt, plus some delta.
    Thread.sleep(2 * DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT * 1000);

    // Wait for the LazyPersistFileScrubber to run, plus some delta.
    Thread.sleep(2 * LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC * 1000);

    // Ensure that path1 does not exist anymore, whereas path2 does.
    assert(!fs.exists(path1));
    assert(fs.exists(path2));

    // We should have only one block that needs replication i.e. the one
    // belonging to path2.
    assertThat(cluster.getNameNode()
                      .getNamesystem()
                      .getBlockManager()
                      .getUnderReplicatedBlocksCount(),
               is(1L));
  }

  @Test (timeout=300000)
  public void testLazyPersistBlocksAreSaved()
      throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR, new StorageType[] {RAM_DISK, DEFAULT }, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path = new Path("/" + METHOD_NAME + ".dat");

    // Create a test file
    makeTestFile(path, BLOCK_SIZE * 10, true);
    LocatedBlocks locatedBlocks = ensureFileReplicasOnStorageType(path, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);

    // Make sure that there is a saved copy of the replica on persistent
    // storage.
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    List<? extends FsVolumeSpi> volumes =
        cluster.getDataNodes().get(0).getFSDataset().getVolumes();

    final Set<Long> persistedBlockIds = new HashSet<Long>();

    // Make sure at least one non-transient volume has a saved copy of
    // the replica.
    for (FsVolumeSpi v : volumes) {
      if (v.isTransientStorage()) {
        continue;
      }

      FsVolumeImpl volume = (FsVolumeImpl) v;
      File lazyPersistDir = volume.getBlockPoolSlice(bpid).getLazypersistDir();

      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        File targetDir = DatanodeUtil.idToBlockDir(lazyPersistDir, lb.getBlock().getBlockId());
        File blockFile = new File(targetDir, lb.getBlock().getBlockName());
        if (blockFile.exists()) {
          // Found a persisted copy for this block!
          boolean added = persistedBlockIds.add(lb.getBlock().getBlockId());
          assertThat(added, is(true));
        }
      }
    }

    // We should have found a persisted copy for each located block.
    assertThat(persistedBlockIds.size(), is(locatedBlocks.getLocatedBlocks().size()));
  }


  @Test (timeout=300000)
  public void testRamDiskEviction()
      throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR,
        new StorageType[] {RAM_DISK, DEFAULT },
        (2 * BLOCK_SIZE - 1));     // 1 replica + delta.
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    Path path2 = new Path("/" + METHOD_NAME + ".02.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    // However the block replica should not be evicted from RAM_DISK yet.
    Thread.sleep(3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Create another file with a replica on RAM_DISK.
    makeTestFile(path2, BLOCK_SIZE, true);
    DataNodeTestUtils.triggerBlockReport(cluster.getDataNodes().get(0));
    Thread.sleep(10 * 1000);

    // Make sure that the second file's block replica is on RAM_DISK, whereas
    // the original file's block replica is now on disk.
    ensureFileReplicasOnStorageType(path2, RAM_DISK);
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  /**
   * TODO: Stub test, to be completed.
   * Verify that checksum computation is skipped for files written to memory.
   */
  @Test (timeout=300000)
  public void testChecksumIsSkipped()
      throws IOException, InterruptedException {
    startUpCluster(REPL_FACTOR,
                   new StorageType[] {RAM_DISK, DEFAULT }, -1);
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Verify checksum was not computed.

  }

  // ---- Utility functions for all test cases -------------------------------

  /**
   * If ramDiskStorageLimit is >=0, then RAM_DISK capacity is artificially
   * capped. If tmpfsStorageLimit < 0 then it is ignored.
   */
  private void startUpCluster(final int numDataNodes,
                              final StorageType[] storageTypes,
                              final long ramDiskStorageLimit)
      throws IOException {

    conf = new Configuration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_NAMENODE_LAZY_PERSIST_FILE_SCRUB_INTERVAL_SEC,
                LAZY_WRITE_FILE_SCRUBBER_INTERVAL_SEC);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL_SEC);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                HEARTBEAT_RECHECK_INTERVAL_MSEC);
    conf.setInt(DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
                LAZY_WRITER_INTERVAL_SEC);

    REPL_FACTOR = 1; //Reset if case a test has modified the value

    cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(numDataNodes)
        .storageTypes(storageTypes != null ? storageTypes : new StorageType[] { DEFAULT, DEFAULT })
        .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();

    // Artifically cap the storage capacity of the tmpfs volume.
    if (ramDiskStorageLimit >= 0) {
      List<? extends FsVolumeSpi> volumes =
          cluster.getDataNodes().get(0).getFSDataset().getVolumes();

      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageType() == RAM_DISK) {
          ((FsTransientVolumeImpl) volume).setCapacityForTesting(ramDiskStorageLimit);
        }
      }
    }

    LOG.info("Cluster startup complete");
  }

  private void makeTestFile(Path path, long length, final boolean isLazyPersist)
      throws IOException {

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

  private LocatedBlocks ensureFileReplicasOnStorageType(
      Path path, StorageType storageType) throws IOException {
    // Ensure that returned block locations returned are correct!
    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
        client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      assertThat(locatedBlock.getStorageTypes()[0], is(storageType));
    }

    return locatedBlocks;
  }
}
