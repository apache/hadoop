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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.MountMode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.MountManager;
import org.apache.hadoop.hdfs.server.namenode.SyncMountManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test FIFOWriteCacheEvictor used for provided write back cache.
 */
public class TestFIFOWriteCacheEvictor {
  public static final long BLOCK_SIZE = 1024;
  public static final long WRITE_CACHE_QUOTA_SIZE = 20 * 1024;
  public static final short REPLICA_NUM = 3;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem fs;
  private static FSNamesystem fsNamesystem;

  @Mock
  private static MountManager mountManagerMock;
  @Mock
  private static SyncMountManager syncMountManagerMock;

  static {
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(WriteCacheEvictor.class), Level.DEBUG);
  }

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    final File fBASE = GenericTestUtils.getRandomizedTestDir();
    final Path pBASE = new Path(fBASE.toURI().toString());
    final Path providedPath = new Path(pBASE, "providedDir");
    final Path nnDirPath = new Path(pBASE, "nnDir");
    final String bpid = "BP-1234-10.1.1.1-1224";

    // Config for provided storage
    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_PROVIDED_ENABLED, true);
    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR,
        nnDirPath.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
        new Path(nnDirPath, fileNameFromBlockPoolID(bpid)).toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER, "\t");
    conf.setLong(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_QUOTA_SIZE,
        WRITE_CACHE_QUOTA_SIZE);
    conf.setFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK, 0.7f);
    conf.setFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK, 0.5f);
    // Use FIFOWriteCacheEvictor
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        FIFOWriteCacheEvictor.class.getName());

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(providedPath.toUri()).toString());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    fsNamesystem = cluster.getNamesystem();

    mountManagerMock = mock(MountManager.class);
    fsNamesystem.setMountManager(mountManagerMock);
    syncMountManagerMock = mock(SyncMountManager.class);
    when(mountManagerMock.getSyncMountManager())
        .thenReturn(syncMountManagerMock);
    when(syncMountManagerMock.getWriteCacheEvictor())
        .thenReturn(WriteCacheEvictor.getInstance(conf, fsNamesystem));
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testConfig() {
//    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        FIFOWriteCacheEvictor.class.getName());
    // Wrongly configure low watermark and high watermark.
    // The former should not be larger than the latter.
    conf.setFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK, 0.8f);
    conf.setFloat(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK, 0.6f);
    WriteCacheEvictor writeCacheEvictor = WriteCacheEvictor.getInstance(conf,
        null);
    Assert.assertTrue(writeCacheEvictor.getClass() ==
        FIFOWriteCacheEvictor.class);
    FIFOWriteCacheEvictor fifoEvictor =
        (FIFOWriteCacheEvictor) writeCacheEvictor;
    Assert.assertTrue(fifoEvictor.getHighWatermark() ==
        DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_HIGH_WATERMARK_DEFAULT);
    Assert.assertTrue(fifoEvictor.getLowWatermark() ==
        DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_LOW_WATERMARK_DEFAULT);
  }

  @Test(timeout = 80000)
  public void testEviction() throws IOException, InterruptedException {
    SyncMountManager syncMountManager =
        fsNamesystem.getMountManager().getSyncMountManager();
    WriteCacheEvictor writeCacheEvictor =
        syncMountManager.getWriteCacheEvictor();
    Assert.assertTrue(writeCacheEvictor.getClass() ==
        FIFOWriteCacheEvictor.class);
    FIFOWriteCacheEvictor fifoCacheEvictor =
        (FIFOWriteCacheEvictor) writeCacheEvictor;
    Assert.assertTrue(writeCacheEvictor.isRunning());
    fifoCacheEvictor.setFsNamesystem(fsNamesystem);

    // Mock add a writeback mount
    String mountPoint1 = "/local1/";
    fs.mkdirs(new Path(mountPoint1));
    ProvidedVolumeInfo provided1 =
        new ProvidedVolumeInfo(UUID.randomUUID(), mountPoint1,
            "s3a:///remote_path/", MountMode.WRITEBACK, null);
    Assert.assertEquals(0L, fifoCacheEvictor.getSpaceUsed(mountPoint1));

    when(syncMountManagerMock.getWriteBackMounts()).thenReturn(
        Arrays.asList(new ProvidedVolumeInfo[]{provided1}));

    String testFileName1 = "testFile1";
    Path testFile1 = new Path(mountPoint1, testFileName1);
    final long testFileLen = 3 * BLOCK_SIZE;
    DFSTestUtil.createFile(fs, testFile1,
        testFileLen, REPLICA_NUM, 0xbeef);
    while (fifoCacheEvictor.getSpaceUsed(mountPoint1) !=
        testFileLen * REPLICA_NUM) {
      Thread.sleep(500);
    }
    // high water mark bytes are 0.7 * 20KB = 14KB, but current is 9KB
    Assert.assertEquals(0L, fifoCacheEvictor.bytesToEvict());

    // Mock add another writeback mount
    String mountPoint2 = "/local2";
    fs.mkdirs(new Path(mountPoint2));
    ProvidedVolumeInfo provided2 =
        new ProvidedVolumeInfo(UUID.randomUUID(), mountPoint2,
            "s3a:///remote_path/", MountMode.WRITEBACK, null);
    Assert.assertEquals(0L, fifoCacheEvictor.getSpaceUsed(mountPoint2));

    when(syncMountManagerMock.getWriteBackMounts()).thenReturn(
        Arrays.asList(new ProvidedVolumeInfo[]{provided1, provided2}));

    String testFileName2 = "testFile2";
    Path testFile2 = new Path(mountPoint2, testFileName2);
    DFSTestUtil.createFile(fs, testFile2,
        testFileLen, REPLICA_NUM, 0xbeef);
    while (fifoCacheEvictor.getSpaceUsed(mountPoint2) !=
        testFileLen * REPLICA_NUM) {
      Thread.sleep(500);
    }

    // Current space used is 2 * 3 * 3KB = 18KB, high water mark is reached.
    long spaceUsed = fifoCacheEvictor.getSpaceUsed(mountPoint1) +
        fifoCacheEvictor.getSpaceUsed(mountPoint2);
    Assert.assertTrue(spaceUsed >= fifoCacheEvictor.getHighWatermark());
    Assert.assertTrue(fifoCacheEvictor.bytesToEvict() > 0);
    long lowWatermarkValue = (long) (fifoCacheEvictor.getLowWatermark() *
        fifoCacheEvictor.getWriteCacheQuota());
    // Two test files have same length and replica num.
    long expectValue = testFileLen * REPLICA_NUM * 2 - lowWatermarkValue;
    Assert.assertEquals(expectValue, fifoCacheEvictor.bytesToEvict());

    INode iNode = fsNamesystem.getFSDirectory().getINode(testFile2.toString());
    long computeSize = iNode.asFile().computeFileSize();
    Assert.assertEquals(testFileLen, computeSize);
  }
}
