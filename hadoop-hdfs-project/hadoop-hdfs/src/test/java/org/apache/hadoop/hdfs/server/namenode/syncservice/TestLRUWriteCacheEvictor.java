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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
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
import java.util.Iterator;
import java.util.UUID;

import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test LRUWriteCacheEvictor.
 */
public class TestLRUWriteCacheEvictor {
  public static final long BLOCK_SIZE = 1024;
  // Set a large quota size to help test LRU policy, avoiding any actual
  // evict operation that will change the evict queue.
  public static final long WRITE_CACHE_QUOTA_SIZE = 200 * 1024;
  public static final short REPLICA_NUM = 3;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem fs;
  private static DFSClient dfsClient;
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
    // Use LRUWriteCacheEvictor
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        LRUWriteCacheEvictor.class.getName());
    // Use a small precision for access time, thus, in our test
    // MetadataUpdateEvent will be captured by edit log.
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1L);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(providedPath.toUri()).toString());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    dfsClient = cluster.getFileSystem().getClient();
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

  @Test(timeout = 50000)
  public void testEvictPolicy() throws IOException, InterruptedException {
    SyncMountManager syncMountManager =
        fsNamesystem.getMountManager().getSyncMountManager();
    WriteCacheEvictor writeCacheEvictor =
        syncMountManager.getWriteCacheEvictor();
    writeCacheEvictor.setFsNamesystem(fsNamesystem);
    Assert.assertTrue(writeCacheEvictor.getClass() ==
        LRUWriteCacheEvictor.class);
    LRUWriteCacheEvictor lruCacheEvictor =
        (LRUWriteCacheEvictor) writeCacheEvictor;
    String mountPoint = "/local/";
    Path testFile1 = new Path(mountPoint, "testFile1");
    final long testFileLen = 3 * BLOCK_SIZE;
    DFSTestUtil.createFile(fs, testFile1,
        testFileLen, REPLICA_NUM, 0xbeef);
    long blkCollectId1 = 1000L;
    when(syncMountManagerMock.getBlkCollectId(testFile1.toString()))
        .thenReturn(blkCollectId1);
    // Mock file1 is synced.
    lruCacheEvictor.add(blkCollectId1);

    Path testFile2 = new Path(mountPoint, "testFile2");
    DFSTestUtil.createFile(fs, testFile2,
        testFileLen, REPLICA_NUM, 0xbeef);
    long blkCollectId2 = 2000L;
    when(syncMountManagerMock.getBlkCollectId(testFile2.toString()))
        .thenReturn(blkCollectId2);
    // Mock file2 is synced.
    lruCacheEvictor.add(blkCollectId2);

    Path testFile3 = new Path(mountPoint, "/testFile3");
    DFSTestUtil.createFile(fs, testFile3,
        testFileLen, REPLICA_NUM, 0xbeef);
    long blkCollectId3 = 3000L;
    when(syncMountManagerMock.getBlkCollectId(testFile3.toString()))
        .thenReturn(blkCollectId3);
    // Mock file3 is synced.
    lruCacheEvictor.add(blkCollectId3);

    ProvidedVolumeInfo provided =
        new ProvidedVolumeInfo(UUID.randomUUID(), mountPoint,
            "s3a:///remote_path/", MountMode.WRITEBACK, null);
    // Mock a mount operation.
    when(syncMountManagerMock.getWriteBackMounts()).thenReturn(Arrays.asList(
        new ProvidedVolumeInfo[]{provided}));

    // Create another file outside the mount path. We expect its edit event
    // will not take effect in LRUWriteCacheEvictor.
    Path testFile = new Path(mountPoint, "/testFile");
    DFSTestUtil.createFile(fs, testFile,
        testFileLen, REPLICA_NUM, 0xbeef);
    readFile(testFile.toString());

    Assert.assertEquals(3, lruCacheEvictor.evictQueue.size());
    Assert.assertEquals(3, lruCacheEvictor.getEvictSet().size());
    Iterator<Long> iter = lruCacheEvictor.evictQueue.iterator();
    // Expect data order in evict queue is [blkCollectId1, blkCollectId2]
    Assert.assertTrue(iter.next() == blkCollectId1);
    Assert.assertTrue(iter.next() == blkCollectId2);
    Assert.assertTrue(iter.next() == blkCollectId3);

    // Trigger MetaUpdateEvent, to be captured by edit log.
    readFile(testFile1.toString());
    // Expect data order in evict queue will be changed to
    // [blkCollectId2, blkCollectId3, blkCollectId1]
    while (true) {
      lruCacheEvictor.updateCacheByLRU();
      iter = lruCacheEvictor.evictQueue.iterator();
      if (iter.next() == blkCollectId2) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertTrue(iter.next() == blkCollectId3);
    Assert.assertTrue(iter.next() == blkCollectId1);

    // Trigger another MetaUpdateEvent.
    readFile(testFile2.toString());
    // Expect data order in evict queue will be changed to
    // [blkCollectId3, blkCollectId1, blkCollectId2]
    while (true) {
      lruCacheEvictor.updateCacheByLRU();
      iter = lruCacheEvictor.evictQueue.iterator();
      if (iter.next() == blkCollectId3) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertTrue(iter.next() == blkCollectId1);
    Assert.assertTrue(iter.next() == blkCollectId2);
  }

  public void readFile(String filePath) throws IOException {
    DFSInputStream dfsInputStream = dfsClient.open(filePath);
    byte[] buffer = new byte[100];
    // read from HDFS
    while (dfsInputStream.read(buffer, 0, 100) != -1) {
    }
    dfsInputStream.close();
  }
}
