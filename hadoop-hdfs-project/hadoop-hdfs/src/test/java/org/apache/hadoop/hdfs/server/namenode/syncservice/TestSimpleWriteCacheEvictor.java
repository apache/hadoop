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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.SyncMountManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;

/**
 * Test SimpleWriteCacheEvictor used for provided write back mount.
 */
public class TestSimpleWriteCacheEvictor {

  public static final long BLOCK_SIZE = 4 * 1024;
  public static final long WRITE_CACHE_QUOTA_SIZE = 1024 * 1024;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static DistributedFileSystem fs;
  private static FSNamesystem fsNamesystem;

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
    conf.set(DFSConfigKeys.DFS_PROVIDED_WRITE_CACHE_EVICTOR_CLASS,
        SimpleWriteCacheEvictor.class.getName());

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(providedPath.toUri()).toString());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    fsNamesystem = cluster.getNamesystem();
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

  @Test(timeout = 80000)
  public void testEviction() throws IOException, InterruptedException {
    SyncMountManager syncMountManager =
        fsNamesystem.getMountManager().getSyncMountManager();
    WriteCacheEvictor writeCacheEvictor =
        syncMountManager.getWriteCacheEvictor();
    Assert.assertTrue(writeCacheEvictor.getClass() ==
        SimpleWriteCacheEvictor.class);
    Assert.assertTrue(writeCacheEvictor.isRunning());

    String mountPoint = "/local/";
    String testFileName1 = "testFile1";
    Path testFile1 = new Path(mountPoint, testFileName1);
    final long testFileLen = 3 * BLOCK_SIZE;
    DFSTestUtil.createFile(fs, testFile1,
        testFileLen, (short) 1, 0xbeef);

    INodeFile inode =
        fsNamesystem.getFSDirectory().getINode(testFile1.toString()).asFile();
    long blockCollectionId = inode.getLastBlock().getBlockCollectionId();
    writeCacheEvictor.add(blockCollectionId);
    while (writeCacheEvictor.evictQueue.size() != 0) {
      Thread.sleep(500);
    }
  }
}
