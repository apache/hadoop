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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_CACHING_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCacheReplicationManager {

  private static final long BLOCK_SIZE = 512;
  private static final int REPL_FACTOR = 3;
  private static final int NUM_DATANODES = 4;
  // Most Linux installs allow a default of 64KB locked memory
  private static final long CACHE_CAPACITY = 64 * 1024 / NUM_DATANODES;

  private static Configuration conf;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fs;
  private static NameNode nn;
  private static NamenodeProtocols nnRpc;
  private static CacheReplicationManager cacheReplManager;
  final private static FileSystemTestHelper helper = new FileSystemTestHelper();
  private static Path rootDir;

  @Before
  public void setUp() throws Exception {

    assumeTrue(NativeIO.isAvailable());

    conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        CACHE_CAPACITY);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setBoolean(DFS_NAMENODE_CACHING_ENABLED_KEY, true);
    conf.setLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 1000);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    nn = cluster.getNameNode();
    nnRpc = nn.getRpcServer();
    cacheReplManager = nn.getNamesystem().getCacheReplicationManager();
    rootDir = helper.getDefaultWorkingDirectory(fs);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private int countNumCachedBlocks() {
    return cacheReplManager.cachedBlocksMap.size();
  }

  private void waitForExpectedNumCachedBlocks(final int expected)
      throws Exception {
    int actual = countNumCachedBlocks();
    while (expected != actual)  {
      Thread.sleep(500);
      actual = countNumCachedBlocks();
    }
    waitForExpectedNumCachedReplicas(expected*REPL_FACTOR);
  }

  private void waitForExpectedNumCachedReplicas(final int expected)
      throws Exception {
    BlocksMap cachedBlocksMap = cacheReplManager.cachedBlocksMap;
    int actual = 0;
    while (expected != actual) {
      Thread.sleep(500);
      nn.getNamesystem().readLock();
      try {
        actual = 0;
        for (BlockInfo b : cachedBlocksMap.getBlocks()) {
          actual += cachedBlocksMap.numNodes(b);
        }
      } finally {
        nn.getNamesystem().readUnlock();
      }
    }
  }

  @Test(timeout=60000)
  public void testCachePaths() throws Exception {
    // Create the pool
    final String pool = "friendlyPool";
    nnRpc.addCachePool(new CachePoolInfo("friendlyPool"));
    // Create some test files
    final int numFiles = 2;
    final int numBlocksPerFile = 2;
    final List<String> paths = new ArrayList<String>(numFiles);
    for (int i=0; i<numFiles; i++) {
      Path p = new Path(rootDir, "testCachePaths-" + i);
      FileSystemTestHelper.createFile(fs, p, numBlocksPerFile, (int)BLOCK_SIZE);
      paths.add(p.toUri().getPath());
    }
    // Check the initial statistics at the namenode
    int expected = 0;
    waitForExpectedNumCachedBlocks(expected);
    // Cache and check each path in sequence
    for (int i=0; i<numFiles; i++) {
      PathBasedCacheDirective directive = new PathBasedCacheDirective(paths
          .get(i), pool);
      PathBasedCacheDescriptor descriptor =
          nnRpc.addPathBasedCacheDirective(directive);
      assertEquals("Descriptor does not match requested path", paths.get(i),
          directive.getPath());
      assertEquals("Descriptor does not match requested pool", pool,
          directive.getPool());
      expected += numBlocksPerFile;
      waitForExpectedNumCachedBlocks(expected);
    }
    // Uncache and check each path in sequence
    RemoteIterator<PathBasedCacheDescriptor> entries =
        nnRpc.listPathBasedCacheDescriptors(0, null, null);
    for (int i=0; i<numFiles; i++) {
      PathBasedCacheDescriptor descriptor = entries.next();
      nnRpc.removePathBasedCacheDescriptor(descriptor.getEntryId());
      expected -= numBlocksPerFile;
      waitForExpectedNumCachedBlocks(expected);
    }
  }
}
