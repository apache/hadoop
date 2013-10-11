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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
  private static DistributedFileSystem dfs;
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

    dfs = cluster.getFileSystem();
    nn = cluster.getNameNode();
    nnRpc = nn.getRpcServer();
    cacheReplManager = nn.getNamesystem().getCacheReplicationManager();
    rootDir = helper.getDefaultWorkingDirectory(dfs);
  }

  @After
  public void tearDown() throws Exception {
    if (dfs != null) {
      dfs.close();
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
      FileSystemTestHelper.createFile(dfs, p, numBlocksPerFile, (int)BLOCK_SIZE);
      paths.add(p.toUri().getPath());
    }
    // Check the initial statistics at the namenode
    int expected = 0;
    waitForExpectedNumCachedBlocks(expected);
    // Cache and check each path in sequence
    for (int i=0; i<numFiles; i++) {
      PathBasedCacheDirective directive = new PathBasedCacheDirective.Builder().
          setPath(new Path(paths.get(i))).
          setPool(pool).
          build();
      PathBasedCacheDescriptor descriptor =
          nnRpc.addPathBasedCacheDirective(directive);
      assertEquals("Descriptor does not match requested path", paths.get(i),
          descriptor.getPath().toUri().getPath());
      assertEquals("Descriptor does not match requested pool", pool,
          descriptor.getPool());
      expected += numBlocksPerFile;
      waitForExpectedNumCachedBlocks(expected);
      HdfsBlockLocation[] locations =
          (HdfsBlockLocation[]) dfs.getFileBlockLocations(
              new Path(paths.get(i)), 0, numBlocksPerFile * BLOCK_SIZE);
      assertEquals("Unexpected number of locations", numBlocksPerFile,
          locations.length);
      for (HdfsBlockLocation loc: locations) {
        assertEquals("Block should be present on all datanodes",
            3, loc.getHosts().length);
        DatanodeInfo[] cachedLocs = loc.getLocatedBlock().getCachedLocations();
        assertEquals("Block should be cached on all datanodes",
            loc.getHosts().length, cachedLocs.length);
      }
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

  @Test(timeout=60000)
  public void testCacheManagerRestart() throws Exception {
    // Create and validate a pool
    final String pool = "poolparty";
    String groupName = "partygroup";
    FsPermission mode = new FsPermission((short)0777);
    int weight = 747;
    dfs.addCachePool(new CachePoolInfo(pool)
        .setGroupName(groupName)
        .setMode(mode)
        .setWeight(weight));
    RemoteIterator<CachePoolInfo> pit = dfs.listCachePools();
    assertTrue("No cache pools found", pit.hasNext());
    CachePoolInfo info = pit.next();
    assertEquals(pool, info.getPoolName());
    assertEquals(groupName, info.getGroupName());
    assertEquals(mode, info.getMode());
    assertEquals(weight, (int)info.getWeight());
    assertFalse("Unexpected # of cache pools found", pit.hasNext());

    // Create some cache entries
    int numEntries = 10;
    String entryPrefix = "/party-";
    for (int i=0; i<numEntries; i++) {
      dfs.addPathBasedCacheDirective(new PathBasedCacheDirective.Builder().
          setPath(new Path(entryPrefix + i)).
          setPool(pool).
          build());
    }
    RemoteIterator<PathBasedCacheDescriptor> dit
        = dfs.listPathBasedCacheDescriptors(null, null);
    for (int i=0; i<numEntries; i++) {
      assertTrue("Unexpected # of cache entries: " + i, dit.hasNext());
      PathBasedCacheDescriptor cd = dit.next();
      assertEquals(i+1, cd.getEntryId());
      assertEquals(entryPrefix + i, cd.getPath().toUri().getPath());
      assertEquals(pool, cd.getPool());
    }
    assertFalse("Unexpected # of cache descriptors found", dit.hasNext());

    // Restart namenode
    cluster.restartNameNode();

    // Check that state came back up
    pit = dfs.listCachePools();
    assertTrue("No cache pools found", pit.hasNext());
    info = pit.next();
    assertEquals(pool, info.getPoolName());
    assertEquals(pool, info.getPoolName());
    assertEquals(groupName, info.getGroupName());
    assertEquals(mode, info.getMode());
    assertEquals(weight, (int)info.getWeight());
    assertFalse("Unexpected # of cache pools found", pit.hasNext());

    dit = dfs.listPathBasedCacheDescriptors(null, null);
    for (int i=0; i<numEntries; i++) {
      assertTrue("Unexpected # of cache entries: " + i, dit.hasNext());
      PathBasedCacheDescriptor cd = dit.next();
      assertEquals(i+1, cd.getEntryId());
      assertEquals(entryPrefix + i, cd.getPath().toUri().getPath());
      assertEquals(pool, cd.getPool());
    }
    assertFalse("Unexpected # of cache descriptors found", dit.hasNext());
  }

}
