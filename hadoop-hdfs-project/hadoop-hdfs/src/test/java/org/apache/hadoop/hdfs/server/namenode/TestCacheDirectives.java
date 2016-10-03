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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS;
import static org.apache.hadoop.hdfs.protocol.CachePoolInfo.RELATIVE_EXPIRY_NEVER;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveIterator;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.GSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;

public class TestCacheDirectives {
  static final Log LOG = LogFactory.getLog(TestCacheDirectives.class);

  private static final UserGroupInformation unprivilegedUser =
      UserGroupInformation.createRemoteUser("unprivilegedUser");

  static private Configuration conf;
  static private MiniDFSCluster cluster;
  static private DistributedFileSystem dfs;
  static private NamenodeProtocols proto;
  static private NameNode namenode;
  static private CacheManipulator prevCacheManipulator;

  static {
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
  }

  private static final long BLOCK_SIZE = 4096;
  private static final int NUM_DATANODES = 4;
  // Most Linux installs will allow non-root users to lock 64KB.
  // In this test though, we stub out mlock so this doesn't matter.
  private static final long CACHE_CAPACITY = 64 * 1024 / NUM_DATANODES;

  private static HdfsConfiguration createCachingConf() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFS_DATANODE_MAX_LOCKED_MEMORY_KEY, CACHE_CAPACITY);
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setLong(DFS_NAMENODE_PATH_BASED_CACHE_REFRESH_INTERVAL_MS, 1000);
    // set low limits here for testing purposes
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES,
        2);

    return conf;
  }

  @Before
  public void setup() throws Exception {
    conf = createCachingConf();
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    proto = cluster.getNameNodeRpc();
    namenode = cluster.getNameNode();
    prevCacheManipulator = NativeIO.POSIX.getCacheManipulator();
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
    BlockReaderTestUtil.enableHdfsCachingTracing();
  }

  @After
  public void teardown() throws Exception {
    // Remove cache directives left behind by tests so that we release mmaps.
    RemoteIterator<CacheDirectiveEntry> iter = dfs.listCacheDirectives(null);
    while (iter.hasNext()) {
      dfs.removeCacheDirective(iter.next().getInfo().getId());
    }
    waitForCachedBlocks(namenode, 0, 0, "teardown");
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    // Restore the original CacheManipulator
    NativeIO.POSIX.setCacheManipulator(prevCacheManipulator);
  }

  @Test(timeout=60000)
  public void testBasicPoolOperations() throws Exception {
    final String poolName = "pool1";
    CachePoolInfo info = new CachePoolInfo(poolName).
        setOwnerName("bob").setGroupName("bobgroup").
        setMode(new FsPermission((short)0755)).setLimit(150l);

    // Add a pool
    dfs.addCachePool(info);

    // Do some bad addCachePools
    try {
      dfs.addCachePool(info);
      fail("added the pool with the same name twice");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("pool1 already exists", ioe);
    }
    try {
      dfs.addCachePool(new CachePoolInfo(""));
      fail("added empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      dfs.addCachePool(null);
      fail("added null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("CachePoolInfo is null", ioe);
    }
    try {
      proto.addCachePool(new CachePoolInfo(""));
      fail("added empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      proto.addCachePool(null);
      fail("added null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("CachePoolInfo is null", ioe);
    }

    // Modify the pool
    info.setOwnerName("jane").setGroupName("janegroup")
        .setMode(new FsPermission((short)0700)).setLimit(314l);
    dfs.modifyCachePool(info);

    // Do some invalid modify pools
    try {
      dfs.modifyCachePool(new CachePoolInfo("fool"));
      fail("modified non-existent cache pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("fool does not exist", ioe);
    }
    try {
      dfs.modifyCachePool(new CachePoolInfo(""));
      fail("modified empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      dfs.modifyCachePool(null);
      fail("modified null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("CachePoolInfo is null", ioe);
    }
    try {
      proto.modifyCachePool(new CachePoolInfo(""));
      fail("modified empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      proto.modifyCachePool(null);
      fail("modified null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("CachePoolInfo is null", ioe);
    }

    // Remove the pool
    dfs.removeCachePool(poolName);
    // Do some bad removePools
    try {
      dfs.removeCachePool("pool99");
      fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Cannot remove " +
          "non-existent cache pool", ioe);
    }
    try {
      dfs.removeCachePool(poolName);
      fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Cannot remove " +
          "non-existent cache pool", ioe);
    }
    try {
      dfs.removeCachePool("");
      fail("removed empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      dfs.removeCachePool(null);
      fail("removed null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      proto.removeCachePool("");
      fail("removed empty pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }
    try {
      proto.removeCachePool(null);
      fail("removed null pool");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("invalid empty cache pool name",
          ioe);
    }

    info = new CachePoolInfo("pool2");
    dfs.addCachePool(info);

    // Perform cache pool operations using a closed file system.
    DistributedFileSystem dfs1 = (DistributedFileSystem) cluster
        .getNewFileSystemInstance(0);
    dfs1.close();
    try {
      dfs1.listCachePools();
      fail("listCachePools using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.addCachePool(info);
      fail("addCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.modifyCachePool(info);
      fail("modifyCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.removeCachePool(poolName);
      fail("removeCachePool using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test(timeout=60000)
  public void testCreateAndModifyPools() throws Exception {
    String poolName = "pool1";
    String ownerName = "abc";
    String groupName = "123";
    FsPermission mode = new FsPermission((short)0755);
    long limit = 150;
    dfs.addCachePool(new CachePoolInfo(poolName).
        setOwnerName(ownerName).setGroupName(groupName).
        setMode(mode).setLimit(limit));
    
    RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
    CachePoolInfo info = iter.next().getInfo();
    assertEquals(poolName, info.getPoolName());
    assertEquals(ownerName, info.getOwnerName());
    assertEquals(groupName, info.getGroupName());

    ownerName = "def";
    groupName = "456";
    mode = new FsPermission((short)0700);
    limit = 151;
    dfs.modifyCachePool(new CachePoolInfo(poolName).
        setOwnerName(ownerName).setGroupName(groupName).
        setMode(mode).setLimit(limit));

    iter = dfs.listCachePools();
    info = iter.next().getInfo();
    assertEquals(poolName, info.getPoolName());
    assertEquals(ownerName, info.getOwnerName());
    assertEquals(groupName, info.getGroupName());
    assertEquals(mode, info.getMode());
    assertEquals(limit, (long)info.getLimit());

    dfs.removeCachePool(poolName);
    iter = dfs.listCachePools();
    assertFalse("expected no cache pools after deleting pool", iter.hasNext());

    proto.listCachePools(null);

    try {
      proto.removeCachePool("pool99");
      fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Cannot remove non-existent",
          ioe);
    }
    try {
      proto.removeCachePool(poolName);
      fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Cannot remove non-existent",
          ioe);
    }

    iter = dfs.listCachePools();
    assertFalse("expected no cache pools after deleting pool", iter.hasNext());
  }

  private static void validateListAll(
      RemoteIterator<CacheDirectiveEntry> iter,
      Long... ids) throws Exception {
    for (Long id: ids) {
      assertTrue("Unexpectedly few elements", iter.hasNext());
      assertEquals("Unexpected directive ID", id,
          iter.next().getInfo().getId());
    }
    assertFalse("Unexpectedly many list elements", iter.hasNext());
  }

  private static long addAsUnprivileged(
      final CacheDirectiveInfo directive) throws Exception {
    return unprivilegedUser
        .doAs(new PrivilegedExceptionAction<Long>() {
          @Override
          public Long run() throws IOException {
            DistributedFileSystem myDfs =
                (DistributedFileSystem) FileSystem.get(conf);
            return myDfs.addCacheDirective(directive);
          }
        });
  }

  @Test(timeout=60000)
  public void testAddRemoveDirectives() throws Exception {
    proto.addCachePool(new CachePoolInfo("pool1").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool2").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool3").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool4").
        setMode(new FsPermission((short)0)));

    CacheDirectiveInfo alpha = new CacheDirectiveInfo.Builder().
        setPath(new Path("/alpha")).
        setPool("pool1").
        build();
    CacheDirectiveInfo beta = new CacheDirectiveInfo.Builder().
        setPath(new Path("/beta")).
        setPool("pool2").
        build();
    CacheDirectiveInfo delta = new CacheDirectiveInfo.Builder().
        setPath(new Path("/delta")).
        setPool("pool1").
        build();

    long alphaId = addAsUnprivileged(alpha);
    long alphaId2 = addAsUnprivileged(alpha);
    assertFalse("Expected to get unique directives when re-adding an "
        + "existing CacheDirectiveInfo",
        alphaId == alphaId2);
    long betaId = addAsUnprivileged(beta);

    try {
      addAsUnprivileged(new CacheDirectiveInfo.Builder().
          setPath(new Path("/unicorn")).
          setPool("no_such_pool").
          build());
      fail("expected an error when adding to a non-existent pool.");
    } catch (InvalidRequestException ioe) {
      GenericTestUtils.assertExceptionContains("Unknown pool", ioe);
    }

    try {
      addAsUnprivileged(new CacheDirectiveInfo.Builder().
          setPath(new Path("/blackhole")).
          setPool("pool4").
          build());
      fail("expected an error when adding to a pool with " +
          "mode 0 (no permissions for anyone).");
    } catch (AccessControlException e) {
      GenericTestUtils.
          assertExceptionContains("Permission denied while accessing pool", e);
    }

    try {
      addAsUnprivileged(new CacheDirectiveInfo.Builder().
          setPath(new Path("/illegal:path/")).
          setPool("pool1").
          build());
      fail("expected an error when adding a malformed path " +
          "to the cache directives.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("is not a valid DFS filename", e);
    }

    try {
      addAsUnprivileged(new CacheDirectiveInfo.Builder().
          setPath(new Path("/emptypoolname")).
          setReplication((short)1).
          setPool("").
          build());
      fail("expected an error when adding a cache " +
          "directive with an empty pool name.");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("Invalid empty pool name", e);
    }

    long deltaId = addAsUnprivileged(delta);

    // We expect the following to succeed, because DistributedFileSystem
    // qualifies the path.
    long relativeId = addAsUnprivileged(
        new CacheDirectiveInfo.Builder().
            setPath(new Path("relative")).
            setPool("pool1").
            build());

    RemoteIterator<CacheDirectiveEntry> iter;
    iter = dfs.listCacheDirectives(null);
    validateListAll(iter, alphaId, alphaId2, betaId, deltaId, relativeId );
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setPool("pool3").build());
    assertFalse(iter.hasNext());
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setPool("pool1").build());
    validateListAll(iter, alphaId, alphaId2, deltaId, relativeId );
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setPool("pool2").build());
    validateListAll(iter, betaId);
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setId(alphaId2).build());
    validateListAll(iter, alphaId2);
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setId(relativeId).build());
    validateListAll(iter, relativeId);

    dfs.removeCacheDirective(betaId);
    iter = dfs.listCacheDirectives(
        new CacheDirectiveInfo.Builder().setPool("pool2").build());
    assertFalse(iter.hasNext());

    try {
      dfs.removeCacheDirective(betaId);
      fail("expected an error when removing a non-existent ID");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("No directive with ID", e);
    }

    try {
      proto.removeCacheDirective(-42l);
      fail("expected an error when removing a negative ID");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains(
          "Invalid negative ID", e);
    }
    try {
      proto.removeCacheDirective(43l);
      fail("expected an error when removing a non-existent ID");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("No directive with ID", e);
    }

    dfs.removeCacheDirective(alphaId);
    dfs.removeCacheDirective(alphaId2);
    dfs.removeCacheDirective(deltaId);

    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder().
        setId(relativeId).
        setReplication((short)555).
        build());
    iter = dfs.listCacheDirectives(null);
    assertTrue(iter.hasNext());
    CacheDirectiveInfo modified = iter.next().getInfo();
    assertEquals(relativeId, modified.getId().longValue());
    assertEquals((short)555, modified.getReplication().shortValue());
    dfs.removeCacheDirective(relativeId);
    iter = dfs.listCacheDirectives(null);
    assertFalse(iter.hasNext());

    // Verify that PBCDs with path "." work correctly
    CacheDirectiveInfo directive =
        new CacheDirectiveInfo.Builder().setPath(new Path("."))
            .setPool("pool1").build();
    long id = dfs.addCacheDirective(directive);
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(
        directive).setId(id).setReplication((short)2).build());
    dfs.removeCacheDirective(id);

    // Perform cache directive operations using a closed file system.
    DistributedFileSystem dfs1 = (DistributedFileSystem) cluster
        .getNewFileSystemInstance(0);
    dfs1.close();
    try {
      dfs1.listCacheDirectives(null);
      fail("listCacheDirectives using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.addCacheDirective(alpha);
      fail("addCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.modifyCacheDirective(alpha);
      fail("modifyCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
    try {
      dfs1.removeCacheDirective(alphaId);
      fail("removeCacheDirective using a closed filesystem!");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("Filesystem closed", ioe);
    }
  }

  @Test(timeout=60000)
  public void testCacheManagerRestart() throws Exception {
    SecondaryNameNode secondary = null;
    try {
      // Start a secondary namenode
      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          "0.0.0.0:0");
      secondary = new SecondaryNameNode(conf);
  
      // Create and validate a pool
      final String pool = "poolparty";
      String groupName = "partygroup";
      FsPermission mode = new FsPermission((short)0777);
      long limit = 747;
      dfs.addCachePool(new CachePoolInfo(pool)
          .setGroupName(groupName)
          .setMode(mode)
          .setLimit(limit));
      RemoteIterator<CachePoolEntry> pit = dfs.listCachePools();
      assertTrue("No cache pools found", pit.hasNext());
      CachePoolInfo info = pit.next().getInfo();
      assertEquals(pool, info.getPoolName());
      assertEquals(groupName, info.getGroupName());
      assertEquals(mode, info.getMode());
      assertEquals(limit, (long)info.getLimit());
      assertFalse("Unexpected # of cache pools found", pit.hasNext());
    
      // Create some cache entries
      int numEntries = 10;
      String entryPrefix = "/party-";
      long prevId = -1;
      final Date expiry = new Date();
      for (int i=0; i<numEntries; i++) {
        prevId = dfs.addCacheDirective(
            new CacheDirectiveInfo.Builder().
              setPath(new Path(entryPrefix + i)).setPool(pool).
              setExpiration(
                  CacheDirectiveInfo.Expiration.newAbsolute(expiry.getTime())).
              build());
      }
      RemoteIterator<CacheDirectiveEntry> dit
          = dfs.listCacheDirectives(null);
      for (int i=0; i<numEntries; i++) {
        assertTrue("Unexpected # of cache entries: " + i, dit.hasNext());
        CacheDirectiveInfo cd = dit.next().getInfo();
        assertEquals(i+1, cd.getId().longValue());
        assertEquals(entryPrefix + i, cd.getPath().toUri().getPath());
        assertEquals(pool, cd.getPool());
      }
      assertFalse("Unexpected # of cache directives found", dit.hasNext());
      
      // Checkpoint once to set some cache pools and directives on 2NN side
      secondary.doCheckpoint();
      
      // Add some more CacheManager state
      final String imagePool = "imagePool";
      dfs.addCachePool(new CachePoolInfo(imagePool));
      prevId = dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPath(new Path("/image")).setPool(imagePool).build());

      // Save a new image to force a fresh fsimage download
      dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      dfs.saveNamespace();
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // Checkpoint again forcing a reload of FSN state
      boolean fetchImage = secondary.doCheckpoint();
      assertTrue("Secondary should have fetched a new fsimage from NameNode",
          fetchImage);

      // Remove temp pool and directive
      dfs.removeCachePool(imagePool);

      // Restart namenode
      cluster.restartNameNode();
    
      // Check that state came back up
      pit = dfs.listCachePools();
      assertTrue("No cache pools found", pit.hasNext());
      info = pit.next().getInfo();
      assertEquals(pool, info.getPoolName());
      assertEquals(pool, info.getPoolName());
      assertEquals(groupName, info.getGroupName());
      assertEquals(mode, info.getMode());
      assertEquals(limit, (long)info.getLimit());
      assertFalse("Unexpected # of cache pools found", pit.hasNext());
    
      dit = dfs.listCacheDirectives(null);
      for (int i=0; i<numEntries; i++) {
        assertTrue("Unexpected # of cache entries: " + i, dit.hasNext());
        CacheDirectiveInfo cd = dit.next().getInfo();
        assertEquals(i+1, cd.getId().longValue());
        assertEquals(entryPrefix + i, cd.getPath().toUri().getPath());
        assertEquals(pool, cd.getPool());
        assertEquals(expiry.getTime(), cd.getExpiration().getMillis());
      }
      assertFalse("Unexpected # of cache directives found", dit.hasNext());
  
      long nextId = dfs.addCacheDirective(
            new CacheDirectiveInfo.Builder().
              setPath(new Path("/foobar")).setPool(pool).build());
      assertEquals(prevId + 1, nextId);
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
    }
  }

  /**
   * Wait for the NameNode to have an expected number of cached blocks
   * and replicas.
   * @param nn NameNode
   * @param expectedCachedBlocks if -1, treat as wildcard
   * @param expectedCachedReplicas if -1, treat as wildcard
   * @throws Exception
   */
  private static void waitForCachedBlocks(NameNode nn,
      final int expectedCachedBlocks, final int expectedCachedReplicas,
      final String logString) throws Exception {
    final FSNamesystem namesystem = nn.getNamesystem();
    final CacheManager cacheManager = namesystem.getCacheManager();
    LOG.info("Waiting for " + expectedCachedBlocks + " blocks with " +
             expectedCachedReplicas + " replicas.");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        int numCachedBlocks = 0, numCachedReplicas = 0;
        namesystem.readLock();
        try {
          GSet<CachedBlock, CachedBlock> cachedBlocks =
              cacheManager.getCachedBlocks();
          if (cachedBlocks != null) {
            for (Iterator<CachedBlock> iter = cachedBlocks.iterator();
                iter.hasNext(); ) {
              CachedBlock cachedBlock = iter.next();
              numCachedBlocks++;
              numCachedReplicas += cachedBlock.getDatanodes(Type.CACHED).size();
            }
          }
        } finally {
          namesystem.readUnlock();
        }

        LOG.info(logString + " cached blocks: have " + numCachedBlocks +
            " / " + expectedCachedBlocks + ".  " +
            "cached replicas: have " + numCachedReplicas +
            " / " + expectedCachedReplicas);

        if (expectedCachedBlocks == -1 ||
            numCachedBlocks == expectedCachedBlocks) {
          if (expectedCachedReplicas == -1 ||
              numCachedReplicas == expectedCachedReplicas) {
            return true;
          }
        }
        return false;
      }
    }, 500, 60000);
  }

  private static void waitForCacheDirectiveStats(final DistributedFileSystem dfs,
      final long targetBytesNeeded, final long targetBytesCached,
      final long targetFilesNeeded, final long targetFilesCached,
      final CacheDirectiveInfo filter, final String infoString)
            throws Exception {
    LOG.info("Polling listCacheDirectives " + 
        ((filter == null) ? "ALL" : filter.toString()) + " for " +
        targetBytesNeeded + " targetBytesNeeded, " +
        targetBytesCached + " targetBytesCached, " +
        targetFilesNeeded + " targetFilesNeeded, " +
        targetFilesCached + " targetFilesCached");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        RemoteIterator<CacheDirectiveEntry> iter = null;
        CacheDirectiveEntry entry = null;
        try {
          iter = dfs.listCacheDirectives(filter);
          entry = iter.next();
        } catch (IOException e) {
          fail("got IOException while calling " +
              "listCacheDirectives: " + e.getMessage());
        }
        Assert.assertNotNull(entry);
        CacheDirectiveStats stats = entry.getStats();
        if ((targetBytesNeeded == stats.getBytesNeeded()) &&
            (targetBytesCached == stats.getBytesCached()) &&
            (targetFilesNeeded == stats.getFilesNeeded()) &&
            (targetFilesCached == stats.getFilesCached())) {
          return true;
        } else {
          LOG.info(infoString + ": " +
              "filesNeeded: " +
              stats.getFilesNeeded() + "/" + targetFilesNeeded +
              ", filesCached: " + 
              stats.getFilesCached() + "/" + targetFilesCached +
              ", bytesNeeded: " +
              stats.getBytesNeeded() + "/" + targetBytesNeeded +
              ", bytesCached: " + 
              stats.getBytesCached() + "/" + targetBytesCached);
          return false;
        }
      }
    }, 500, 60000);
  }

  private static void waitForCachePoolStats(final DistributedFileSystem dfs,
      final long targetBytesNeeded, final long targetBytesCached,
      final long targetFilesNeeded, final long targetFilesCached,
      final CachePoolInfo pool, final String infoString)
            throws Exception {
    LOG.info("Polling listCachePools " + pool.toString() + " for " +
        targetBytesNeeded + " targetBytesNeeded, " +
        targetBytesCached + " targetBytesCached, " +
        targetFilesNeeded + " targetFilesNeeded, " +
        targetFilesCached + " targetFilesCached");
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        RemoteIterator<CachePoolEntry> iter = null;
        try {
          iter = dfs.listCachePools();
        } catch (IOException e) {
          fail("got IOException while calling " +
              "listCachePools: " + e.getMessage());
        }
        while (true) {
          CachePoolEntry entry = null;
          try {
            if (!iter.hasNext()) {
              break;
            }
            entry = iter.next();
          } catch (IOException e) {
            fail("got IOException while iterating through " +
                "listCachePools: " + e.getMessage());
          }
          if (entry == null) {
            break;
          }
          if (!entry.getInfo().getPoolName().equals(pool.getPoolName())) {
            continue;
          }
          CachePoolStats stats = entry.getStats();
          if ((targetBytesNeeded == stats.getBytesNeeded()) &&
              (targetBytesCached == stats.getBytesCached()) &&
              (targetFilesNeeded == stats.getFilesNeeded()) &&
              (targetFilesCached == stats.getFilesCached())) {
            return true;
          } else {
            LOG.info(infoString + ": " +
                "filesNeeded: " +
                stats.getFilesNeeded() + "/" + targetFilesNeeded +
                ", filesCached: " + 
                stats.getFilesCached() + "/" + targetFilesCached +
                ", bytesNeeded: " +
                stats.getBytesNeeded() + "/" + targetBytesNeeded +
                ", bytesCached: " + 
                stats.getBytesCached() + "/" + targetBytesCached);
            return false;
          }
        }
        return false;
      }
    }, 500, 60000);
  }

  private static void checkNumCachedReplicas(final DistributedFileSystem dfs,
      final List<Path> paths, final int expectedBlocks,
      final int expectedReplicas)
      throws Exception {
    int numCachedBlocks = 0;
    int numCachedReplicas = 0;
    for (Path p: paths) {
      final FileStatus f = dfs.getFileStatus(p);
      final long len = f.getLen();
      final long blockSize = f.getBlockSize();
      // round it up to full blocks
      final long numBlocks = (len + blockSize - 1) / blockSize;
      BlockLocation[] locs = dfs.getFileBlockLocations(p, 0, len);
      assertEquals("Unexpected number of block locations for path " + p,
          numBlocks, locs.length);
      for (BlockLocation l: locs) {
        if (l.getCachedHosts().length > 0) {
          numCachedBlocks++;
        }
        numCachedReplicas += l.getCachedHosts().length;
      }
    }
    LOG.info("Found " + numCachedBlocks + " of " + expectedBlocks + " blocks");
    LOG.info("Found " + numCachedReplicas + " of " + expectedReplicas
        + " replicas");
    assertEquals("Unexpected number of cached blocks", expectedBlocks,
        numCachedBlocks);
    assertEquals("Unexpected number of cached replicas", expectedReplicas,
        numCachedReplicas);
  }

  @Test(timeout=120000)
  public void testWaitForCachedReplicas() throws Exception {
    FileSystemTestHelper helper = new FileSystemTestHelper();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return ((namenode.getNamesystem().getCacheCapacity() ==
            (NUM_DATANODES * CACHE_CAPACITY)) &&
              (namenode.getNamesystem().getCacheUsed() == 0));
      }
    }, 500, 60000);

    // Send a cache report referring to a bogus block.  It is important that
    // the NameNode be robust against this.
    NamenodeProtocols nnRpc = namenode.getRpcServer();
    DataNode dn0 = cluster.getDataNodes().get(0);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    LinkedList<Long> bogusBlockIds = new LinkedList<Long> ();
    bogusBlockIds.add(999999L);
    nnRpc.cacheReport(dn0.getDNRegistrationForBP(bpid), bpid, bogusBlockIds);

    Path rootDir = helper.getDefaultWorkingDirectory(dfs);
    // Create the pool
    final String pool = "friendlyPool";
    nnRpc.addCachePool(new CachePoolInfo("friendlyPool"));
    // Create some test files
    final int numFiles = 2;
    final int numBlocksPerFile = 2;
    final List<String> paths = new ArrayList<String>(numFiles);
    for (int i=0; i<numFiles; i++) {
      Path p = new Path(rootDir, "testCachePaths-" + i);
      FileSystemTestHelper.createFile(dfs, p, numBlocksPerFile,
          (int)BLOCK_SIZE);
      paths.add(p.toUri().getPath());
    }
    // Check the initial statistics at the namenode
    waitForCachedBlocks(namenode, 0, 0, "testWaitForCachedReplicas:0");
    // Cache and check each path in sequence
    int expected = 0;
    for (int i=0; i<numFiles; i++) {
      CacheDirectiveInfo directive =
          new CacheDirectiveInfo.Builder().
            setPath(new Path(paths.get(i))).
            setPool(pool).
            build();
      nnRpc.addCacheDirective(directive, EnumSet.noneOf(CacheFlag.class));
      expected += numBlocksPerFile;
      waitForCachedBlocks(namenode, expected, expected,
          "testWaitForCachedReplicas:1");
    }

    // Check that the datanodes have the right cache values
    DatanodeInfo[] live = dfs.getDataNodeStats(DatanodeReportType.LIVE);
    assertEquals("Unexpected number of live nodes", NUM_DATANODES, live.length);
    long totalUsed = 0;
    for (DatanodeInfo dn : live) {
      final long cacheCapacity = dn.getCacheCapacity();
      final long cacheUsed = dn.getCacheUsed();
      final long cacheRemaining = dn.getCacheRemaining();
      assertEquals("Unexpected cache capacity", CACHE_CAPACITY, cacheCapacity);
      assertEquals("Capacity not equal to used + remaining",
          cacheCapacity, cacheUsed + cacheRemaining);
      assertEquals("Remaining not equal to capacity - used",
          cacheCapacity - cacheUsed, cacheRemaining);
      totalUsed += cacheUsed;
    }
    assertEquals(expected*BLOCK_SIZE, totalUsed);

    // Uncache and check each path in sequence
    RemoteIterator<CacheDirectiveEntry> entries =
      new CacheDirectiveIterator(nnRpc, null, FsTracer.get(conf));
    for (int i=0; i<numFiles; i++) {
      CacheDirectiveEntry entry = entries.next();
      nnRpc.removeCacheDirective(entry.getInfo().getId());
      expected -= numBlocksPerFile;
      waitForCachedBlocks(namenode, expected, expected,
          "testWaitForCachedReplicas:2");
    }
  }

  @Test(timeout=120000)
  public void testWaitForCachedReplicasInDirectory() throws Exception {
    // Create the pool
    final String pool = "friendlyPool";
    final CachePoolInfo poolInfo = new CachePoolInfo(pool);
    dfs.addCachePool(poolInfo);
    // Create some test files
    final List<Path> paths = new LinkedList<Path>();
    paths.add(new Path("/foo/bar"));
    paths.add(new Path("/foo/baz"));
    paths.add(new Path("/foo2/bar2"));
    paths.add(new Path("/foo2/baz2"));
    dfs.mkdir(new Path("/foo"), FsPermission.getDirDefault());
    dfs.mkdir(new Path("/foo2"), FsPermission.getDirDefault());
    final int numBlocksPerFile = 2;
    for (Path path : paths) {
      FileSystemTestHelper.createFile(dfs, path, numBlocksPerFile,
          (int)BLOCK_SIZE, (short)3, false);
    }
    waitForCachedBlocks(namenode, 0, 0,
        "testWaitForCachedReplicasInDirectory:0");

    // cache entire directory
    long id = dfs.addCacheDirective(
          new CacheDirectiveInfo.Builder().
            setPath(new Path("/foo")).
            setReplication((short)2).
            setPool(pool).
            build());
    waitForCachedBlocks(namenode, 4, 8,
        "testWaitForCachedReplicasInDirectory:1:blocks");
    // Verify that listDirectives gives the stats we want.
    waitForCacheDirectiveStats(dfs,
        4 * numBlocksPerFile * BLOCK_SIZE, 4 * numBlocksPerFile * BLOCK_SIZE,
        2, 2,
        new CacheDirectiveInfo.Builder().
            setPath(new Path("/foo")).
            build(),
        "testWaitForCachedReplicasInDirectory:1:directive");
    waitForCachePoolStats(dfs,
        4 * numBlocksPerFile * BLOCK_SIZE, 4 * numBlocksPerFile * BLOCK_SIZE,
        2, 2,
        poolInfo, "testWaitForCachedReplicasInDirectory:1:pool");

    long id2 = dfs.addCacheDirective(
          new CacheDirectiveInfo.Builder().
            setPath(new Path("/foo/bar")).
            setReplication((short)4).
            setPool(pool).
            build());
    // wait for an additional 2 cached replicas to come up
    waitForCachedBlocks(namenode, 4, 10,
        "testWaitForCachedReplicasInDirectory:2:blocks");
    // the directory directive's stats are unchanged
    waitForCacheDirectiveStats(dfs,
        4 * numBlocksPerFile * BLOCK_SIZE, 4 * numBlocksPerFile * BLOCK_SIZE,
        2, 2,
        new CacheDirectiveInfo.Builder().
            setPath(new Path("/foo")).
            build(),
        "testWaitForCachedReplicasInDirectory:2:directive-1");
    // verify /foo/bar's stats
    waitForCacheDirectiveStats(dfs,
        4 * numBlocksPerFile * BLOCK_SIZE,
        // only 3 because the file only has 3 replicas, not 4 as requested.
        3 * numBlocksPerFile * BLOCK_SIZE,
        1,
        // only 0 because the file can't be fully cached
        0,
        new CacheDirectiveInfo.Builder().
            setPath(new Path("/foo/bar")).
            build(),
        "testWaitForCachedReplicasInDirectory:2:directive-2");
    waitForCachePoolStats(dfs,
        (4+4) * numBlocksPerFile * BLOCK_SIZE,
        (4+3) * numBlocksPerFile * BLOCK_SIZE,
        3, 2,
        poolInfo, "testWaitForCachedReplicasInDirectory:2:pool");
    // remove and watch numCached go to 0
    dfs.removeCacheDirective(id);
    dfs.removeCacheDirective(id2);
    waitForCachedBlocks(namenode, 0, 0,
        "testWaitForCachedReplicasInDirectory:3:blocks");
    waitForCachePoolStats(dfs,
        0, 0,
        0, 0,
        poolInfo, "testWaitForCachedReplicasInDirectory:3:pool");
  }

  /**
   * Tests stepping the cache replication factor up and down, checking the
   * number of cached replicas and blocks as well as the advertised locations.
   * @throws Exception
   */
  @Test(timeout=120000)
  public void testReplicationFactor() throws Exception {
    // Create the pool
    final String pool = "friendlyPool";
    dfs.addCachePool(new CachePoolInfo(pool));
    // Create some test files
    final List<Path> paths = new LinkedList<Path>();
    paths.add(new Path("/foo/bar"));
    paths.add(new Path("/foo/baz"));
    paths.add(new Path("/foo2/bar2"));
    paths.add(new Path("/foo2/baz2"));
    dfs.mkdir(new Path("/foo"), FsPermission.getDirDefault());
    dfs.mkdir(new Path("/foo2"), FsPermission.getDirDefault());
    final int numBlocksPerFile = 2;
    for (Path path : paths) {
      FileSystemTestHelper.createFile(dfs, path, numBlocksPerFile,
          (int)BLOCK_SIZE, (short)3, false);
    }
    waitForCachedBlocks(namenode, 0, 0, "testReplicationFactor:0");
    checkNumCachedReplicas(dfs, paths, 0, 0);
    // cache directory
    long id = dfs.addCacheDirective(
        new CacheDirectiveInfo.Builder().
          setPath(new Path("/foo")).
          setReplication((short)1).
          setPool(pool).
          build());
    waitForCachedBlocks(namenode, 4, 4, "testReplicationFactor:1");
    checkNumCachedReplicas(dfs, paths, 4, 4);
    // step up the replication factor
    for (int i=2; i<=3; i++) {
      dfs.modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
          setId(id).
          setReplication((short)i).
          build());
      waitForCachedBlocks(namenode, 4, 4*i, "testReplicationFactor:2");
      checkNumCachedReplicas(dfs, paths, 4, 4*i);
    }
    // step it down
    for (int i=2; i>=1; i--) {
      dfs.modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
          setId(id).
          setReplication((short)i).
          build());
      waitForCachedBlocks(namenode, 4, 4*i, "testReplicationFactor:3");
      checkNumCachedReplicas(dfs, paths, 4, 4*i);
    }
    // remove and watch numCached go to 0
    dfs.removeCacheDirective(id);
    waitForCachedBlocks(namenode, 0, 0, "testReplicationFactor:4");
    checkNumCachedReplicas(dfs, paths, 0, 0);
  }

  @Test(timeout=60000)
  public void testListCachePoolPermissions() throws Exception {
    final UserGroupInformation myUser = UserGroupInformation
        .createRemoteUser("myuser");
    final DistributedFileSystem myDfs = 
        (DistributedFileSystem)DFSTestUtil.getFileSystemAs(myUser, conf);
    final String poolName = "poolparty";
    dfs.addCachePool(new CachePoolInfo(poolName)
        .setMode(new FsPermission((short)0700)));
    // Should only see partial info
    RemoteIterator<CachePoolEntry> it = myDfs.listCachePools();
    CachePoolInfo info = it.next().getInfo();
    assertFalse(it.hasNext());
    assertEquals("Expected pool name", poolName, info.getPoolName());
    assertNull("Unexpected owner name", info.getOwnerName());
    assertNull("Unexpected group name", info.getGroupName());
    assertNull("Unexpected mode", info.getMode());
    assertNull("Unexpected limit", info.getLimit());
    // Modify the pool so myuser is now the owner
    final long limit = 99;
    dfs.modifyCachePool(new CachePoolInfo(poolName)
        .setOwnerName(myUser.getShortUserName())
        .setLimit(limit));
    // Should see full info
    it = myDfs.listCachePools();
    info = it.next().getInfo();
    assertFalse(it.hasNext());
    assertEquals("Expected pool name", poolName, info.getPoolName());
    assertEquals("Mismatched owner name", myUser.getShortUserName(),
        info.getOwnerName());
    assertNotNull("Expected group name", info.getGroupName());
    assertEquals("Mismatched mode", (short) 0700,
        info.getMode().toShort());
    assertEquals("Mismatched limit", limit, (long)info.getLimit());
  }

  @Test(timeout=120000)
  public void testExpiry() throws Exception {
    String pool = "pool1";
    dfs.addCachePool(new CachePoolInfo(pool));
    Path p = new Path("/mypath");
    DFSTestUtil.createFile(dfs, p, BLOCK_SIZE*2, (short)2, 0x999);
    // Expire after test timeout
    Date start = new Date();
    Date expiry = DateUtils.addSeconds(start, 120);
    final long id = dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
        .setPath(p)
        .setPool(pool)
        .setExpiration(CacheDirectiveInfo.Expiration.newAbsolute(expiry))
        .setReplication((short)2)
        .build());
    waitForCachedBlocks(cluster.getNameNode(), 2, 4, "testExpiry:1");
    // Change it to expire sooner
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder().setId(id)
        .setExpiration(Expiration.newRelative(0)).build());
    waitForCachedBlocks(cluster.getNameNode(), 0, 0, "testExpiry:2");
    RemoteIterator<CacheDirectiveEntry> it = dfs.listCacheDirectives(null);
    CacheDirectiveEntry ent = it.next();
    assertFalse(it.hasNext());
    Date entryExpiry = new Date(ent.getInfo().getExpiration().getMillis());
    assertTrue("Directive should have expired",
        entryExpiry.before(new Date()));
    // Change it back to expire later
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder().setId(id)
        .setExpiration(Expiration.newRelative(120000)).build());
    waitForCachedBlocks(cluster.getNameNode(), 2, 4, "testExpiry:3");
    it = dfs.listCacheDirectives(null);
    ent = it.next();
    assertFalse(it.hasNext());
    entryExpiry = new Date(ent.getInfo().getExpiration().getMillis());
    assertTrue("Directive should not have expired",
        entryExpiry.after(new Date()));
    // Verify that setting a negative TTL throws an error
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder().setId(id)
          .setExpiration(Expiration.newRelative(-1)).build());
    } catch (InvalidRequestException e) {
      GenericTestUtils
          .assertExceptionContains("Cannot set a negative expiration", e);
    }
  }

  @Test(timeout=120000)
  public void testLimit() throws Exception {
    try {
      dfs.addCachePool(new CachePoolInfo("poolofnegativity").setLimit(-99l));
      fail("Should not be able to set a negative limit");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("negative", e);
    }
    final String destiny = "poolofdestiny";
    final Path path1 = new Path("/destiny");
    DFSTestUtil.createFile(dfs, path1, 2*BLOCK_SIZE, (short)1, 0x9494);
    // Start off with a limit that is too small
    final CachePoolInfo poolInfo = new CachePoolInfo(destiny)
        .setLimit(2*BLOCK_SIZE-1);
    dfs.addCachePool(poolInfo);
    final CacheDirectiveInfo info1 = new CacheDirectiveInfo.Builder()
        .setPool(destiny).setPath(path1).build();
    try {
      dfs.addCacheDirective(info1);
      fail("Should not be able to cache when there is no more limit");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("remaining capacity", e);
    }
    // Raise the limit up to fit and it should work this time
    poolInfo.setLimit(2*BLOCK_SIZE);
    dfs.modifyCachePool(poolInfo);
    long id1 = dfs.addCacheDirective(info1);
    waitForCachePoolStats(dfs,
        2*BLOCK_SIZE, 2*BLOCK_SIZE,
        1, 1,
        poolInfo, "testLimit:1");
    // Adding another file, it shouldn't be cached
    final Path path2 = new Path("/failure");
    DFSTestUtil.createFile(dfs, path2, BLOCK_SIZE, (short)1, 0x9495);
    try {
      dfs.addCacheDirective(new CacheDirectiveInfo.Builder()
          .setPool(destiny).setPath(path2).build(),
          EnumSet.noneOf(CacheFlag.class));
      fail("Should not be able to add another cached file");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("remaining capacity", e);
    }
    // Bring the limit down, the first file should get uncached
    poolInfo.setLimit(BLOCK_SIZE);
    dfs.modifyCachePool(poolInfo);
    waitForCachePoolStats(dfs,
        2*BLOCK_SIZE, 0,
        1, 0,
        poolInfo, "testLimit:2");
    RemoteIterator<CachePoolEntry> it = dfs.listCachePools();
    assertTrue("Expected a cache pool", it.hasNext());
    CachePoolStats stats = it.next().getStats();
    assertEquals("Overlimit bytes should be difference of needed and limit",
        BLOCK_SIZE, stats.getBytesOverlimit());
    // Moving a directive to a pool without enough limit should fail
    CachePoolInfo inadequate =
        new CachePoolInfo("poolofinadequacy").setLimit(BLOCK_SIZE);
    dfs.addCachePool(inadequate);
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(info1)
          .setId(id1).setPool(inadequate.getPoolName()).build(),
          EnumSet.noneOf(CacheFlag.class));
    } catch(InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("remaining capacity", e);
    }
    // Succeeds when force=true
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(info1).setId(id1)
        .setPool(inadequate.getPoolName()).build(),
        EnumSet.of(CacheFlag.FORCE));
    // Also can add with force=true
    dfs.addCacheDirective(
        new CacheDirectiveInfo.Builder().setPool(inadequate.getPoolName())
            .setPath(path1).build(), EnumSet.of(CacheFlag.FORCE));
  }

  @Test(timeout=30000)
  public void testMaxRelativeExpiry() throws Exception {
    // Test that negative and really big max expirations can't be set during add
    try {
      dfs.addCachePool(new CachePoolInfo("failpool").setMaxRelativeExpiryMs(-1l));
      fail("Added a pool with a negative max expiry.");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("negative", e);
    }
    try {
      dfs.addCachePool(new CachePoolInfo("failpool")
          .setMaxRelativeExpiryMs(Long.MAX_VALUE - 1));
      fail("Added a pool with too big of a max expiry.");
    } catch (InvalidRequestException e) {
      GenericTestUtils.assertExceptionContains("too big", e);
    }
    // Test that setting a max relative expiry on a pool works
    CachePoolInfo coolPool = new CachePoolInfo("coolPool");
    final long poolExpiration = 1000 * 60 * 10l;
    dfs.addCachePool(coolPool.setMaxRelativeExpiryMs(poolExpiration));
    RemoteIterator<CachePoolEntry> poolIt = dfs.listCachePools();
    CachePoolInfo listPool = poolIt.next().getInfo();
    assertFalse("Should only be one pool", poolIt.hasNext());
    assertEquals("Expected max relative expiry to match set value",
        poolExpiration, listPool.getMaxRelativeExpiryMs().longValue());
    // Test that negative and really big max expirations can't be modified
    try {
      dfs.addCachePool(coolPool.setMaxRelativeExpiryMs(-1l));
      fail("Added a pool with a negative max expiry.");
    } catch (InvalidRequestException e) {
      assertExceptionContains("negative", e);
    }
    try {
      dfs.modifyCachePool(coolPool
          .setMaxRelativeExpiryMs(CachePoolInfo.RELATIVE_EXPIRY_NEVER+1));
      fail("Added a pool with too big of a max expiry.");
    } catch (InvalidRequestException e) {
      assertExceptionContains("too big", e);
    }
    // Test that adding a directives without an expiration uses the pool's max
    CacheDirectiveInfo defaultExpiry = new CacheDirectiveInfo.Builder()
        .setPath(new Path("/blah"))
        .setPool(coolPool.getPoolName())
        .build();
    dfs.addCacheDirective(defaultExpiry);
    RemoteIterator<CacheDirectiveEntry> dirIt =
        dfs.listCacheDirectives(defaultExpiry);
    CacheDirectiveInfo listInfo = dirIt.next().getInfo();
    assertFalse("Should only have one entry in listing", dirIt.hasNext());
    long listExpiration = listInfo.getExpiration().getAbsoluteMillis()
        - new Date().getTime();
    assertTrue("Directive expiry should be approximately the pool's max expiry",
        Math.abs(listExpiration - poolExpiration) < 10*1000);
    // Test that the max is enforced on add for relative and absolute
    CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder()
        .setPath(new Path("/lolcat"))
        .setPool(coolPool.getPoolName());
    try {
      dfs.addCacheDirective(builder
          .setExpiration(Expiration.newRelative(poolExpiration+1))
          .build());
      fail("Added a directive that exceeds pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    try {
      dfs.addCacheDirective(builder
          .setExpiration(Expiration.newAbsolute(
              new Date().getTime() + poolExpiration + (10*1000)))
          .build());
      fail("Added a directive that exceeds pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    // Test that max is enforced on modify for relative and absolute Expirations
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
          .setId(listInfo.getId())
          .setExpiration(Expiration.newRelative(poolExpiration+1))
          .build());
      fail("Modified a directive to exceed pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
          .setId(listInfo.getId())
          .setExpiration(Expiration.newAbsolute(
              new Date().getTime() + poolExpiration + (10*1000)))
          .build());
      fail("Modified a directive to exceed pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    // Test some giant limit values with add
    try {
      dfs.addCacheDirective(builder
          .setExpiration(Expiration.newRelative(
              Long.MAX_VALUE))
          .build());
      fail("Added a directive with a gigantic max value");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("is too far in the future", e);
    }
    try {
      dfs.addCacheDirective(builder
          .setExpiration(Expiration.newAbsolute(
              Long.MAX_VALUE))
          .build());
      fail("Added a directive with a gigantic max value");
    } catch (InvalidRequestException e) {
      assertExceptionContains("is too far in the future", e);
    }
    // Test some giant limit values with modify
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
          .setId(listInfo.getId())
          .setExpiration(Expiration.NEVER)
          .build());
      fail("Modified a directive to exceed pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
          .setId(listInfo.getId())
          .setExpiration(Expiration.newAbsolute(
              Long.MAX_VALUE))
          .build());
      fail("Modified a directive to exceed pool's max relative expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("is too far in the future", e);
    }
    // Test that the max is enforced on modify correctly when changing pools
    CachePoolInfo destPool = new CachePoolInfo("destPool");
    dfs.addCachePool(destPool.setMaxRelativeExpiryMs(poolExpiration / 2));
    try {
      dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
          .setId(listInfo.getId())
          .setPool(destPool.getPoolName())
          .build());
      fail("Modified a directive to a pool with a lower max expiration");
    } catch (InvalidRequestException e) {
      assertExceptionContains("exceeds the max relative expiration", e);
    }
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder(defaultExpiry)
        .setId(listInfo.getId())
        .setPool(destPool.getPoolName())
        .setExpiration(Expiration.newRelative(poolExpiration / 2))
        .build());
    dirIt = dfs.listCacheDirectives(new CacheDirectiveInfo.Builder()
        .setPool(destPool.getPoolName())
        .build());
    listInfo = dirIt.next().getInfo();
    listExpiration = listInfo.getExpiration().getAbsoluteMillis()
        - new Date().getTime();
    assertTrue("Unexpected relative expiry " + listExpiration
        + " expected approximately " + poolExpiration/2,
        Math.abs(poolExpiration/2 - listExpiration) < 10*1000);
    // Test that cache pool and directive expiry can be modified back to never
    dfs.modifyCachePool(destPool
        .setMaxRelativeExpiryMs(CachePoolInfo.RELATIVE_EXPIRY_NEVER));
    poolIt = dfs.listCachePools();
    listPool = poolIt.next().getInfo();
    while (!listPool.getPoolName().equals(destPool.getPoolName())) {
      listPool = poolIt.next().getInfo();
    }
    assertEquals("Expected max relative expiry to match set value",
        CachePoolInfo.RELATIVE_EXPIRY_NEVER,
        listPool.getMaxRelativeExpiryMs().longValue());
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder()
        .setId(listInfo.getId())
        .setExpiration(Expiration.newRelative(RELATIVE_EXPIRY_NEVER))
        .build());
    // Test modifying close to the limit
    dfs.modifyCacheDirective(new CacheDirectiveInfo.Builder()
        .setId(listInfo.getId())
        .setExpiration(Expiration.newRelative(RELATIVE_EXPIRY_NEVER - 1))
        .build());
  }

  /**
   * Check that the NameNode is not attempting to cache anything.
   */
  private void checkPendingCachedEmpty(MiniDFSCluster cluster)
      throws Exception {
    Thread.sleep(1000);
    cluster.getNamesystem().readLock();
    try {
      final DatanodeManager datanodeManager =
          cluster.getNamesystem().getBlockManager().getDatanodeManager();
      for (DataNode dn : cluster.getDataNodes()) {
        DatanodeDescriptor descriptor =
            datanodeManager.getDatanode(dn.getDatanodeId());
        Assert.assertTrue("Pending cached list of " + descriptor +
                " is not empty, "
                + Arrays.toString(descriptor.getPendingCached().toArray()), 
            descriptor.getPendingCached().isEmpty());
      }
    } finally {
      cluster.getNamesystem().readUnlock();
    }
  }

  @Test(timeout=60000)
  public void testExceedsCapacity() throws Exception {
    // Create a giant file
    final Path fileName = new Path("/exceeds");
    final long fileLen = CACHE_CAPACITY * (NUM_DATANODES*2);
    int numCachedReplicas = (int) ((CACHE_CAPACITY*NUM_DATANODES)/BLOCK_SIZE);
    DFSTestUtil.createFile(dfs, fileName, fileLen, (short) NUM_DATANODES,
        0xFADED);
    dfs.addCachePool(new CachePoolInfo("pool"));
    dfs.addCacheDirective(new CacheDirectiveInfo.Builder().setPool("pool")
        .setPath(fileName).setReplication((short) 1).build());
    waitForCachedBlocks(namenode, -1, numCachedReplicas,
        "testExceeds:1");
    checkPendingCachedEmpty(cluster);
    checkPendingCachedEmpty(cluster);

    // Try creating a file with giant-sized blocks that exceed cache capacity
    dfs.delete(fileName, false);
    DFSTestUtil.createFile(dfs, fileName, 4096, fileLen, CACHE_CAPACITY * 2,
        (short) 1, 0xFADED);
    checkPendingCachedEmpty(cluster);
    checkPendingCachedEmpty(cluster);
  }

  @Test(timeout=60000)
  public void testNoBackingReplica() throws Exception {
    // Cache all three replicas for a file.
    final Path filename = new Path("/noback");
    final short replication = (short) 3;
    DFSTestUtil.createFile(dfs, filename, 1, replication, 0x0BAC);
    dfs.addCachePool(new CachePoolInfo("pool"));
    dfs.addCacheDirective(
        new CacheDirectiveInfo.Builder().setPool("pool").setPath(filename)
            .setReplication(replication).build());
    waitForCachedBlocks(namenode, 1, replication, "testNoBackingReplica:1");
    // Pause cache reports while we change the replication factor.
    // This will orphan some cached replicas.
    DataNodeTestUtils.setCacheReportsDisabledForTests(cluster, true);
    try {
      dfs.setReplication(filename, (short) 1);
      DFSTestUtil.waitForReplication(dfs, filename, (short) 1, 30000);
      // The cache locations should drop down to 1 even without cache reports.
      waitForCachedBlocks(namenode, 1, (short) 1, "testNoBackingReplica:2");
    } finally {
      DataNodeTestUtils.setCacheReportsDisabledForTests(cluster, false);
    }
  }

  @Test
  public void testNoLookupsWhenNotUsed() throws Exception {
    CacheManager cm = cluster.getNamesystem().getCacheManager();
    LocatedBlocks locations = Mockito.mock(LocatedBlocks.class);
    cm.setCachedLocations(locations);
    Mockito.verifyZeroInteractions(locations);
  }
}
