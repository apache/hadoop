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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPathNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException.NoSuchIdException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPathBasedCacheRequests {
  static final Log LOG = LogFactory.getLog(TestPathBasedCacheRequests.class);

  private static final UserGroupInformation unprivilegedUser =
      UserGroupInformation.createRemoteUser("unprivilegedUser");

  static private Configuration conf;
  static private MiniDFSCluster cluster;
  static private DistributedFileSystem dfs;
  static private NamenodeProtocols proto;

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    // set low limits here for testing purposes
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DESCRIPTORS_NUM_RESPONSES, 2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    proto = cluster.getNameNodeRpc();
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBasicPoolOperations() throws Exception {
    final String poolName = "pool1";
    CachePoolInfo info = new CachePoolInfo(poolName).
        setOwnerName("bob").setGroupName("bobgroup").
        setMode(new FsPermission((short)0755)).setWeight(150);

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
        .setMode(new FsPermission((short)0700)).setWeight(314);
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
      GenericTestUtils.assertExceptionContains("can't remove " +
          "non-existent cache pool", ioe);
    }
    try {
      dfs.removeCachePool(poolName);
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("can't remove " +
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
  }

  @Test
  public void testCreateAndModifyPools() throws Exception {
    String poolName = "pool1";
    String ownerName = "abc";
    String groupName = "123";
    FsPermission mode = new FsPermission((short)0755);
    int weight = 150;
    dfs.addCachePool(new CachePoolInfo(poolName).
        setOwnerName(ownerName).setGroupName(groupName).
        setMode(mode).setWeight(weight));
    
    RemoteIterator<CachePoolInfo> iter = dfs.listCachePools();
    CachePoolInfo info = iter.next();
    assertEquals(poolName, info.getPoolName());
    assertEquals(ownerName, info.getOwnerName());
    assertEquals(groupName, info.getGroupName());

    ownerName = "def";
    groupName = "456";
    mode = new FsPermission((short)0700);
    weight = 151;
    dfs.modifyCachePool(new CachePoolInfo(poolName).
        setOwnerName(ownerName).setGroupName(groupName).
        setMode(mode).setWeight(weight));

    iter = dfs.listCachePools();
    info = iter.next();
    assertEquals(poolName, info.getPoolName());
    assertEquals(ownerName, info.getOwnerName());
    assertEquals(groupName, info.getGroupName());
    assertEquals(mode, info.getMode());
    assertEquals(Integer.valueOf(weight), info.getWeight());

    dfs.removeCachePool(poolName);
    iter = dfs.listCachePools();
    assertFalse("expected no cache pools after deleting pool", iter.hasNext());

    proto.listCachePools(null);

    try {
      proto.removeCachePool("pool99");
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("can't remove non-existent",
          ioe);
    }
    try {
      proto.removeCachePool(poolName);
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("can't remove non-existent",
          ioe);
    }

    iter = dfs.listCachePools();
    assertFalse("expected no cache pools after deleting pool", iter.hasNext());
  }

  private static void validateListAll(
      RemoteIterator<PathBasedCacheDescriptor> iter,
      PathBasedCacheDescriptor... descriptors) throws Exception {
    for (PathBasedCacheDescriptor descriptor: descriptors) {
      assertTrue("Unexpectedly few elements", iter.hasNext());
      assertEquals("Unexpected descriptor", descriptor, iter.next());
    }
    assertFalse("Unexpectedly many list elements", iter.hasNext());
  }

  private static PathBasedCacheDescriptor addAsUnprivileged(
      final PathBasedCacheDirective directive) throws Exception {
    return unprivilegedUser
        .doAs(new PrivilegedExceptionAction<PathBasedCacheDescriptor>() {
          @Override
          public PathBasedCacheDescriptor run() throws IOException {
            DistributedFileSystem myDfs =
                (DistributedFileSystem) FileSystem.get(conf);
            return myDfs.addPathBasedCacheDirective(directive);
          }
        });
  }

  @Test
  public void testAddRemoveDirectives() throws Exception {
    proto.addCachePool(new CachePoolInfo("pool1").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool2").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool3").
        setMode(new FsPermission((short)0777)));
    proto.addCachePool(new CachePoolInfo("pool4").
        setMode(new FsPermission((short)0)));

    PathBasedCacheDirective alpha =
        new PathBasedCacheDirective("/alpha", "pool1");
    PathBasedCacheDirective beta =
        new PathBasedCacheDirective("/beta", "pool2");
    PathBasedCacheDirective delta =
        new PathBasedCacheDirective("/delta", "pool1");

    PathBasedCacheDescriptor alphaD = addAsUnprivileged(alpha);
    PathBasedCacheDescriptor alphaD2 = addAsUnprivileged(alpha);
    assertEquals("Expected to get the same descriptor when re-adding"
        + "an existing PathBasedCacheDirective", alphaD, alphaD2);
    PathBasedCacheDescriptor betaD = addAsUnprivileged(beta);

    try {
      addAsUnprivileged(new PathBasedCacheDirective("", "pool3"));
      fail("expected an error when adding an empty path");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof EmptyPathError);
    }

    try {
      addAsUnprivileged(new PathBasedCacheDirective("/unicorn", "no_such_pool"));
      fail("expected an error when adding to a non-existent pool.");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof InvalidPoolNameError);
    }

    try {
      addAsUnprivileged(new PathBasedCacheDirective("/blackhole", "pool4"));
      fail("expected an error when adding to a pool with " +
          "mode 0 (no permissions for anyone).");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof PoolWritePermissionDeniedError);
    }

    try {
      addAsUnprivileged(new PathBasedCacheDirective("//illegal/path/", "pool1"));
      fail("expected an error when adding a malformed path " +
          "to the cache directives.");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof InvalidPathNameError);
    }

    try {
      addAsUnprivileged(new PathBasedCacheDirective("/emptypoolname", ""));
      Assert.fail("expected an error when adding a PathBasedCache " +
          "directive with an empty pool name.");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof InvalidPoolNameError);
    }

    try {
      addAsUnprivileged(new PathBasedCacheDirective("bogus", "pool1"));
      Assert.fail("expected an error when adding a PathBasedCache " +
          "directive with a non-absolute path name.");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof InvalidPathNameError);
    }

    PathBasedCacheDescriptor deltaD = addAsUnprivileged(delta);

    RemoteIterator<PathBasedCacheDescriptor> iter;
    iter = proto.listPathBasedCacheDescriptors(0, null, null);
    validateListAll(iter, alphaD, betaD, deltaD);
    iter = proto.listPathBasedCacheDescriptors(0, "pool3", null);
    Assert.assertFalse(iter.hasNext());
    iter = proto.listPathBasedCacheDescriptors(0, "pool1", null);
    validateListAll(iter, alphaD, deltaD);
    iter = proto.listPathBasedCacheDescriptors(0, "pool2", null);
    validateListAll(iter, betaD);

    dfs.removePathBasedCacheDescriptor(betaD);
    iter = proto.listPathBasedCacheDescriptors(0, "pool2", null);
    Assert.assertFalse(iter.hasNext());

    try {
      dfs.removePathBasedCacheDescriptor(betaD);
      Assert.fail("expected an error when removing a non-existent ID");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof NoSuchIdException);
    }

    try {
      proto.removePathBasedCacheDescriptor(-42l);
      Assert.fail("expected an error when removing a negative ID");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof InvalidIdException);
    }
    try {
      proto.removePathBasedCacheDescriptor(43l);
      Assert.fail("expected an error when removing a non-existent ID");
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof NoSuchIdException);
    }

    dfs.removePathBasedCacheDescriptor(alphaD);
    dfs.removePathBasedCacheDescriptor(deltaD);
    iter = proto.listPathBasedCacheDescriptors(0, null, null);
    assertFalse(iter.hasNext());
  }
}
