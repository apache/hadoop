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

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPathNameError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.PathCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Fallible;
import org.junit.Test;

public class TestPathCacheRequests {
  static final Log LOG = LogFactory.getLog(TestPathCacheRequests.class);

  private static final UserGroupInformation unprivilegedUser =
      UserGroupInformation.createRemoteUser("unprivilegedUser");

  @Test
  public void testCreateAndRemovePools() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    NamenodeProtocols proto = cluster.getNameNodeRpc();
    CachePoolInfo req = new CachePoolInfo("pool1").
        setOwnerName("bob").setGroupName("bobgroup").
        setMode(new FsPermission((short)0755)).setWeight(150);
    proto.addCachePool(req);
    try {
      proto.removeCachePool("pool99");
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("can't remove " +
          "nonexistent cache pool", ioe);
    }
    proto.removeCachePool("pool1");
    try {
      proto.removeCachePool("pool1");
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("can't remove " +
          "nonexistent cache pool", ioe);
    }
    req = new CachePoolInfo("pool2");
    proto.addCachePool(req);
  }

  @Test
  public void testCreateAndModifyPools() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    // set low limits here for testing purposes
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES, 2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    NamenodeProtocols proto = cluster.getNameNodeRpc();
    proto.addCachePool(new CachePoolInfo("pool1").
        setOwnerName("abc").setGroupName("123").
        setMode(new FsPermission((short)0755)).setWeight(150));
    proto.modifyCachePool(new CachePoolInfo("pool1").
        setOwnerName("def").setGroupName("456"));
    RemoteIterator<CachePoolInfo> iter = proto.listCachePools("");
    CachePoolInfo info = iter.next();
    assertEquals("pool1", info.getPoolName());
    assertEquals("def", info.getOwnerName());
    assertEquals("456", info.getGroupName());
    assertEquals(new FsPermission((short)0755), info.getMode());
    assertEquals(Integer.valueOf(150), info.getWeight());

    try {
      proto.removeCachePool("pool99");
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
    proto.removeCachePool("pool1");
    try {
      proto.removeCachePool("pool1");
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
  }

  private static void validateListAll(
      RemoteIterator<PathCacheEntry> iter,
      long id0, long id1, long id2) throws Exception {
    Assert.assertEquals(new PathCacheEntry(id0,
        new PathCacheDirective("/alpha", "pool1")),
        iter.next());
    Assert.assertEquals(new PathCacheEntry(id1,
        new PathCacheDirective("/beta", "pool2")),
        iter.next());
    Assert.assertEquals(new PathCacheEntry(id2,
        new PathCacheDirective("/gamma", "pool1")),
        iter.next());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testSetAndGet() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final NamenodeProtocols proto = cluster.getNameNodeRpc();
      proto.addCachePool(new CachePoolInfo("pool1").
          setMode(new FsPermission((short)0777)));
      proto.addCachePool(new CachePoolInfo("pool2").
          setMode(new FsPermission((short)0777)));
      proto.addCachePool(new CachePoolInfo("pool3").
          setMode(new FsPermission((short)0777)));
      proto.addCachePool(new CachePoolInfo("pool4").
          setMode(new FsPermission((short)0)));

      List<Fallible<PathCacheEntry>> addResults1 = 
        unprivilegedUser.doAs(new PrivilegedExceptionAction<
            List<Fallible<PathCacheEntry>>>() {
          @Override
          public List<Fallible<PathCacheEntry>> run() throws IOException {
            return proto.addPathCacheDirectives(Arrays.asList(
              new PathCacheDirective[] {
                new PathCacheDirective("/alpha", "pool1"),
                new PathCacheDirective("/beta", "pool2"),
                new PathCacheDirective("", "pool3"),
                new PathCacheDirective("/zeta", "nonexistent_pool"),
                new PathCacheDirective("/zeta", "pool4")
              }));
            }
          });
      long ids1[] = new long[2];
      ids1[0] = addResults1.get(0).get().getEntryId();
      ids1[1] = addResults1.get(1).get().getEntryId();
      try {
        addResults1.get(2).get();
        Assert.fail("expected an error when adding an empty path");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof EmptyPathError);
      }
      try {
        addResults1.get(3).get();
        Assert.fail("expected an error when adding to a nonexistent pool.");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof InvalidPoolNameError);
      }
      try {
        addResults1.get(4).get();
        Assert.fail("expected an error when adding to a pool with " +
            "mode 0 (no permissions for anyone).");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause()
            instanceof PoolWritePermissionDeniedError);
      }

      List<Fallible<PathCacheEntry>> addResults2 = 
          proto.addPathCacheDirectives(Arrays.asList(
            new PathCacheDirective[] {
        new PathCacheDirective("/alpha", "pool1"),
        new PathCacheDirective("/theta", ""),
        new PathCacheDirective("bogus", "pool1"),
        new PathCacheDirective("/gamma", "pool1")
      }));
      long id = addResults2.get(0).get().getEntryId();
      Assert.assertEquals("expected to get back the same ID as last time " +
          "when re-adding an existing path cache directive.", ids1[0], id);
      try {
        addResults2.get(1).get();
        Assert.fail("expected an error when adding a path cache " +
            "directive with an empty pool name.");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof InvalidPoolNameError);
      }
      try {
        addResults2.get(2).get();
        Assert.fail("expected an error when adding a path cache " +
            "directive with a non-absolute path name.");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof InvalidPathNameError);
      }
      long ids2[] = new long[1];
      ids2[0] = addResults2.get(3).get().getEntryId();

      RemoteIterator<PathCacheEntry> iter =
          proto.listPathCacheEntries(0, "");
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      iter = proto.listPathCacheEntries(0, "");
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      iter = proto.listPathCacheEntries(0, "pool3");
      Assert.assertFalse(iter.hasNext());
      iter = proto.listPathCacheEntries(0, "pool2");
      Assert.assertEquals(addResults1.get(1).get(),
          iter.next());
      Assert.assertFalse(iter.hasNext());

      List<Fallible<Long>> removeResults1 = 
          proto.removePathCacheEntries(Arrays.asList(
            new Long[] { ids1[1], -42L, 999999L }));
      Assert.assertEquals(Long.valueOf(ids1[1]),
          removeResults1.get(0).get());
      try {
        removeResults1.get(1).get();
        Assert.fail("expected an error when removing a negative ID");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof InvalidIdException);
      }
      try {
        removeResults1.get(2).get();
        Assert.fail("expected an error when removing a nonexistent ID");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof NoSuchIdException);
      }
      iter = proto.listPathCacheEntries(0, "pool2");
      Assert.assertFalse(iter.hasNext());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
