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

import static org.junit.Assert.assertEquals;

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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPathNameError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.PoolWritePermissionDeniedError;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Fallible;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPathCacheRequests {
  static final Log LOG = LogFactory.getLog(TestPathCacheRequests.class);

  private static Configuration conf = new HdfsConfiguration();
  private static MiniDFSCluster cluster = null;
  private static NamenodeProtocols proto = null;

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    proto = cluster.getNameNodeRpc();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateAndRemovePools() throws Exception {
    CachePoolInfo req =
        CachePoolInfo.newBuilder().setPoolName("pool1").setOwnerName("bob")
            .setGroupName("bobgroup").setMode(new FsPermission((short) 0755))
            .setWeight(150).build();
    CachePool pool = proto.addCachePool(req);
    try {
      proto.removeCachePool(909);
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
    proto.removeCachePool(pool.getId());
    try {
      proto.removeCachePool(pool.getId());
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
    req = new CachePoolInfo("pool2");
    proto.addCachePool(req);
  }

  @Test
  public void testCreateAndModifyPools() throws Exception {
    // Create a new pool
    CachePoolInfo info = CachePoolInfo.newBuilder().
        setPoolName("pool1").
        setOwnerName("abc").
        setGroupName("123").
        setMode(new FsPermission((short)0755)).
        setWeight(150).
        build();
    CachePool pool = proto.addCachePool(info);
    CachePoolInfo actualInfo = pool.getInfo();
    assertEquals("Expected info to match create time settings",
        info, actualInfo);
    // Modify the pool
    info = CachePoolInfo.newBuilder().
        setPoolName("pool2").
        setOwnerName("def").
        setGroupName("456").
        setMode(new FsPermission((short)0644)).
        setWeight(200).
        build();
    proto.modifyCachePool(pool.getId(), info);
    // Check via listing this time
    RemoteIterator<CachePool> iter = proto.listCachePools(0, 1);
    CachePool listedPool = iter.next();
    actualInfo = listedPool.getInfo();
    assertEquals("Expected info to match modified settings", info, actualInfo);

    try {
      proto.removeCachePool(808);
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
    proto.removeCachePool(pool.getId());
    try {
      proto.removeCachePool(pool.getId());
      Assert.fail("expected to get an exception when " +
          "removing a non-existent pool.");
    } catch (IOException ioe) {
    }
  }

  private static void validateListAll(
      RemoteIterator<PathCacheEntry> iter,
      long id0, long id1, long id2) throws Exception {
    Assert.assertEquals(new PathCacheEntry(id0,
        new PathCacheDirective("/alpha", 1)),
        iter.next());
    Assert.assertEquals(new PathCacheEntry(id1,
        new PathCacheDirective("/beta", 2)),
        iter.next());
    Assert.assertEquals(new PathCacheEntry(id2,
        new PathCacheDirective("/gamma", 1)),
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
      final CachePool pool1 = proto.addCachePool(new CachePoolInfo("pool1"));
      final CachePool pool2 = proto.addCachePool(new CachePoolInfo("pool2"));
      final CachePool pool3 = proto.addCachePool(new CachePoolInfo("pool3"));
      final CachePool pool4 = proto.addCachePool(CachePoolInfo.newBuilder()
          .setPoolName("pool4")
          .setMode(new FsPermission((short)0)).build());
      UserGroupInformation testUgi = UserGroupInformation
          .createUserForTesting("myuser", new String[]{"mygroup"});
      List<Fallible<PathCacheEntry>> addResults1 = testUgi.doAs(
          new PrivilegedExceptionAction<List<Fallible<PathCacheEntry>>>() {
            @Override
            public List<Fallible<PathCacheEntry>> run() throws IOException {
              List<Fallible<PathCacheEntry>> entries;
              entries = proto.addPathCacheDirectives(
                  Arrays.asList(new PathCacheDirective[] {
                      new PathCacheDirective("/alpha", pool1.getId()),
                      new PathCacheDirective("/beta", pool2.getId()),
                      new PathCacheDirective("", pool3.getId()),
                      new PathCacheDirective("/zeta", 404),
                      new PathCacheDirective("/zeta", pool4.getId())
                  }));
              return entries;
            }
      });
      // Save the successful additions
      long ids1[] = new long[2];
      for (int i=0; i<2; i++) {
        ids1[i] = addResults1.get(i).get().getEntryId();
      }
      // Verify that the unsuccessful additions failed properly
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
        Assert.assertTrue(ioe.getCause() instanceof InvalidPoolError);
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
        new PathCacheDirective("/alpha", pool1.getId()),
        new PathCacheDirective("/theta", 404),
        new PathCacheDirective("bogus", pool1.getId()),
        new PathCacheDirective("/gamma", pool1.getId())
      }));
      long id = addResults2.get(0).get().getEntryId();
      Assert.assertEquals("expected to get back the same ID as last time " +
          "when re-adding an existing path cache directive.", ids1[0], id);
      try {
        addResults2.get(1).get();
        Assert.fail("expected an error when adding a path cache " +
            "directive with an empty pool name.");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof InvalidPoolError);
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

      // Validate listing all entries
      RemoteIterator<PathCacheEntry> iter =
          proto.listPathCacheEntries(-1l, -1l, 100);
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      iter = proto.listPathCacheEntries(-1l, -1l, 1);
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      // Validate listing certain pools
      iter = proto.listPathCacheEntries(0, pool3.getId(), 1);
      Assert.assertFalse(iter.hasNext());
      iter = proto.listPathCacheEntries(0, pool2.getId(), 4444);
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
      iter = proto.listPathCacheEntries(0, pool2.getId(), 4444);
      Assert.assertFalse(iter.hasNext());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
