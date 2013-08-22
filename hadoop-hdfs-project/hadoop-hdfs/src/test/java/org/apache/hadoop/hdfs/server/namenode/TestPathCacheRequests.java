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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPathNameError;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.PathCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathCacheEntry;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.util.Fallible;
import org.junit.Test;

public class TestPathCacheRequests {
  static final Log LOG = LogFactory.getLog(TestPathCacheRequests.class);

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
      NamenodeProtocols proto = cluster.getNameNodeRpc();
      List<Fallible<PathCacheEntry>> addResults1 = 
          proto.addPathCacheDirectives(Arrays.asList(
            new PathCacheDirective[] {
        new PathCacheDirective("/alpha", "pool1"),
        new PathCacheDirective("/beta", "pool2"),
        new PathCacheDirective("", "pool3")
      }));
      long ids1[] = new long[2];
      ids1[0] = addResults1.get(0).get().getEntryId();
      ids1[1] = addResults1.get(1).get().getEntryId();
      try {
        addResults1.get(2).get();
        Assert.fail("expected an error when adding an empty path");
      } catch (IOException ioe) {
        Assert.assertTrue(ioe.getCause() instanceof EmptyPathError);
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
          proto.listPathCacheEntries(0, "", 100);
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      iter = proto.listPathCacheEntries(0, "", 1);
      validateListAll(iter, ids1[0], ids1[1], ids2[0]);
      iter = proto.listPathCacheEntries(0, "pool3", 1);
      Assert.assertFalse(iter.hasNext());
      iter = proto.listPathCacheEntries(0, "pool2", 4444);
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
      iter = proto.listPathCacheEntries(0, "pool2", 4444);
      Assert.assertFalse(iter.hasNext());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
