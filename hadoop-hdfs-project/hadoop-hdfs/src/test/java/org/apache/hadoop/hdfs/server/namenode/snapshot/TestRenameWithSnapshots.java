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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Testing rename with snapshots. */
public class TestRenameWithSnapshots {
  {
    SnapshotTestHelper.disableLogs();
  }

  private static final long SEED = 0;

  private static final short REPL = 3;
  private static final long BLOCKSIZE = 1024;
  
  private static Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;
  
  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();

    hdfs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test (timeout=300000)
  public void testRenameWithSnapshot() throws Exception {
    final String dirStr = "/testRenameWithSnapshot";
    final String abcStr = dirStr + "/abc";
    final Path abc = new Path(abcStr);
    hdfs.mkdirs(abc, new FsPermission((short)0777));
    hdfs.allowSnapshot(abcStr);

    final Path foo = new Path(abc, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(abc, "s0");
    final INode fooINode = fsdir.getINode(foo.toString());

    final String xyzStr = dirStr + "/xyz";
    final Path xyz = new Path(xyzStr);
    hdfs.mkdirs(xyz, new FsPermission((short)0777));
    final Path bar = new Path(xyz, "bar");
    hdfs.rename(foo, bar);
    
    final INode fooRef = fsdir.getINode(
        SnapshotTestHelper.getSnapshotPath(abc, "s0", "foo").toString());
    Assert.assertTrue(fooRef.isReference());
    Assert.assertTrue(fooRef.asReference() instanceof INodeReference.WithName);

    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)fooRef.asReference().getReferredINode();
    Assert.assertEquals(2, withCount.getReferenceCount());

    final INode barRef = fsdir.getINode(bar.toString());
    Assert.assertTrue(barRef.isReference());

    Assert.assertSame(withCount, barRef.asReference().getReferredINode());
    Assert.assertSame(fooINode, withCount.asReference().getReferredINode());
    
    hdfs.delete(bar, false);
    Assert.assertEquals(1, withCount.getReferenceCount());
  }
}
