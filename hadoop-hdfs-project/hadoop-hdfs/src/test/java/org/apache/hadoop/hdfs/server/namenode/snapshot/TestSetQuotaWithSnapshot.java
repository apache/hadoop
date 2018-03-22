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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature.DirectoryDiff;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestSetQuotaWithSnapshot {
  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final long BLOCKSIZE = 1024;
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected FSDirectory fsdir;
  protected DistributedFileSystem hdfs;
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  @Test (timeout=60000)
  public void testSetQuota() throws Exception {
    final Path dir = new Path("/TestSnapshot");
    hdfs.mkdirs(dir);
    // allow snapshot on dir and create snapshot s1
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    
    Path sub = new Path(dir, "sub");
    hdfs.mkdirs(sub);
    Path fileInSub = new Path(sub, "file");
    DFSTestUtil.createFile(hdfs, fileInSub, BLOCKSIZE, REPLICATION, seed);
    INodeDirectory subNode = INodeDirectory.valueOf(
        fsdir.getINode(sub.toString()), sub);
    // subNode should be a INodeDirectory, but not an INodeDirectoryWithSnapshot
    assertFalse(subNode.isWithSnapshot());
    
    hdfs.setQuota(sub, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1);
    subNode = INodeDirectory.valueOf(fsdir.getINode(sub.toString()), sub);
    assertTrue(subNode.isQuotaSet());
    assertFalse(subNode.isWithSnapshot());
  }
  
  /**
   * Test clear quota of a snapshottable dir or a dir with snapshot.
   */
  @Test
  public void testClearQuota() throws Exception {
    final Path dir = new Path("/TestSnapshot");
    hdfs.mkdirs(dir);
    
    hdfs.allowSnapshot(dir);
    hdfs.setQuota(dir, HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
    INodeDirectory dirNode = fsdir.getINode4Write(dir.toString()).asDirectory();
    assertTrue(dirNode.isSnapshottable());
    assertEquals(0, dirNode.getDiffs().asList().size());
    
    hdfs.setQuota(dir, HdfsConstants.QUOTA_DONT_SET - 1,
        HdfsConstants.QUOTA_DONT_SET - 1);
    dirNode = fsdir.getINode4Write(dir.toString()).asDirectory();
    assertTrue(dirNode.isSnapshottable());
    assertEquals(0, dirNode.getDiffs().asList().size());
    
    hdfs.setQuota(dir, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_RESET);
    dirNode = fsdir.getINode4Write(dir.toString()).asDirectory();
    assertTrue(dirNode.isSnapshottable());
    assertEquals(0, dirNode.getDiffs().asList().size());
    
    // allow snapshot on dir and create snapshot s1
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s1");
    
    // clear quota of dir
    hdfs.setQuota(dir, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_RESET);
    // dir should still be a snapshottable directory
    dirNode = fsdir.getINode4Write(dir.toString()).asDirectory();
    assertTrue(dirNode.isSnapshottable());
    assertEquals(1, dirNode.getDiffs().asList().size());
    SnapshottableDirectoryStatus[] status = hdfs.getSnapshottableDirListing();
    assertEquals(1, status.length);
    assertEquals(dir, status[0].getFullPath());
    
    final Path subDir = new Path(dir, "sub");
    hdfs.mkdirs(subDir);
    hdfs.createSnapshot(dir, "s2");
    final Path file = new Path(subDir, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    hdfs.setQuota(dir, HdfsConstants.QUOTA_RESET, HdfsConstants.QUOTA_RESET);
    INode subNode = fsdir.getINode4Write(subDir.toString());
    assertTrue(subNode.asDirectory().isWithSnapshot());
    DiffList<DirectoryDiff> diffList =
        subNode.asDirectory().getDiffs().asList();
    assertEquals(1, diffList.size());
    Snapshot s2 = dirNode.getSnapshot(DFSUtil.string2Bytes("s2"));
    final DirectoryDiff diff = diffList.get(0);
    assertEquals(s2.getId(), diff.getSnapshotId());
    List<INode> createdList = diff.getChildrenDiff().getCreatedUnmodifiable();
    assertEquals(1, createdList.size());
    assertSame(fsdir.getINode4Write(file.toString()), createdList.get(0));
  }
}
