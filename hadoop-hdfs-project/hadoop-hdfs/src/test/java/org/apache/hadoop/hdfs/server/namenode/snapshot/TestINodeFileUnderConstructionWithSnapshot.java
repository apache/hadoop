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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.ChildrenDiff;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test snapshot functionalities while file appending.
 */
public class TestINodeFileUnderConstructionWithSnapshot {

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final int BLOCKSIZE = 1024;

  private final Path dir = new Path("/TestSnapshot");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  FSDirectory fsdir;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = cluster.getFileSystem();
    hdfs.mkdirs(dir);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Check if the given nodes can form a circular list
   */
  private void checkCircularList(FileWithSnapshot... nodes) {
    for (int i = 0; i < nodes.length; i++) {
      FileWithSnapshot next = nodes[i].getNext();
      FileWithSnapshot expectedNext = nodes[(i + 1) % nodes.length];
      Assert.assertTrue(next == expectedNext);
    }
  }
  
  /**
   * Test snapshot after file appending
   */
  @Test
  public void testSnapshotAfterAppending() throws Exception {
    Path file = new Path(dir, "file");
    // 1. create snapshot --> create file --> append
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    // check the circular list and corresponding inodes: there should only be a
    // reference of the current node in the created list
    INodeFile fileNode = (INodeFile) fsdir.getINode(file.toString());
    INodeDirectorySnapshottable dirNode = (INodeDirectorySnapshottable) fsdir
        .getINode(dir.toString());
    ChildrenDiff diff = dirNode.getDiffs().getLast().getChildrenDiff();
    INode nodeInCreated = diff.searchCreated(fileNode.getLocalNameBytes());
    assertTrue(fileNode == nodeInCreated);
    INode nodeInDeleted = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertNull(nodeInDeleted);
    
    // 2. create snapshot --> modify the file --> append
    hdfs.createSnapshot(dir, "s1");
    hdfs.setReplication(file, (short) (REPLICATION - 1));
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    // check the circular list and corresponding inodes
    diff = dirNode.getDiffs().getLast().getChildrenDiff();
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    nodeInCreated = diff.searchCreated(fileNode.getLocalNameBytes());
    assertTrue(fileNode == nodeInCreated);
    assertEquals(REPLICATION - 1,
        ((INodeFile) nodeInCreated).getFileReplication());
    assertEquals(BLOCKSIZE * 3, ((INodeFile) fileNode).computeFileSize(true));
    nodeInDeleted = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertEquals(REPLICATION,
        ((INodeFile) nodeInDeleted).getFileReplication());
    assertEquals(BLOCKSIZE * 2,
        ((INodeFile) nodeInDeleted).computeFileSize(true));
    checkCircularList((INodeFileWithSnapshot) fileNode,
        (INodeFileSnapshot) nodeInDeleted);
    
    // 3. create snapshot --> append
    hdfs.createSnapshot(dir, "s2");
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    // check the circular list and corresponding inodes
    diff = dirNode.getDiffs().getLast().getChildrenDiff();
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    nodeInCreated = diff.searchCreated(fileNode.getLocalNameBytes());
    assertTrue(fileNode == nodeInCreated);
    assertEquals(REPLICATION - 1,
        ((INodeFile) nodeInCreated).getFileReplication());
    assertEquals(BLOCKSIZE * 4, ((INodeFile) fileNode).computeFileSize(true));
    INode nodeInDeleted2 = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertEquals(REPLICATION - 1,
        ((INodeFile) nodeInDeleted2).getFileReplication());
    assertEquals(BLOCKSIZE * 3,
        ((INodeFile) nodeInDeleted2).computeFileSize(true));
    checkCircularList((INodeFileWithSnapshot) fileNode,
        (INodeFileSnapshot) nodeInDeleted2, (INodeFileSnapshot) nodeInDeleted);
  }
  
  private HdfsDataOutputStream appendFileWithoutClosing(Path file, int length)
      throws IOException {
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    HdfsDataOutputStream out = (HdfsDataOutputStream) hdfs.append(file);
    out.write(toAppend);
    return out;
  }
  
  /**
   * Test snapshot during file appending, before the corresponding
   * {@link FSDataOutputStream} instance closes.
   */
  @Test
  public void testSnapshotWhileAppending() throws Exception {
    Path file = new Path(dir, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    
    // 1. append without closing stream --> create snapshot
    HdfsDataOutputStream out = appendFileWithoutClosing(file, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    out.close();
    
    // check: an INodeFileUnderConstructionSnapshot should be stored into s0's
    // deleted list, with size BLOCKSIZE*2
    INodeFile fileNode = (INodeFile) fsdir.getINode(file.toString());
    assertEquals(BLOCKSIZE * 2, ((INodeFile) fileNode).computeFileSize(true));
    INodeDirectorySnapshottable dirNode = (INodeDirectorySnapshottable) fsdir
        .getINode(dir.toString());
    ChildrenDiff diff = dirNode.getDiffs().getLast().getChildrenDiff();
    INode nodeInDeleted_S0 = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertTrue(nodeInDeleted_S0 instanceof INodeFileUnderConstructionSnapshot);
    assertEquals(BLOCKSIZE * 2,
        ((INodeFile) nodeInDeleted_S0).computeFileSize(true));
    
    // 2. append without closing stream
    out = appendFileWithoutClosing(file, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    
    // re-check nodeInDeleted_S0
    dirNode = (INodeDirectorySnapshottable) fsdir.getINode(dir.toString());
    diff = dirNode.getDiffs().getLast().getChildrenDiff();
    nodeInDeleted_S0 = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertTrue(nodeInDeleted_S0 instanceof INodeFileUnderConstructionSnapshot);
    assertEquals(BLOCKSIZE * 2,
        ((INodeFile) nodeInDeleted_S0).computeFileSize(true));
    
    // 3. take snapshot --> close stream
    hdfs.createSnapshot(dir, "s1");
    out.close();
    
    // check: an INodeFileUnderConstructionSnapshot with size BLOCKSIZE*3 should
    // have been stored in s1's deleted list
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    dirNode = (INodeDirectorySnapshottable) fsdir.getINode(dir.toString());
    diff = dirNode.getDiffs().getLast().getChildrenDiff();
    INode nodeInCreated_S1 = diff.searchCreated(fileNode.getLocalNameBytes());
    assertTrue(fileNode == nodeInCreated_S1);
    assertTrue(fileNode instanceof INodeFileWithSnapshot);
    INode nodeInDeleted_S1 = diff.searchDeleted(fileNode.getLocalNameBytes());
    assertTrue(nodeInDeleted_S1 instanceof INodeFileUnderConstructionSnapshot);
    assertEquals(BLOCKSIZE * 3,
        ((INodeFile) nodeInDeleted_S1).computeFileSize(true));
    // also check the circular linked list
    checkCircularList((INodeFileWithSnapshot) fileNode,
        (INodeFileUnderConstructionSnapshot) nodeInDeleted_S1,
        (INodeFileUnderConstructionSnapshot) nodeInDeleted_S0);
    
    // 4. modify file --> append without closing stream --> take snapshot -->
    // close stream
    hdfs.setReplication(file, (short) (REPLICATION - 1));
    out = appendFileWithoutClosing(file, BLOCKSIZE);
    hdfs.createSnapshot(dir, "s2");
    out.close();
    
    // re-check the size of nodeInDeleted_S1
    assertEquals(BLOCKSIZE * 3,
        ((INodeFile) nodeInDeleted_S1).computeFileSize(true));
  }  
}