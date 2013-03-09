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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
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
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot.DirectoryDiff;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test snapshot functionalities while file appending.
 */
public class TestINodeFileUnderConstructionWithSnapshot {
  {
    ((Log4JLogger)INode.LOG).getLogger().setLevel(Level.ALL);
    SnapshotTestHelper.disableLogs();
  }

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
   * Test snapshot after file appending
   */
  @Test (timeout=60000)
  public void testSnapshotAfterAppending() throws Exception {
    Path file = new Path(dir, "file");
    // 1. create snapshot --> create file --> append
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    INodeFile fileNode = (INodeFile) fsdir.getINode(file.toString());
    
    // 2. create snapshot --> modify the file --> append
    hdfs.createSnapshot(dir, "s1");
    hdfs.setReplication(file, (short) (REPLICATION - 1));
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    // check corresponding inodes
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    assertEquals(REPLICATION - 1, fileNode.getFileReplication());
    assertEquals(BLOCKSIZE * 3, fileNode.computeFileSize());

    // 3. create snapshot --> append
    hdfs.createSnapshot(dir, "s2");
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE);
    
    // check corresponding inodes
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    assertEquals(REPLICATION - 1,  fileNode.getFileReplication());
    assertEquals(BLOCKSIZE * 4, fileNode.computeFileSize());
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
  @Test (timeout=60000)
  public void testSnapshotWhileAppending() throws Exception {
    Path file = new Path(dir, "file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    
    // 1. append without closing stream --> create snapshot
    HdfsDataOutputStream out = appendFileWithoutClosing(file, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    out.close();
    
    // check: an INodeFileUnderConstructionWithSnapshot should be stored into s0's
    // deleted list, with size BLOCKSIZE*2
    INodeFile fileNode = (INodeFile) fsdir.getINode(file.toString());
    assertEquals(BLOCKSIZE * 2, fileNode.computeFileSize());
    INodeDirectorySnapshottable dirNode = (INodeDirectorySnapshottable) fsdir
        .getINode(dir.toString());
    DirectoryDiff last = dirNode.getDiffs().getLast();
    Snapshot s0 = last.snapshot;
    
    // 2. append without closing stream
    out = appendFileWithoutClosing(file, BLOCKSIZE);
    out.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    
    // re-check nodeInDeleted_S0
    dirNode = (INodeDirectorySnapshottable) fsdir.getINode(dir.toString());
    assertEquals(BLOCKSIZE * 2, fileNode.computeFileSize(s0));
    
    // 3. take snapshot --> close stream
    hdfs.createSnapshot(dir, "s1");
    out.close();
    
    // check: an INodeFileUnderConstructionWithSnapshot with size BLOCKSIZE*3 should
    // have been stored in s1's deleted list
    fileNode = (INodeFile) fsdir.getINode(file.toString());
    dirNode = (INodeDirectorySnapshottable) fsdir.getINode(dir.toString());
    last = dirNode.getDiffs().getLast();
    Snapshot s1 = last.snapshot;
    assertTrue(fileNode instanceof INodeFileWithSnapshot);
    assertEquals(BLOCKSIZE * 3, fileNode.computeFileSize(s1));
    
    // 4. modify file --> append without closing stream --> take snapshot -->
    // close stream
    hdfs.setReplication(file, (short) (REPLICATION - 1));
    out = appendFileWithoutClosing(file, BLOCKSIZE);
    hdfs.createSnapshot(dir, "s2");
    out.close();
    
    // re-check the size of nodeInDeleted_S1
    assertEquals(BLOCKSIZE * 3, fileNode.computeFileSize(s1));
  }  
}