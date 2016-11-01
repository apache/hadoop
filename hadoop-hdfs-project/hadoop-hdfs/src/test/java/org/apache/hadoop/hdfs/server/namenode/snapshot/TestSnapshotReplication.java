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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the replication handling/calculation of snapshots to make
 * sure the number of replication is calculated correctly with/without
 * snapshots.
 */
public class TestSnapshotReplication {
  
  private static final long seed = 0;
  private static final short REPLICATION = 3;
  private static final int NUMDATANODE = 5;
  private static final long BLOCKSIZE = 1024;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  FSDirectory fsdir;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUMDATANODE)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    fsdir = fsn.getFSDirectory();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  /**
   * Check the replication of a given file.
   *
   * @param file The given file
   * @param replication The expected replication number
   * @param blockReplication The expected replication number for the block
   * @throws Exception
   */
  private void checkFileReplication(Path file, short replication,
      short blockReplication) throws Exception {
    // Get FileStatus of file1, and identify the replication number of file1.
    // Note that the replication number in FileStatus was derived from
    // INodeFile#getFileReplication().
    short fileReplication = hdfs.getFileStatus(file1).getReplication();
    assertEquals(replication, fileReplication);
    // Check the correctness of getPreferredBlockReplication()
    INode inode = fsdir.getINode(file1.toString());
    assertTrue(inode instanceof INodeFile);
    for (BlockInfo b: inode.asFile().getBlocks()) {
      assertEquals(blockReplication, b.getReplication());
    }
  }
  
  /**
   * Test replication number calculation for a normal file without snapshots.
   */
  @Test (timeout=60000)
  public void testReplicationWithoutSnapshot() throws Exception {
    // Create file1, set its replication to REPLICATION
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    // Check the replication of file1
    checkFileReplication(file1, REPLICATION, REPLICATION);
    // Change the replication factor of file1 from 3 to 2
    hdfs.setReplication(file1, (short) (REPLICATION - 1));
    // Check the replication again
    checkFileReplication(file1, (short) (REPLICATION - 1),
        (short) (REPLICATION - 1));
  }
  
  INodeFile getINodeFile(Path p) throws Exception {
    final String s = p.toString();
    return INodeFile.valueOf(fsdir.getINode(s), s);
  }
 
  /**
   * Check the replication for both the current file and all its prior snapshots
   * 
   * @param currentFile
   *          the Path of the current file
   * @param snapshotRepMap
   *          A map maintaining all the snapshots of the current file, as well
   *          as their expected replication number stored in their corresponding
   *          INodes
   * @param expectedBlockRep
   *          The expected replication number
   * @throws Exception
   */
  private void checkSnapshotFileReplication(Path currentFile,
      Map<Path, Short> snapshotRepMap, short expectedBlockRep) throws Exception {
    // First check the getPreferredBlockReplication for the INode of
    // the currentFile
    final INodeFile inodeOfCurrentFile = getINodeFile(currentFile);
    for (BlockInfo b : inodeOfCurrentFile.getBlocks()) {
      assertEquals(expectedBlockRep, b.getReplication());
    }
    // Then check replication for every snapshot
    for (Path ss : snapshotRepMap.keySet()) {
      final INodesInPath iip = fsdir.getINodesInPath(ss.toString(), DirOp.READ);
      final INodeFile ssInode = iip.getLastINode().asFile();
      // The replication number derived from the
      // INodeFileWithLink#getPreferredBlockReplication should
      // always == expectedBlockRep
      for (BlockInfo b : ssInode.getBlocks()) {
        assertEquals(expectedBlockRep, b.getReplication());
      }
      // Also check the number derived from INodeFile#getFileReplication
      assertEquals(snapshotRepMap.get(ss).shortValue(),
          ssInode.getFileReplication(iip.getPathSnapshotId()));
    }
  }
  
  /**
   * Test replication number calculation for a file with snapshots.
   */
  @Test (timeout=60000)
  public void testReplicationWithSnapshot() throws Exception {
    short fileRep = 1;
    // Create file1, set its replication to 1
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, fileRep, seed);
    Map<Path, Short> snapshotRepMap = new HashMap<Path, Short>();
    // Change replication factor from 1 to 5. In the meanwhile, keep taking
    // snapshots for sub1
    for (; fileRep < NUMDATANODE; ) {
      // Create snapshot for sub1
      Path snapshotRoot = SnapshotTestHelper.createSnapshot(hdfs, sub1, "s"
          + fileRep);
      Path snapshot = new Path(snapshotRoot, file1.getName());
      
      // Check the replication stored in the INode of the snapshot of file1
      assertEquals(fileRep, getINodeFile(snapshot).getFileReplication());
      snapshotRepMap.put(snapshot, fileRep);
      
      // Increase the replication factor by 1
      hdfs.setReplication(file1, ++fileRep);
      // Check the replication for file1
      checkFileReplication(file1, fileRep, fileRep);
      // Also check the replication for all the prior snapshots of file1
      checkSnapshotFileReplication(file1, snapshotRepMap, fileRep);
    }
    
    // Change replication factor back to 3.
    hdfs.setReplication(file1, REPLICATION);
    // Check the replication for file1
    // Currently the max replication among snapshots should be 4
    checkFileReplication(file1, REPLICATION, (short) (NUMDATANODE - 1));
    // Also check the replication for all the prior snapshots of file1.
    // Currently the max replication among snapshots should be 4
    checkSnapshotFileReplication(file1, snapshotRepMap,
        (short) (NUMDATANODE - 1));
  }
  
  /**
   * Test replication for a file with snapshots, also including the scenario
   * where the original file is deleted
   */
  @Test (timeout=60000)
  public void testReplicationAfterDeletion() throws Exception {
    // Create file1, set its replication to 3
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    Map<Path, Short> snapshotRepMap = new HashMap<Path, Short>();
    // Take 3 snapshots of sub1
    for (int i = 1; i <= 3; i++) {
      Path root = SnapshotTestHelper.createSnapshot(hdfs, sub1, "s" + i);
      Path ssFile = new Path(root, file1.getName());
      snapshotRepMap.put(ssFile, REPLICATION);
    }
    // Check replication
    checkFileReplication(file1, REPLICATION, REPLICATION);
    checkSnapshotFileReplication(file1, snapshotRepMap, REPLICATION);
    
    // Delete file1
    hdfs.delete(file1, true);
    // Check replication of snapshots
    for (Path ss : snapshotRepMap.keySet()) {
      final INodeFile ssInode = getINodeFile(ss);
      // The replication number derived from the
      // INodeFileWithLink#getPreferredBlockReplication should
      // always == expectedBlockRep
      for (BlockInfo b : ssInode.getBlocks()) {
        assertEquals(REPLICATION, b.getReplication());
      }

      // Also check the number derived from INodeFile#getFileReplication
      assertEquals(snapshotRepMap.get(ss).shortValue(),
          ssInode.getFileReplication());
    }
  }
  
}
