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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Level;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * A JUnit test for corrupted file handling.
 */
public class TestFileCorruption {
  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }
  static Logger LOG = NameNode.stateChangeLog;

  /** check if DFS can handle corrupted blocks properly */
  @Test
  public void testFileCorruption() throws Exception {
    MiniDFSCluster cluster = null;
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFileCorruption").
        setNumFiles(20).build();
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      // Now deliberately remove the blocks
      String bpid = cluster.getNamesystem().getBlockPoolId();
      DataNode dn = cluster.getDataNodes().get(2);
      Map<DatanodeStorage, BlockListAsLongs> blockReports =
          dn.getFSDataset().getBlockReports(bpid);
      assertTrue("Blocks do not exist on data-dir", !blockReports.isEmpty());
      for (BlockListAsLongs report : blockReports.values()) {
        for (BlockReportReplica brr : report) {
          LOG.info("Deliberately removing block {}", brr.getBlockName());
          cluster.getFsDatasetTestUtils(2).getMaterializedReplica(
              new ExtendedBlock(bpid, brr)).deleteData();
        }
      }
      assertTrue("Corrupted replicas not handled properly.",
                 util.checkFiles(fs, "/srcdat"));
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** check if local FS can handle corrupted blocks properly */
  @Test
  public void testLocalFileCorruption() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Path file = new Path(PathUtils.getTestDirName(getClass()), "corruptFile");
    FileSystem fs = FileSystem.getLocal(conf);
    DataOutputStream dos = fs.create(file);
    dos.writeBytes("original bytes");
    dos.close();
    // Now deliberately corrupt the file
    dos = new DataOutputStream(new FileOutputStream(file.toString()));
    dos.writeBytes("corruption");
    dos.close();
    // Now attempt to read the file
    DataInputStream dis = fs.open(file, 512);
    try {
      LOG.info("A ChecksumException is expected to be logged.");
      dis.readByte();
    } catch (ChecksumException ignore) {
      //expect this exception but let any NPE get thrown
    }
    fs.delete(file, true);
  }
  
  /** Test the case that a replica is reported corrupt while it is not
   * in blocksMap. Make sure that ArrayIndexOutOfBounds does not thrown.
   * See Hadoop-4351.
   */
  @Test
  public void testArrayOutOfBoundsException() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      final Path FILE_PATH = new Path("/tmp.txt");
      final long FILE_LEN = 1L;
      DFSTestUtil.createFile(fs, FILE_PATH, FILE_LEN, (short)2, 1L);
      
      // get the block
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      ExtendedBlock blk = getFirstBlock(cluster.getDataNodes().get(0), bpid);
      assertFalse("Data directory does not contain any blocks or there was an "
          + "IO error", blk==null);

      // start a third datanode
      cluster.startDataNodes(conf, 1, true, null, null);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);
      DataNode dataNode = datanodes.get(2);
      
      // report corrupted block by the third datanode
      DatanodeRegistration dnR = InternalDataNodeTestUtils.
        getDNRegistrationForBP(dataNode, blk.getBlockPoolId());
      FSNamesystem ns = cluster.getNamesystem();
      ns.writeLock();
      try {
        cluster.getNamesystem().getBlockManager().findAndMarkBlockAsCorrupt(
            blk, new DatanodeInfo(dnR), "TEST", "STORAGE_ID");
      } finally {
        ns.writeUnlock();
      }
      
      // open the file
      fs.open(FILE_PATH);
      
      //clean up
      fs.delete(FILE_PATH, false);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCorruptionWithDiskFailure() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      FileSystem fs = cluster.getFileSystem();
      final Path FILE_PATH = new Path("/tmp.txt");
      final long FILE_LEN = 1L;
      DFSTestUtil.createFile(fs, FILE_PATH, FILE_LEN, (short) 3, 1L);

      // get the block
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      File storageDir = cluster.getInstanceStorageDir(0, 0);
      File dataDir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      assertTrue("Data directory does not exist", dataDir.exists());
      ExtendedBlock blk = getFirstBlock(cluster.getDataNodes().get(0), bpid);
      if (blk == null) {
        blk = getFirstBlock(cluster.getDataNodes().get(0), bpid);
      }
      assertFalse("Data directory does not contain any blocks or there was an" +
          " " +
          "IO error", blk == null);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);
      FSNamesystem ns = cluster.getNamesystem();
      //fail the storage on that node which has the block
      try {
        ns.writeLock();
        updateAllStorages(bm);
      } finally {
        ns.writeUnlock();
      }
      ns.writeLock();
      try {
        markAllBlocksAsCorrupt(bm, blk);
      } finally {
        ns.writeUnlock();
      }

      // open the file
      fs.open(FILE_PATH);

      //clean up
      fs.delete(FILE_PATH, false);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }

  }

  private void markAllBlocksAsCorrupt(BlockManager bm,
                                      ExtendedBlock blk) throws IOException {
    for (DatanodeStorageInfo info : bm.getStorages(blk.getLocalBlock())) {
      bm.findAndMarkBlockAsCorrupt(
          blk, info.getDatanodeDescriptor(), info.getStorageID(), "STORAGE_ID");
    }
  }

  private void updateAllStorages(BlockManager bm) {
    for (DatanodeDescriptor dd : bm.getDatanodeManager().getDatanodes()) {
      Set<DatanodeStorageInfo> setInfos = new HashSet<DatanodeStorageInfo>();
      DatanodeStorageInfo[] infos = dd.getStorageInfos();
      Random random = new Random();
      for (int i = 0; i < infos.length; i++) {
        int blkId = random.nextInt(101);
        DatanodeStorage storage = new DatanodeStorage(Integer.toString(blkId),
            DatanodeStorage.State.FAILED, StorageType.DISK);
        infos[i].updateFromStorage(storage);
        setInfos.add(infos[i]);
      }
    }
  }

  private static ExtendedBlock getFirstBlock(DataNode dn, String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReports =
        dn.getFSDataset().getBlockReports(bpid);
    for (BlockListAsLongs blockLongs : blockReports.values()) {
      for (BlockReportReplica block : blockLongs) {
        return new ExtendedBlock(bpid, block);
      }
    }
    return null;
  }
}
