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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that datanodes can correctly handle errors during block read/write.
 */
public class TestDiskError {

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  /**
   * Test to check that a DN goes down when all its volumes have failed.
   */
  @Test
  public void testShutdown() throws Exception {
    if (System.getProperty("os.name").startsWith("Windows")) {
      /**
       * This test depends on OS not allowing file creations on a directory
       * that does not have write permissions for the user. Apparently it is 
       * not the case on Windows (at least under Cygwin), and possibly AIX.
       * This is disabled on Windows.
       */
      return;
    }
    // Bring up two more datanodes
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();
    final int dnIndex = 0;
    String bpid = cluster.getNamesystem().getBlockPoolId();
    File storageDir = cluster.getInstanceStorageDir(dnIndex, 0);
    File dir1 = MiniDFSCluster.getRbwDir(storageDir, bpid);
    storageDir = cluster.getInstanceStorageDir(dnIndex, 1);
    File dir2 = MiniDFSCluster.getRbwDir(storageDir, bpid);
    try {
      // make the data directory of the first datanode to be readonly
      assertTrue("Couldn't chmod local vol", dir1.setReadOnly());
      assertTrue("Couldn't chmod local vol", dir2.setReadOnly());

      // create files and make sure that first datanode will be down
      DataNode dn = cluster.getDataNodes().get(dnIndex);
      for (int i=0; dn.isDatanodeUp(); i++) {
        Path fileName = new Path("/test.txt"+i);
        DFSTestUtil.createFile(fs, fileName, 1024, (short)2, 1L);
        DFSTestUtil.waitReplication(fs, fileName, (short)2);
        fs.delete(fileName, true);
      }
    } finally {
      // restore its old permission
      FileUtil.setWritable(dir1, true);
      FileUtil.setWritable(dir2, true);
    }
  }

  /**
   * Test that when there is a failure replicating a block the temporary
   * and meta files are cleaned up and subsequent replication succeeds.
   */
  @Test
  public void testReplicationError() throws Exception {
    // create a file of replication factor of 1
    final Path fileName = new Path("/test.txt");
    final int fileLen = 1;
    DFSTestUtil.createFile(fs, fileName, 1, (short)1, 1L);
    DFSTestUtil.waitReplication(fs, fileName, (short)1);

    // get the block belonged to the created file
    LocatedBlocks blocks = NameNodeAdapter.getBlockLocations(
        cluster.getNameNode(), fileName.toString(), 0, (long)fileLen);
    assertEquals("Should only find 1 block", blocks.locatedBlockCount(), 1);
    LocatedBlock block = blocks.get(0);

    // bring up a second datanode
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();
    final int sndNode = 1;
    DataNode datanode = cluster.getDataNodes().get(sndNode);
    
    // replicate the block to the second datanode
    InetSocketAddress target = datanode.getXferAddress();
    Socket s = new Socket(target.getAddress(), target.getPort());
    // write the header.
    DataOutputStream out = new DataOutputStream(s.getOutputStream());

    DataChecksum checksum = DataChecksum.newDataChecksum(
        DataChecksum.Type.CRC32, 512);
    new Sender(out).writeBlock(block.getBlock(), StorageType.DEFAULT,
        BlockTokenSecretManager.DUMMY_TOKEN, "",
        new DatanodeInfo[0], new StorageType[0], null,
        BlockConstructionStage.PIPELINE_SETUP_CREATE, 1, 0L, 0L, 0L,
        checksum, CachingStrategy.newDefaultStrategy(), false, false, null);
    out.flush();

    // close the connection before sending the content of the block
    out.close();

    // the temporary block & meta files should be deleted
    String bpid = cluster.getNamesystem().getBlockPoolId();
    File storageDir = cluster.getInstanceStorageDir(sndNode, 0);
    File dir1 = MiniDFSCluster.getRbwDir(storageDir, bpid);
    storageDir = cluster.getInstanceStorageDir(sndNode, 1);
    File dir2 = MiniDFSCluster.getRbwDir(storageDir, bpid);
    while (dir1.listFiles().length != 0 || dir2.listFiles().length != 0) {
      Thread.sleep(100);
    }

    // then increase the file's replication factor
    fs.setReplication(fileName, (short)2);
    // replication should succeed
    DFSTestUtil.waitReplication(fs, fileName, (short)1);

    // clean up the file
    fs.delete(fileName, false);
  }

  /**
   * Check that the permissions of the local DN directories are as expected.
   */
  @Test
  public void testLocalDirs() throws Exception {
    Configuration conf = new Configuration();
    final String permStr = conf.get(
      DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY);
    FsPermission expected = new FsPermission(permStr);

    // Check permissions on directories in 'dfs.datanode.data.dir'
    FileSystem localFS = FileSystem.getLocal(conf);
    for (DataNode dn : cluster.getDataNodes()) {
      for (FsVolumeSpi v : dn.getFSDataset().getVolumes()) {
        String dir = v.getBasePath();
        Path dataDir = new Path(dir);
        FsPermission actual = localFS.getFileStatus(dataDir).getPermission();
          assertEquals("Permission for dir: " + dataDir + ", is " + actual +
              ", while expected is " + expected, expected, actual);
      }
    }
  }
  
  /**
   * Checks whether {@link DataNode#checkDiskErrorAsync()} is being called or not.
   * Before refactoring the code the above function was not getting called 
   * @throws IOException, InterruptedException
   */
  @Test
  public void testcheckDiskError() throws IOException, InterruptedException {
    if(cluster.getDataNodes().size() <= 0) {
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();
    }
    DataNode dataNode = cluster.getDataNodes().get(0);
    long slackTime = dataNode.checkDiskErrorInterval/2;
    //checking for disk error
    dataNode.checkDiskErrorAsync();
    Thread.sleep(dataNode.checkDiskErrorInterval);
    long lastDiskErrorCheck = dataNode.getLastDiskErrorCheck();
    assertTrue("Disk Error check is not performed within  " + dataNode.checkDiskErrorInterval +  "  ms", ((Time.monotonicNow()-lastDiskErrorCheck) < (dataNode.checkDiskErrorInterval + slackTime)));
  }
}
