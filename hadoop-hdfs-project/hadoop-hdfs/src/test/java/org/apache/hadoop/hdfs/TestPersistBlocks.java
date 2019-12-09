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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * A JUnit test for checking if restarting DFS preserves the
 * blocks that are part of an unclosed file.
 */
public class TestPersistBlocks {
  static {
    GenericTestUtils.setLogLevel(FSImage.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.TRACE);
  }
  
  private static final int BLOCK_SIZE = 4096;
  private static final int NUM_BLOCKS = 5;

  private static final String FILE_NAME = "/data";
  private static final Path FILE_PATH = new Path(FILE_NAME);
  
  static final byte[] DATA_BEFORE_RESTART = new byte[BLOCK_SIZE * NUM_BLOCKS];
  static final byte[] DATA_AFTER_RESTART = new byte[BLOCK_SIZE * NUM_BLOCKS];
  
  private static final String HADOOP_1_0_MULTIBLOCK_TGZ =
    "hadoop-1.0-multiblock-file.tgz";
  static {
    Random rand = new Random();
    rand.nextBytes(DATA_BEFORE_RESTART);
    rand.nextBytes(DATA_AFTER_RESTART);
  }
 
  /** check if DFS remains in proper condition after a restart 
   **/
  @Test  
  public void TestRestartDfsWithFlush() throws Exception {
    testRestartDfs(true);
  }
  
  
  /** check if DFS remains in proper condition after a restart 
   **/
  public void TestRestartDfsWithSync() throws Exception {
    testRestartDfs(false);
  }
  
  /** check if DFS remains in proper condition after a restart
   * @param useFlush - if true then flush is used instead of sync (ie hflush)
   */
  void testRestartDfs(boolean useFlush) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    long len = 0;
    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART);
      if (useFlush)
        stream.flush();
      else 
        stream.hflush();
      
      // Wait for at least a few blocks to get through
      while (len <= BLOCK_SIZE) {
        FileStatus status = fs.getFileStatus(FILE_PATH);
        len = status.getLen();
        Thread.sleep(100);
      }
      
      // explicitly do NOT close the file.
      cluster.restartNameNode();
      
      // Check that the file has no less bytes than before the restart
      // This would mean that blocks were successfully persisted to the log
      FileStatus status = fs.getFileStatus(FILE_PATH);
      assertTrue("Length too short: " + status.getLen(),
          status.getLen() >= len);
      
      // And keep writing (ensures that leases are also persisted correctly)
      stream.write(DATA_AFTER_RESTART);
      stream.close();
      
      // Verify that the data showed up, both from before and after the restart.
      FSDataInputStream readStream = fs.open(FILE_PATH);
      try {
        byte[] verifyBuf = new byte[DATA_BEFORE_RESTART.length];
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        assertArrayEquals(DATA_BEFORE_RESTART, verifyBuf);
        
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        assertArrayEquals(DATA_AFTER_RESTART, verifyBuf);
      } finally {
        IOUtils.closeStream(readStream);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testRestartDfsWithAbandonedBlock() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    long len = 0;
    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART);
      stream.hflush();
      
      // Wait for all of the blocks to get through
      while (len < BLOCK_SIZE * (NUM_BLOCKS - 1)) {
        FileStatus status = fs.getFileStatus(FILE_PATH);
        len = status.getLen();
        Thread.sleep(100);
      }
      
      // Abandon the last block
      DFSClient dfsclient = DFSClientAdapter.getDFSClient((DistributedFileSystem)fs);
      HdfsFileStatus fileStatus = dfsclient.getNamenode().getFileInfo(FILE_NAME);
      LocatedBlocks blocks = dfsclient.getNamenode().getBlockLocations(
          FILE_NAME, 0, BLOCK_SIZE * NUM_BLOCKS);
      assertEquals(NUM_BLOCKS, blocks.getLocatedBlocks().size());
      LocatedBlock b = blocks.getLastLocatedBlock();
      dfsclient.getNamenode().abandonBlock(b.getBlock(), fileStatus.getFileId(),
          FILE_NAME, dfsclient.clientName);
      
      // explicitly do NOT close the file.
      cluster.restartNameNode();
      
      // Check that the file has no less bytes than before the restart
      // This would mean that blocks were successfully persisted to the log
      FileStatus status = fs.getFileStatus(FILE_PATH);
      assertTrue("Length incorrect: " + status.getLen(),
          status.getLen() == len - BLOCK_SIZE);

      // Verify the data showed up from before restart, sans abandoned block.
      FSDataInputStream readStream = fs.open(FILE_PATH);
      try {
        byte[] verifyBuf = new byte[DATA_BEFORE_RESTART.length - BLOCK_SIZE];
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        byte[] expectedBuf = new byte[DATA_BEFORE_RESTART.length - BLOCK_SIZE];
        System.arraycopy(DATA_BEFORE_RESTART, 0,
            expectedBuf, 0, expectedBuf.length);
        assertArrayEquals(expectedBuf, verifyBuf);
      } finally {
        IOUtils.closeStream(readStream);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testRestartWithPartialBlockHflushed() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      DFSUtilClient.getNNAddress(conf).getPort();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART);
      stream.write((byte)1);
      stream.hflush();
      
      // explicitly do NOT close the file before restarting the NN.
      cluster.restartNameNode();
      
      // this will fail if the final block of the file is prematurely COMPLETEd
      stream.write((byte)2);
      stream.hflush();
      stream.close();
      
      assertEquals(DATA_BEFORE_RESTART.length + 2,
          fs.getFileStatus(FILE_PATH).getLen());
      
      FSDataInputStream readStream = fs.open(FILE_PATH);
      try {
        byte[] verifyBuf = new byte[DATA_BEFORE_RESTART.length + 2];
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        byte[] expectedBuf = new byte[DATA_BEFORE_RESTART.length + 2];
        System.arraycopy(DATA_BEFORE_RESTART, 0, expectedBuf, 0,
            DATA_BEFORE_RESTART.length);
        System.arraycopy(new byte[]{1, 2}, 0, expectedBuf,
            DATA_BEFORE_RESTART.length, 2);
        assertArrayEquals(expectedBuf, verifyBuf);
      } finally {
        IOUtils.closeStream(readStream);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testRestartWithAppend() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      DFSUtilClient.getNNAddress(conf).getPort();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART, 0, DATA_BEFORE_RESTART.length / 2);
      stream.close();
      stream = fs.append(FILE_PATH, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART, DATA_BEFORE_RESTART.length / 2,
          DATA_BEFORE_RESTART.length / 2);
      stream.close();
      
      assertEquals(DATA_BEFORE_RESTART.length,
          fs.getFileStatus(FILE_PATH).getLen());
      
      cluster.restartNameNode();
      
      assertEquals(DATA_BEFORE_RESTART.length,
          fs.getFileStatus(FILE_PATH).getLen());
      
      FSDataInputStream readStream = fs.open(FILE_PATH);
      try {
        byte[] verifyBuf = new byte[DATA_BEFORE_RESTART.length];
        IOUtils.readFully(readStream, verifyBuf, 0, verifyBuf.length);
        assertArrayEquals(DATA_BEFORE_RESTART, verifyBuf);
      } finally {
        IOUtils.closeStream(readStream);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /**
   * Earlier versions of HDFS didn't persist block allocation to the edit log.
   * This makes sure that we can still load an edit log when the OP_CLOSE
   * is the opcode which adds all of the blocks. This is a regression
   * test for HDFS-2773.
   * This test uses a tarred pseudo-distributed cluster from Hadoop 1.0
   * which has a multi-block file. This is similar to the tests in
   * {@link TestDFSUpgradeFromImage} but none of those images include
   * a multi-block file.
   */
  @Test
  public void testEarlierVersionEditLog() throws Exception {
    final Configuration conf = new HdfsConfiguration();
        
    String tarFile = System.getProperty("test.cache.data", "build/test/cache")
      + "/" + HADOOP_1_0_MULTIBLOCK_TGZ;
    String testDir = PathUtils.getTestDirName(getClass());
    File dfsDir = new File(testDir, "image-1.0");
    if (dfsDir.exists() && !FileUtil.fullyDelete(dfsDir)) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    FileUtil.unTar(new File(tarFile), new File(testDir));

    File nameDir = new File(dfsDir, "name");
    GenericTestUtils.assertExists(nameDir);
    File dataDir = new File(dfsDir, "data");
    GenericTestUtils.assertExists(dataDir);
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir.getAbsolutePath());
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
      .format(false)
      .manageDataDfsDirs(false)
      .manageNameDfsDirs(false)
      .numDataNodes(1)
      .startupOption(StartupOption.UPGRADE)
      .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/user/todd/4blocks");
      // Read it without caring about the actual data within - we just need
      // to make sure that the block states and locations are OK.
      DFSTestUtil.readFile(fs, testPath);
      
      // Ensure that we can append to it - if the blocks were in some funny
      // state we'd get some kind of issue here. 
      FSDataOutputStream stm = fs.append(testPath);
      try {
        stm.write(1);
      } finally {
        IOUtils.closeStream(stm);
      }
    } finally {
      cluster.shutdown();
    }
  }
}
