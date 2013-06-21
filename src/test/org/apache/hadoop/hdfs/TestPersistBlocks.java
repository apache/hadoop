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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * A JUnit test for checking if restarting DFS preserves the
 * blocks that are part of an unclosed file.
 */
public class TestPersistBlocks {
  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final String HADOOP_1_0_MULTIBLOCK_TGZ =
      "hadoop-1.0-multiblock-file.tgz";
  
  private static final int BLOCK_SIZE = 4096;
  private static final int NUM_BLOCKS = 5;

  private static final String FILE_NAME = "/data";
  private static final Path FILE_PATH = new Path(FILE_NAME);
  
  static final byte[] DATA_BEFORE_RESTART = new byte[BLOCK_SIZE * NUM_BLOCKS];
  static final byte[] DATA_AFTER_RESTART = new byte[BLOCK_SIZE * NUM_BLOCKS];
  

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
  @Test
  public void TestRestartDfsWithSync() throws Exception {
    testRestartDfs(false);
  }
  
  /**
   * check if DFS remains in proper condition after a restart
   * @param useFlush - if true then flush is used instead of sync (ie hflush)
   */
  void testRestartDfs(boolean useFlush) throws Exception {
    final Configuration conf = new Configuration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt("ipc.client.connection.maxidletime", 0);
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;

    long len = 0;
    FSDataOutputStream stream;
    try {
      // small safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      cluster = new MiniDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART);
      if (useFlush)
        stream.flush();
      else 
         stream.sync();
      
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
  public void testRestartWithPartialBlockHflushed() throws IOException {
    final Configuration conf = new Configuration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt("ipc.client.connection.maxidletime", 0);
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      // small safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      cluster = new MiniDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode.getAddress(conf).getPort();
      // Creating a file with 4096 blockSize to write multiple blocks
      stream = fs.create(FILE_PATH, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
      stream.write(DATA_BEFORE_RESTART);
      stream.write((byte)1);
      stream.sync();
      
      // explicitly do NOT close the file before restarting the NN.
      cluster.restartNameNode();
      
      // this will fail if the final block of the file is prematurely COMPLETEd
      stream.write((byte)2);
      stream.sync(); // hflush was called sync in 20 append
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
    final Configuration conf = new Configuration();
    conf.set("dfs.safemode.extension", "1");
    conf.setBoolean("dfs.support.broken.append", true);
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      // small safemode extension to make the test run faster.
      conf.set("dfs.safemode.extension", "1");
      cluster = new MiniDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode.getAddress(conf).getPort();
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
  

  static void assertFileExists(File f) {
    Assert.assertTrue("File " + f + " should exist", f.exists());
  }
  
  static String readFile(FileSystem fs, Path fileName) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(fileName), os, 1024, true);
    return os.toString();
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
    final Configuration conf = new Configuration();
        
    String tarFile = System.getProperty("test.cache.data", "build/test/cache")
      + "/" + HADOOP_1_0_MULTIBLOCK_TGZ;
    String testDir = System.getProperty("test.build.data", "build/test/data");
    File dfsDir = new File(testDir, "image-1.0");
    if (dfsDir.exists() && !FileUtil.fullyDelete(dfsDir)) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    FileUtil.unTar(new File(tarFile), new File(testDir));

    File nameDir = new File(dfsDir, "name");
    assertFileExists(nameDir);
    File dataDir = new File(dfsDir, "data");
    assertFileExists(dataDir);
    
    conf.set("dfs.name.dir", nameDir.getAbsolutePath());
    conf.set("dfs.data.dir", dataDir.getAbsolutePath());
    
    conf.setBoolean("dfs.support.broken.append", true);
    // small safemode extension to make the test run faster.
    conf.set("dfs.safemode.extension", "1");
    MiniDFSCluster cluster = new  MiniDFSCluster(0, conf, 1, false, false,
        StartupOption.UPGRADE,
        null);
    cluster.waitActive();

    try {
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/user/todd/4blocks");
      // Read it without caring about the actual data within - we just need
      // to make sure that the block states and locations are OK.
      readFile(fs, testPath);
      
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

