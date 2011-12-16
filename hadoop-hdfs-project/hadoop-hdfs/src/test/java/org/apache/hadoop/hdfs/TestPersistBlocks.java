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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.Random;
import static org.junit.Assert.*;
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
  
  /** check if DFS remains in proper condition after a restart */
  @Test
  public void testRestartDfs() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
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
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
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
      LocatedBlocks blocks = dfsclient.getNamenode().getBlockLocations(
          FILE_NAME, 0, BLOCK_SIZE * NUM_BLOCKS);
      assertEquals(NUM_BLOCKS, blocks.getLocatedBlocks().size());
      LocatedBlock b = blocks.getLastLocatedBlock();
      dfsclient.getNamenode().abandonBlock(b.getBlock(), FILE_NAME,
          dfsclient.clientName);
      
      // explicitly do NOT close the file.
      cluster.restartNameNode();
      
      // Check that the file has no less bytes than before the restart
      // This would mean that blocks were successfully persisted to the log
      FileStatus status = fs.getFileStatus(FILE_PATH);
      assertTrue("Length incorrect: " + status.getLen(),
          status.getLen() != len - BLOCK_SIZE);

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
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      FileSystem fs = cluster.getFileSystem();
      NameNode.getAddress(conf).getPort();
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
    conf.setBoolean(DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY, true);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
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
}
