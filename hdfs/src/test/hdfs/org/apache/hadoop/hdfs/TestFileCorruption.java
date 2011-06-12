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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

/**
 * A JUnit test for corrupted file handling.
 */
public class TestFileCorruption extends TestCase {
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
  }
  static Log LOG = ((Log4JLogger)NameNode.stateChangeLog);

  /** check if DFS can handle corrupted blocks properly */
  public void testFileCorruption() throws Exception {
    MiniDFSCluster cluster = null;
    DFSTestUtil util = new DFSTestUtil("TestFileCorruption", 20, 3, 8*1024);
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      // Now deliberately remove the blocks
      File data_dir = new File(System.getProperty("test.build.data"),
                               "dfs/data/data5/current");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (!blocks[idx].getName().startsWith("blk_")) {
          continue;
        }
        System.out.println("Deliberately removing file "+blocks[idx].getName());
        assertTrue("Cannot remove file.", blocks[idx].delete());
      }
      assertTrue("Corrupted replicas not handled properly.",
                 util.checkFiles(fs, "/srcdat"));
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** check if local FS can handle corrupted blocks properly */
  public void testLocalFileCorruption() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Path file = new Path(System.getProperty("test.build.data"), "corruptFile");
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
      System.out.println("A ChecksumException is expected to be logged.");
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
  public void testArrayOutOfBoundsException() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      final Path FILE_PATH = new Path("/tmp.txt");
      final long FILE_LEN = 1L;
      DFSTestUtil.createFile(fs, FILE_PATH, FILE_LEN, (short)2, 1L);
      
      // get the block
      File dataDir = new File(cluster.getDataDirectory(),
          "data1" + MiniDFSCluster.FINALIZED_DIR_NAME);
      Block blk = getBlock(dataDir);
      if (blk == null) {
        blk = getBlock(new File(cluster.getDataDirectory(),
          "dfs/data/data2" + MiniDFSCluster.FINALIZED_DIR_NAME));
      }
      assertFalse(blk==null);

      // start a third datanode
      cluster.startDataNodes(conf, 1, true, null, null);
      ArrayList<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 3);
      DataNode dataNode = datanodes.get(2);
      
      // report corrupted block by the third datanode
      cluster.getNamesystem().markBlockAsCorrupt(blk, 
          new DatanodeInfo(dataNode.dnRegistration ));
      
      // open the file
      fs.open(FILE_PATH);
      
      //clean up
      fs.delete(FILE_PATH, false);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
    
  }
  
  private Block getBlock(File dataDir) {
    assertTrue("data directory does not exist", dataDir.exists());
    File[] blocks = dataDir.listFiles();
    assertTrue("Blocks do not exist in dataDir", (blocks != null) && (blocks.length > 0));

    int idx = 0;
    String blockFileName = null;
    for (; idx < blocks.length; idx++) {
      blockFileName = blocks[idx].getName();
      if (blockFileName.startsWith("blk_") && !blockFileName.endsWith(".meta")) {
        break;
      }
    }
    if (blockFileName == null) {
      return null;
    }
    long blockId = Long.parseLong(blockFileName.substring("blk_".length()));
    long blockTimeStamp = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    for (idx=0; idx < blocks.length; idx++) {
      String fileName = blocks[idx].getName();
      if (fileName.startsWith(blockFileName) && fileName.endsWith(".meta")) {
        int startIndex = blockFileName.length()+1;
        int endIndex = fileName.length() - ".meta".length();
        blockTimeStamp = Long.parseLong(fileName.substring(startIndex, endIndex));
        break;
      }
    }
    return new Block(blockId, blocks[idx].length(), blockTimeStamp);
  }

  /** check if ClientProtocol.getCorruptFiles() returns a file that has missing blocks */
  public void testCorruptFilesMissingBlock() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans directories
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000); // datanode sends block reports
      cluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil("testCorruptFilesMissingBlock", 2, 1, 512);
      util.createFiles(fs, "/srcdat");

      // verify that there are no bad blocks.
      ClientProtocol namenode = DFSClient.createNamenode(conf);
      FileStatus[] badFiles = namenode.getCorruptFiles();
      assertTrue("Namenode has " + badFiles.length + " corrupt files. Expecting none.",
          badFiles.length == 0);

      // Now deliberately remove one block
      File data_dir = new File(System.getProperty("test.build.data"),
      "dfs/data/data1/current/finalized");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (!blocks[idx].getName().startsWith("blk_")) {
          continue;
        }
        LOG.info("Deliberately removing file "+blocks[idx].getName());
        assertTrue("Cannot remove file.", blocks[idx].delete());
        break;
      }

      badFiles = namenode.getCorruptFiles();
      while (badFiles.length == 0) {
        Thread.sleep(1000);
        badFiles = namenode.getCorruptFiles();
      }
      LOG.info("Namenode has bad files. " + badFiles.length);
      assertTrue("Namenode has " + badFiles.length + " bad files. Expecting 1.",
          badFiles.length == 1);
      util.cleanup(fs, "/srcdat");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** check if ClientProtocol.getCorruptFiles() returns the right limit */
  public void testMaxCorruptFiles() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt("dfs.corruptfilesreturned.max", 2);
      conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans directories
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000); // datanode sends block reports
      cluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil("testMaxCorruptFiles", 4, 1, 512);
      util.createFiles(fs, "/srcdat2");

      // verify that there are no bad blocks.
      ClientProtocol namenode = DFSClient.createNamenode(conf);
      FileStatus[] badFiles = namenode.getCorruptFiles();
      assertTrue("Namenode has " + badFiles.length + " corrupt files. Expecting none.",
          badFiles.length == 0);

      // Now deliberately remove one block
      File data_dir = new File(System.getProperty("test.build.data"),
      "dfs/data/data1/current/finalized");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (!blocks[idx].getName().startsWith("blk_")) {
          continue;
        }
        LOG.info("Deliberately removing file "+blocks[idx].getName());
        assertTrue("Cannot remove file.", blocks[idx].delete());
      }

      badFiles = namenode.getCorruptFiles();
      while (badFiles.length < 2) {
        badFiles = namenode.getCorruptFiles();
        Thread.sleep(10000);
      }
      badFiles = namenode.getCorruptFiles(); // once more since time has passed
      LOG.info("Namenode has bad files. " + badFiles.length);
      assertTrue("Namenode has " + badFiles.length + " bad files. Expecting 2.",
          badFiles.length == 2);
      util.cleanup(fs, "/srcdat2");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** check if ClientProtocol.getCorruptFiles() returns a file that has corrupted blocks */
  public void testCorruptFilesCorruptedBlock() throws Exception {
    MiniDFSCluster cluster = null;
    Random random = new Random();
    
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt("dfs.datanode.directoryscan.interval", 1); // datanode scans directories
      conf.setInt("dfs.blockreport.intervalMsec", 3 * 1000); // datanode sends block reports
      cluster = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fs = cluster.getFileSystem();

      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil("testCorruptFilesCorruptedBlock", 2, 1, 512);
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      ClientProtocol namenode = DFSClient.createNamenode(conf);
      FileStatus[] badFiles = namenode.getCorruptFiles();
      assertTrue("Namenode has " + badFiles.length + " corrupt files. Expecting None.",
          badFiles.length == 0);

      // Now deliberately corrupt one block
      File data_dir = new File(System.getProperty("test.build.data"),
      "dfs/data/data1/current/finalized");
      assertTrue("data directory does not exist", data_dir.exists());
      File[] blocks = data_dir.listFiles();
      assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length > 0));
      for (int idx = 0; idx < blocks.length; idx++) {
        if (blocks[idx].getName().startsWith("blk_") &&
            blocks[idx].getName().endsWith(".meta")) {
          //
          // shorten .meta file
          //
          RandomAccessFile file = new RandomAccessFile(blocks[idx], "rw");
          FileChannel channel = file.getChannel();
          long position = channel.size() - 2;
          int length = 2;
          byte[] buffer = new byte[length];
          random.nextBytes(buffer);
          channel.write(ByteBuffer.wrap(buffer), position);
          file.close();
          LOG.info("Deliberately corrupting file " + blocks[idx].getName() +
              " at offset " + position + " length " + length);

          // read all files to trigger detection of corrupted replica
          try {
            util.checkFiles(fs, "/srcdat10");
          } catch (BlockMissingException e) {
            System.out.println("Received BlockMissingException as expected.");
          } catch (IOException e) {
            assertTrue("Corrupted replicas not handled properly. Expecting BlockMissingException " +
                " but received IOException " + e, false);
          }
          break;
        }
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = namenode.getCorruptFiles();
      LOG.info("Namenode has bad files. " + badFiles.length);
      assertTrue("Namenode has " + badFiles.length + " bad files. Expecting 1.",
          badFiles.length == 1);
      util.cleanup(fs, "/srcdat10");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
