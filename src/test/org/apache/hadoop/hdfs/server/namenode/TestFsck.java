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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.Exception;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.IOUtils;

/**
 * A JUnit test for doing fsck
 */
public class TestFsck extends TestCase {
  static String runFsck(Configuration conf, int expectedErrCode, 
                        boolean checkErrorCode,String... path) 
                        throws Exception {
    PrintStream oldOut = System.out;
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(bStream, true);
    System.setOut(newOut);
    ((Log4JLogger)PermissionChecker.LOG).getLogger().setLevel(Level.ALL);
    int errCode = ToolRunner.run(new DFSck(conf), path);
    if (checkErrorCode)
      assertEquals(expectedErrCode, errCode);
    ((Log4JLogger)PermissionChecker.LOG).getLogger().setLevel(Level.INFO);
    System.setOut(oldOut);
    return bStream.toString();
  }

  /** do fsck */
  public void testFsck() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      conf.setLong("dfs.blockreport.intervalMsec", 10000L);
      cluster = new MiniDFSCluster(conf, 4, true, null);
      fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      util.waitReplication(fs, "/srcdat", (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      System.out.println(outStr);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
      
      // restart the cluster; bring up namenode but not the data nodes
      cluster = new MiniDFSCluster(conf, 0, false, null);
      outStr = runFsck(conf, 1, true, "/");
      // expect the result is corrupt
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      System.out.println(outStr);
      
      // bring up data nodes & cleanup cluster
      cluster.startDataNodes(conf, 4, true, null, null);
      cluster.waitActive();
      cluster.waitClusterUp();
      fs = cluster.getFileSystem();
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testFsckNonExistent() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 20, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      conf.setLong("dfs.blockreport.intervalMsec", 10000L);
      cluster = new MiniDFSCluster(conf, 4, true, null);
      fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      util.waitReplication(fs, "/srcdat", (short)3);
      String outStr = runFsck(conf, 0, true, "/non-existent");
      assertEquals(-1, outStr.indexOf(NamenodeFsck.HEALTHY_STATUS));
      System.out.println(outStr);
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testFsckMove() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 5, 3, 8*1024);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      conf.setLong("dfs.blockreport.intervalMsec", 10000L);
      cluster = new MiniDFSCluster(conf, 4, true, null);
      String topDir = "/srcdat";
      fs = cluster.getFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      
      // Corrupt a block by deleting it
      String[] fileNames = util.getFileNames(topDir);
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                          cluster.getNameNodePort()), conf);
      String block = dfsClient.namenode.
                      getBlockLocations(fileNames[0], 0, Long.MAX_VALUE).
                      get(0).getBlock().getBlockName();
      File baseDir = new File(System.getProperty("test.build.data",
                                                 "build/test/data"),"dfs/data");
      for (int i=0; i<8; i++) {
        File blockFile = new File(baseDir, "data" +(i+1)+ "/current/" + block);
        if(blockFile.exists()) {
          assertTrue(blockFile.delete());
        }
      }

      // We excpect the filesystem to be corrupted
      outStr = runFsck(conf, 1, false, "/");
      while (!outStr.contains(NamenodeFsck.CORRUPT_STATUS)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
        outStr = runFsck(conf, 1, false, "/");
      } 
      
      // Fix the filesystem by moving corrupted files to lost+found
      outStr = runFsck(conf, 1, true, "/", "-move");
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      
      // Check to make sure we have healthy filesystem
      outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS)); 
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  public void testFsckOpenFiles() throws Exception {
    DFSTestUtil util = new DFSTestUtil("TestFsck", 4, 3, 8*1024); 
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      conf.setLong("dfs.blockreport.intervalMsec", 10000L);
      cluster = new MiniDFSCluster(conf, 4, true, null);
      String topDir = "/srcdat";
      String randomString = "HADOOP  ";
      fs = cluster.getFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      // Open a file for writing and do not close for now
      Path openFile = new Path(topDir + "/openFile");
      FSDataOutputStream out = fs.create(openFile);
      int writeCount = 0;
      while (writeCount != 100) {
        out.write(randomString.getBytes());
        writeCount++;                  
      }
      // We expect the filesystem to be HEALTHY and show one open file
      outStr = runFsck(conf, 0, true, topDir);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE")); 
      // Use -openforwrite option to list open files
      outStr = runFsck(conf, 0, true, topDir, "-openforwrite");
      System.out.println(outStr);
      assertTrue(outStr.contains("OPENFORWRITE"));
      assertTrue(outStr.contains("openFile"));
      // Close the file
      out.close(); 
      // Now, fsck should show HEALTHY fs and should not show any open files
      outStr = runFsck(conf, 0, true, topDir);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE"));
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCorruptBlock() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000);
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    Random random = new Random();
    String outStr = null;

    MiniDFSCluster cluster = null;
    try {
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    Path file1 = new Path("/testCorruptBlock");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 0);
    // Wait until file replication has completed
    DFSTestUtil.waitReplication(fs, file1, (short)3);
    String block = DFSTestUtil.getFirstBlock(fs, file1).getBlockName();

    // Make sure filesystem is in healthy state
    outStr = runFsck(conf, 0, true, "/");
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    
    // corrupt replicas 
    File baseDir = new File(System.getProperty("test.build.data",
                                               "build/test/data"),"dfs/data");
    for (int i=0; i < 6; i++) {
      File blockFile = new File(baseDir, "data" + (i+1) + "/current/" +
                                block);
      if (blockFile.exists()) {
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int)channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }
    }
    // Read the file to trigger reportBadBlocks
    try {
      IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), conf,
                        true);
    } catch (IOException ie) {
      // Ignore exception
    }

    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                               cluster.getNameNodePort()), conf);
    blocks = dfsClient.namenode.
               getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    replicaCount = blocks.get(0).getLocations().length;
    while (replicaCount != 3) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
      blocks = dfsClient.namenode.
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }
    assertTrue (blocks.get(0).isCorrupt());

    // Check if fsck reports the same
    outStr = runFsck(conf, 1, true, "/");
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
    assertTrue(outStr.contains("testCorruptBlock"));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /** Test if fsck can return -1 in case of failure
   * 
   * @throws Exception
   */
  public void testFsckError() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      // bring up a one-node cluster
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      String fileName = "/test.txt";
      Path filePath = new Path(fileName);
      FileSystem fs = cluster.getFileSystem();
      
      // create a one-block file
      DFSTestUtil.createFile(fs, filePath, 1L, (short)1, 1L);
      DFSTestUtil.waitReplication(fs, filePath, (short)1);
      
      // intentionally corrupt NN data structure
      INodeFile node = (INodeFile)cluster.getNameNode().namesystem.dir.rootDir.getNode(fileName);
      assertEquals(node.blocks.length, 1);
      node.blocks[0].setNumBytes(-1L);  // set the block length to be negative
      
      // run fsck and expect a failure with -1 as the error code
      String outStr = runFsck(conf, -1, true, fileName);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.FAILURE_STATUS));
      
      // clean up file system
      fs.delete(filePath, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
