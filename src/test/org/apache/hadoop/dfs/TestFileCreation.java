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
package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import java.io.*;
import java.net.*;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.dfs.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.BlockLocation;


/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestFileCreation extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  // The test file is 2 times the blocksize plus one. This means that when the
  // entire file is written, the first two blocks definitely get flushed to
  // the datanodes.

  //
  // creates a file but does not close it
  //
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
  }

  //
  // writes specified bytes to file.
  //
  private void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer, 0, size);
  }

  //
  // verify that the data written to the full blocks are sane
  // 
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(name, 0, 
                                                                fileSize);
      if (locations.length < numBlocks) {
        done = false;
        continue;
      }
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].getHosts().length < repl) {
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[numBlocks * blockSize];
    if (simulatedStorage) {
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      Random rand = new Random(seed);
      rand.nextBytes(expected);
    }
    // do a sanity check. Read the file
    byte[] actual = new byte[numBlocks * blockSize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 1");
  }

  private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }

  /**
   * Test that file data becomes available before file is closed.
   */
  public void testFileCreation() throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.isDirectory(path));
      System.out.println(fs.getFileStatus(path).isDir()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file filestatus.dat with one "
                         + " replicas.");

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");

      // write to file
      writeFile(stm);

      // Make sure a client can read it before it is closed.
      checkFile(fs, file1, 1);

      // verify that file size has changed
      long len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + (numBlocks * blockSize) +
                 " but found to be of size " + len, 
                  len == numBlocks * blockSize);

      stm.close();

      // verify that file size has changed to the full size
      len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + fileSize +
                 " but found to be of size " + len, 
                  len == fileSize);
      
      
      // Check storage usage 
      // can't check capacities for real storage since the OS file system may be changing under us.
      if (simulatedStorage) {
        DataNode dn = cluster.getDataNodes().get(0);
        assertEquals(fileSize, dn.getFSDataset().getDfsUsed());
        assertEquals(SimulatedFSDataset.DEFAULT_CAPACITY-fileSize, dn.getFSDataset().getRemaining());
      }
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that file data does not become corrupted even in the face of errors.
   */
  public void testFileCreationError1() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    try {

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("testFileCreationError1: "
                         + "Created file filestatus.dat with one "
                         + " replicas.");

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");

      // kill the datanode
      cluster.shutdownDataNodes();

      // wait for the datanode to be declared dead
      while (true) {
        DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
        if (info.length == 0) {
          break;
        }
        System.out.println("testFileCreationError1: waiting for datanode " +
                           " to die.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }

      // write 1 byte to file. 
      // This should fail because all datanodes are dead.
      byte[] buffer = new byte[1];
      Random rand = new Random(seed);
      rand.nextBytes(buffer);
      try {
        stm.write(buffer);
        stm.close();
      } catch (Exception e) {
        System.out.println("Encountered expected exception");
      }

      // verify that no blocks are associated with this file
      // bad block allocations were cleaned up earlier.
      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up",
                 locations.locatedBlockCount() == 0);
    } finally {
      fs.close();
      cluster.shutdown();
      client.close();
    }
  }

  /**
   * Test that the filesystem removes the last block from a file if its
   * lease expires.
   */
  public void testFileCreationError2() throws IOException {
    long leasePeriod = 1000;
    System.out.println("testFileCreationError2 start");
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    try {

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      createFile(fs, file1, 1);
      System.out.println("testFileCreationError2: "
                         + "Created file filestatus.dat with one "
                         + " replicas.");

      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("The file has " + locations.locatedBlockCount() +
                         " blocks.");

      // add another block to the file
      LocatedBlock location = client.namenode.addBlock(file1.toString(), 
                                                       null);
      System.out.println("Added block " + location.getBlock());

      locations = client.namenode.getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      System.out.println("The file now has " + locations.locatedBlockCount() +
                         " blocks.");
      
      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);

      // wait for the lease to expire
      try {
        Thread.sleep(5 * leasePeriod);
      } catch (InterruptedException e) {
      }

      // verify that the last block was cleaned up.
      locations = client.namenode.getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up",
                 locations.locatedBlockCount() == 0);
      System.out.println("testFileCreationError2 successful");
    } finally {
      try {
        fs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
      client.close();
    }
  }

  /**
   * Test that file leases are persisted across namenode restarts.
   */
  public void testFileCreationNamenodeRestart() throws IOException {
    Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    cluster.waitActive();
    int nnport = cluster.getNameNodePort();
    InetSocketAddress addr = new InetSocketAddress("localhost", nnport);

    DFSClient client = null;
    try {

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file filestatus.dat with one "
                         + " replicas.");

      // write two full blocks.
      writeFile(stm, numBlocks * blockSize);
      stm.flush();

      // create another new file.
      //
      Path file2 = new Path("/filestatus2.dat");
      FSDataOutputStream stm2 = createFile(fs, file2, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file filestatus2.dat with one "
                         + " replicas.");

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2*MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();

      // write 1 byte to file.  This should succeed because the 
      // namenode should have persisted leases.
      byte[] buffer = new byte[1];
      Random rand = new Random(seed);
      rand.nextBytes(buffer);
      stm.write(buffer);
      stm.close();
      stm2.write(buffer);
      stm2.close();

      // verify that new block is associated with this file
      client = new DFSClient(addr, conf);
      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file1,
                 locations.locatedBlockCount() == 3);

      // verify filestatus2.dat
      locations = client.namenode.getBlockLocations(
                                  file2.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file2,
                 locations.locatedBlockCount() == 1);
    } finally {
      fs.close();
      cluster.shutdown();
      if (client != null)  client.close();
    }
  }

  /**
   * Test that all open files are closed when client dies abnormally.
   */
  public void testDFSClientDeath() throws IOException {
    Configuration conf = new Configuration();
    System.out.println("Testing adbornal client death.");
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsclient = dfs.dfs;
    try {

      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("/clienttest.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file clienttest.dat");

      // write to file
      writeFile(stm);

      // close the dfsclient before closing the output stream.
      // This should close all existing file.
      dfsclient.close();

      try {
        fs.close();
        fs = null;
      } catch (IOException e) {
      }

      // reopen file system and verify that file exists.
      fs = cluster.getFileSystem();
      assertTrue(file1 + " does not exist.", fs.exists(file1));

    } finally {
      if (fs != null) {
        fs.close();
      }
      cluster.shutdown();
    }
  }

/**
 * Test that file data becomes available before file is closed.
 */
  public void testFileCreationSimulated() throws IOException {
    simulatedStorage = true;
    testFileCreation();
    simulatedStorage = false;
  }

}
