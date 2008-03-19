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
import java.util.Random;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.BlockLocation;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestDatanodeDeath extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  static final int numDatanodes = 15;
  static final short replication = 3;

  int numberOfFiles = 3;
  int numThreads = 5;
  Workload[] workload = null;

  //
  // an object that does a bunch of transactions
  //
  class Workload extends Thread {
    private short replication;
    private int numberOfFiles;
    private int id;
    private FileSystem fs;
    private long stamp;

    Workload(FileSystem fs, int threadIndex, int numberOfFiles, 
             short replication, long stamp) {
      id = threadIndex;
      this.fs = fs;
      this.numberOfFiles = numberOfFiles;
      this.replication = replication;
      this.stamp = stamp;
    }

    // create a bunch of files. Write to them and then verify.
    public void run() {
      System.out.println("Workload starting ");
      for (int i = 0; i < numberOfFiles; i++) {
        Path filename = new Path(id + "." + i);
        long myseed = seed + id + i;
        try {
          System.out.println("Workload processing file " + filename);
          FSDataOutputStream stm = createFile(fs, filename, replication);
          DFSClient.DFSOutputStream dfstream = (DFSClient.DFSOutputStream)
                                                 (stm.getWrappedStream());
          dfstream.setArtificialSlowdown(1000);
          writeFile(stm, myseed);
          stm.close();
          checkFile(fs, filename, replication, numBlocks, fileSize, myseed);
        } catch (Throwable e) {
          System.out.println("Workload exception " + e);
          assertTrue(e.toString(), false);
        }

        // increment the stamp to indicate that another file is done.
        synchronized (this) {
          stamp++;
        }
      }
    }

    public synchronized void resetStamp() {
      this.stamp = 0;
    }

    public synchronized long getStamp() {
      return stamp;
    }
  }

  //
  // creates a file and returns a descriptor for writing to it.
  //
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, short repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file
  //
  private void writeFile(FSDataOutputStream stm, long seed) throws IOException {
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    int mid = fileSize/2;
    stm.write(buffer, 0, mid);
    stm.write(buffer, mid, fileSize - mid);
  }


  // wait till this block is confirmed by the datanodes. 
  private void waitBlockConfirmation(FileSystem fileSys, Path name, 
                                     int repl, int blockNumber) 
                                     throws IOException {
    boolean done = false;
    long start = blockSize * blockNumber;
    long end = blockSize * (blockNumber + 1) -1;

    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(name, start, 
                                                                end);
      if (locations.length < 1) {
        done = false;
        continue;
      }
      if (locations[0].getHosts().length < repl) {
        done = false;
        continue;
      }
    }
  }

  /**
   * For blocks that reside on the nodes that are down, verify that their
   * replication factor is 1 more than the specified one.
   */
  private DatanodeInfo[] getPipeline(FileSystem fileSys, Path name, 
                                     short repl,
                                     int blockNumber) 
                                     throws IOException {
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(), 
               fileSys instanceof DistributedFileSystem);

    DFSClient.DFSDataInputStream dis = (DFSClient.DFSDataInputStream)
      ((DistributedFileSystem)fileSys).open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    int num = 0;
    DatanodeInfo[] status = null;

    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) {     // for each replica
        System.out.println("Block " + blk.getBlock() + " replica " +
                           nodes[j].getName());
      }
      if (blockNumber == num) {
        status = nodes;
      }
      num++;
    }
    return status;
  }

  //
  // verify that the data written are sane
  // 
  private void checkFile(FileSystem fileSys, Path name, int repl,
                         int numblocks, int filesize, long seed)
    throws IOException {
    boolean done = false;
    int attempt = 0;

    long len = fileSys.getFileStatus(name).getLen();
    assertTrue(name + " should be of size " + filesize +
               " but found to be of size " + len, 
               len == filesize);

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      attempt++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(name, 0, 
                                                                filesize);

      if (locations.length < numblocks) {
        if (attempt > 100) {
          System.out.println("File " + name + " has only " +
                             locations.length + " blocks, " +
                             " but is expected to have " + numblocks +
                             " blocks.");
        }
        done = false;
        continue;
      }
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].getHosts().length < repl) {
          if (attempt > 100) {
            System.out.println("File " + name + " has " +
                               locations.length + " blocks: " +
                               " The " + idx + " block has only " +
                               locations[idx].getHosts().length + 
                               " replicas but is expected to have " 
                               + repl + " replicas.");
          }
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[filesize];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    // do a sanity check. Read the file
    byte[] actual = new byte[filesize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 1");
  }

  private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      this.assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }

  /**
   * A class that kills one datanode and recreates a new one. It waits to
   * ensure that that all workers have finished at least one file since the 
   * last kill of a datanode. This guarantees that all three replicas of
   * a block do not get killed (otherwise the file will be corrupt and the
   * test will fail).
   */
  class Modify extends Thread {
    Random rand;
    volatile boolean running;
    MiniDFSCluster cluster;
    Configuration conf;

    Modify(Configuration conf, MiniDFSCluster cluster) {
      rand = new Random();
      running = true;
      this.cluster = cluster;
      this.conf = conf;
    }

    public void run() {

      while (running) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          continue;
        }

        // check if all threads have a new stamp. 
        // If so, then all workers have finished at least one file
        // since the last stamp.
        boolean loop = false;
        for (int i = 0; i < numThreads; i++) {
          if (workload[i].getStamp() == 0) {
            loop = true;
            break;
          }
        }
        if (loop) {
          continue;
        }

        // Now it is guaranteed that there will be at least one valid
        // replica of a file.

        for (int i = 0; i < replication - 1; i++) {
          // pick a random datanode to shutdown
          int victim = rand.nextInt(numDatanodes);
          try {
            System.out.println("Stopping datanode " + victim);
            cluster.restartDataNode(victim);
            // cluster.startDataNodes(conf, 1, true, null, null);
          } catch (IOException e) {
            System.out.println("TestDatanodeDeath Modify exception " + e);
            assertTrue("TestDatanodeDeath Modify exception " + e, false);
            running = false;
          }
        }

        // set a new stamp for all workers
        for (int i = 0; i < numThreads; i++) {
          workload[i].resetStamp();
        }
      }
    }

    // Make the thread exit.
    void close() {
      running = false;
      this.interrupt();
    }
  }

  /**
   * Test that writing to files is good even when datanodes in the pipeline
   * dies.
   */
  private void complexTest() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 2);
    conf.setInt("dfs.replication.pending.timeout.sec", 2);
    conf.setInt("dfs.socket.timeout", 5000);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    Modify modThread = null;

    try {
      
      // Create threads and make them run workload concurrently.
      workload = new Workload[numThreads];
      for (int i = 0; i < numThreads; i++) {
        workload[i] = new Workload(fs, i, numberOfFiles, replication, 0);
        workload[i].start();
      }

      // Create a thread that kills existing datanodes and creates new ones.
      modThread = new Modify(conf, cluster);
      modThread.start();

      // wait for all transactions to get over
      for (int i = 0; i < numThreads; i++) {
        try {
          System.out.println("Waiting for thread " + i + " to complete...");
          workload[i].join();

          // if most of the threads are done, then stop restarting datanodes.
          if (i >= numThreads/2) {
            modThread.close();
          }
         
        } catch (InterruptedException e) {
          i--;      // retry
        }
      }
    } finally {
      if (modThread != null) {
        modThread.close();
        try {
          modThread.join();
        } catch (InterruptedException e) {}
      }
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Write to one file, then kill one datanode in the pipeline and then
   * close the file.
   */
  private void simpleTest(int datanodeToKill) throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 2000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.replication.pending.timeout.sec", 2);
    conf.setInt("dfs.socket.timeout", 5000);
    int myMaxNodes = 5;
    System.out.println("SimpleTest starting with DataNode to Kill " + 
                       datanodeToKill);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, myMaxNodes, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    short repl = 3;

    Path filename = new Path("simpletest.dat");
    Random rand = new Random();
    long myseed = rand.nextInt();
    rand = new Random(myseed);
    try {

      // create a file and write one block of data
      System.out.println("SimpleTest creating file " + filename);
      FSDataOutputStream stm = createFile(fs, filename, repl);
      DFSClient.DFSOutputStream dfstream = (DFSClient.DFSOutputStream)
                                             (stm.getWrappedStream());

      // these are test settings
      int bytesPerChecksum = conf.getInt( "io.bytes.per.checksum", 512); 
      dfstream.setChunksPerPacket(5);
      dfstream.setArtificialSlowdown(3000);

      byte[] buffer = new byte[fileSize];
      rand.nextBytes(buffer);
      int mid = fileSize/4;
      stm.write(buffer, 0, mid);

      DatanodeInfo[] targets = dfstream.getPipeline();
      int count = 5;
      while (count-- > 0 && targets == null) {
        try {
          System.out.println("SimpleTest: Waiting for pipeline to be created.");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        targets = dfstream.getPipeline();
      }

      if (targets == null) {
        int victim = rand.nextInt(myMaxNodes);
        System.out.println("SimpleTest stopping datanode random " + victim);
        cluster.stopDataNode(victim);
      } else {
        int victim = datanodeToKill;
        System.out.println("SimpleTest stopping datanode " +
                            targets[victim].getName());
        cluster.stopDataNode(targets[victim].getName());
      }
      System.out.println("SimpleTest stopping datanode complete");

      // write some more data to file, close and verify
      stm.write(buffer, mid, fileSize - mid);
      stm.close();

      checkFile(fs, filename, repl, numBlocks, fileSize, myseed);
    } catch (Throwable e) {
      System.out.println("Simple Workload exception " + e);
      e.printStackTrace();
      assertTrue(e.toString(), false);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  public void testDatanodeDeath() throws IOException {
    for (int i = 0; i < 3; i++) {
      simpleTest(i); // kills the ith datanode in the pipeline
    }
    complexTest();
  }
}
