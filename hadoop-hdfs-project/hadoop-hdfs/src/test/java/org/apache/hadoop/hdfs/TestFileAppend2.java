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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestFileAppend2 {

  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)NameNode.blockStateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int numBlocks = 5;
  final boolean simulatedStorage = false;

  private byte[] fileContents = null;

  final int numDatanodes = 6;
  final int numberOfFiles = 50;
  final int numThreads = 10;
  final int numAppendsPerThread = 20;
/***
  int numberOfFiles = 1;
  int numThreads = 1;
  int numAppendsPerThread = 2000;
****/
  Workload[] workload = null;
  final ArrayList<Path> testFiles = new ArrayList<Path>();
  volatile static boolean globalStatus = true;

  /**
   * Creates one file, writes a few bytes to it and then closed it.
   * Reopens the same file for appending, write all blocks and then close.
   * Verify that all data exists in file.
   * @throws IOException an exception might be thrown
   */ 
  @Test
  public void testSimpleAppend() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    try {
      { // test appending to a file.

        // create a new file.
        Path file1 = new Path("/simpleAppend.dat");
        FSDataOutputStream stm = AppendTestUtil.createFile(fs, file1, 1);
        System.out.println("Created file simpleAppend.dat");
  
        // write to file
        int mid = 186;   // io.bytes.per.checksum bytes
        System.out.println("Writing " + mid + " bytes to file " + file1);
        stm.write(fileContents, 0, mid);
        stm.close();
        System.out.println("Wrote and Closed first part of file.");
  
        // write to file
        int mid2 = 607;   // io.bytes.per.checksum bytes
        System.out.println("Writing " + mid + " bytes to file " + file1);
        stm = fs.append(file1);
        stm.write(fileContents, mid, mid2-mid);
        stm.close();
        System.out.println("Wrote and Closed second part of file.");
  
        // write the remainder of the file
        stm = fs.append(file1);

        // ensure getPos is set to reflect existing size of the file
        assertTrue(stm.getPos() > 0);

        System.out.println("Writing " + (AppendTestUtil.FILE_SIZE - mid2) +
            " bytes to file " + file1);
        stm.write(fileContents, mid2, AppendTestUtil.FILE_SIZE - mid2);
        System.out.println("Written second part of file");
        stm.close();
        System.out.println("Wrote and Closed second part of file.");
  
        // verify that entire file is good
        AppendTestUtil.checkFullFile(fs, file1, AppendTestUtil.FILE_SIZE,
            fileContents, "Read 2");
      }

      { // test appending to an non-existing file.
        FSDataOutputStream out = null;
        try {
          out = fs.append(new Path("/non-existing.dat"));
          fail("Expected to have FileNotFoundException");
        }
        catch(java.io.FileNotFoundException fnfe) {
          System.out.println("Good: got " + fnfe);
          fnfe.printStackTrace(System.out);
        }
        finally {
          IOUtils.closeStream(out);
        }
      }

      { // test append permission.

        //set root to all writable 
        Path root = new Path("/");
        fs.setPermission(root, new FsPermission((short)0777));
        fs.close();

        // login as a different user
        final UserGroupInformation superuser = 
          UserGroupInformation.getCurrentUser();
        String username = "testappenduser";
        String group = "testappendgroup";
        assertFalse(superuser.getShortUserName().equals(username));
        assertFalse(Arrays.asList(superuser.getGroupNames()).contains(group));
        UserGroupInformation appenduser = 
          UserGroupInformation.createUserForTesting(username, new String[]{group});
        
        fs = DFSTestUtil.getFileSystemAs(appenduser, conf);

        // create a file
        Path dir = new Path(root, getClass().getSimpleName());
        Path foo = new Path(dir, "foo.dat");
        FSDataOutputStream out = null;
        int offset = 0;
        try {
          out = fs.create(foo);
          int len = 10 + AppendTestUtil.nextInt(100);
          out.write(fileContents, offset, len);
          offset += len;
        }
        finally {
          IOUtils.closeStream(out);
        }

        // change dir and foo to minimal permissions.
        fs.setPermission(dir, new FsPermission((short)0100));
        fs.setPermission(foo, new FsPermission((short)0200));

        // try append, should success
        out = null;
        try {
          out = fs.append(foo);
          int len = 10 + AppendTestUtil.nextInt(100);
          out.write(fileContents, offset, len);
          offset += len;
        }
        finally {
          IOUtils.closeStream(out);
        }

        // change dir and foo to all but no write on foo.
        fs.setPermission(foo, new FsPermission((short)0577));
        fs.setPermission(dir, new FsPermission((short)0777));

        // try append, should fail
        out = null;
        try {
          out = fs.append(foo);
          fail("Expected to have AccessControlException");
        }
        catch(AccessControlException ace) {
          System.out.println("Good: got " + ace);
          ace.printStackTrace(System.out);
        }
        finally {
          IOUtils.closeStream(out);
        }
      }
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  //
  // an object that does a bunch of appends to files
  //
  class Workload extends Thread {
    private final int id;
    private final MiniDFSCluster cluster;

    Workload(MiniDFSCluster cluster, int threadIndex) {
      id = threadIndex;
      this.cluster = cluster;
    }

    // create a bunch of files. Write to them and then verify.
    @Override
    public void run() {
      System.out.println("Workload " + id + " starting... ");
      for (int i = 0; i < numAppendsPerThread; i++) {
   
        // pick a file at random and remove it from pool
        Path testfile;
        synchronized (testFiles) {
          if (testFiles.size() == 0) {
            System.out.println("Completed write to almost all files.");
            return;  
          }
          int index = AppendTestUtil.nextInt(testFiles.size());
          testfile = testFiles.remove(index);
        }

        long len = 0;
        int sizeToAppend = 0;
        try {
          FileSystem fs = cluster.getFileSystem();

          // add a random number of bytes to file
          len = fs.getFileStatus(testfile).getLen();

          // if file is already full, then pick another file
          if (len >= AppendTestUtil.FILE_SIZE) {
            System.out.println("File " + testfile + " is full.");
            continue;
          }
  
          // do small size appends so that we can trigger multiple
          // appends to the same file.
          //
          int left = (int)(AppendTestUtil.FILE_SIZE - len)/3;
          if (left <= 0) {
            left = 1;
          }
          sizeToAppend = AppendTestUtil.nextInt(left);

          System.out.println("Workload thread " + id +
                             " appending " + sizeToAppend + " bytes " +
                             " to file " + testfile +
                             " of size " + len);
          FSDataOutputStream stm = fs.append(testfile);
          stm.write(fileContents, (int)len, sizeToAppend);
          stm.close();

          // wait for the file size to be reflected in the namenode metadata
          while (fs.getFileStatus(testfile).getLen() != (len + sizeToAppend)) {
            try {
              System.out.println("Workload thread " + id +
                                 " file " + testfile  +
                                 " size " + fs.getFileStatus(testfile).getLen() +
                                 " expected size " + (len + sizeToAppend) +
                                 " waiting for namenode metadata update.");
              Thread.sleep(5000);
            } catch (InterruptedException e) {;}
          }

          assertTrue("File " + testfile + " size is " + 
                     fs.getFileStatus(testfile).getLen() +
                     " but expected " + (len + sizeToAppend),
                    fs.getFileStatus(testfile).getLen() == (len + sizeToAppend));

          AppendTestUtil.checkFullFile(fs, testfile, (int)(len + sizeToAppend),
              fileContents, "Read 2");
        } catch (Throwable e) {
          globalStatus = false;
          if (e.toString() != null) {
            System.out.println("Workload exception " + id + 
                               " testfile " + testfile +
                               " " + e);
            e.printStackTrace();
          }
          assertTrue("Workload exception " + id + " testfile " + testfile +
                     " expected size " + (len + sizeToAppend),
                     false);
        }

        // Add testfile back to the pool of files.
        synchronized (testFiles) {
          testFiles.add(testfile);
        }
      }
    }
  }

  /**
   * Test that appends to files at random offsets.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testComplexAppend() throws IOException {
    fileContents = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 30000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY, 30000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try {
      // create a bunch of test files with random replication factors.
      // Insert them into a linked list.
      //
      for (int i = 0; i < numberOfFiles; i++) {
        final int replication = AppendTestUtil.nextInt(numDatanodes - 2) + 1;
        Path testFile = new Path("/" + i + ".dat");
        FSDataOutputStream stm =
            AppendTestUtil.createFile(fs, testFile, replication);
        stm.close();
        testFiles.add(testFile);
      }

      // Create threads and make them run workload concurrently.
      workload = new Workload[numThreads];
      for (int i = 0; i < numThreads; i++) {
        workload[i] = new Workload(cluster, i);
        workload[i].start();
      }

      // wait for all transactions to get over
      for (int i = 0; i < numThreads; i++) {
        try {
          System.out.println("Waiting for thread " + i + " to complete...");
          workload[i].join();
          System.out.println("Waiting for thread " + i + " complete.");
        } catch (InterruptedException e) {
          i--;      // retry
        }
      }
    } finally {
      fs.close();
      cluster.shutdown();
    }

    // If any of the worker thread failed in their job, indicate that
    // this test failed.
    //
    assertTrue("testComplexAppend Worker encountered exceptions.", globalStatus);
  }
}
