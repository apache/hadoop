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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.MockitoUtil;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This class tests the access time on files.
 *
 */
public class TestSetTimes {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 1;

  static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  Random myrand = new Random();
  Path hostsFile;
  Path excludeFile;

  private FSDataOutputStream writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    return stm;
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }

  /**
   * Tests mod & access time in DFS.
   */
  @Test
  public void testTimes() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);


    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    final int nnport = cluster.getNameNodePort();
    InetSocketAddress addr = new InetSocketAddress("localhost", 
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    int replicas = 1;
    assertTrue(fileSys instanceof DistributedFileSystem);

    try {
      //
      // create file and record atime/mtime
      //
      System.out.println("Creating testdir1 and testdir1/test1.dat.");
      Path dir1 = new Path("testdir1");
      Path file1 = new Path(dir1, "test1.dat");
      FSDataOutputStream stm = writeFile(fileSys, file1, replicas);
      FileStatus stat = fileSys.getFileStatus(file1);
      long atimeBeforeClose = stat.getAccessTime();
      String adate = dateForm.format(new Date(atimeBeforeClose));
      System.out.println("atime on " + file1 + " before close is " + 
                         adate + " (" + atimeBeforeClose + ")");
      assertTrue(atimeBeforeClose != 0);
      stm.close();

      stat = fileSys.getFileStatus(file1);
      long atime1 = stat.getAccessTime();
      long mtime1 = stat.getModificationTime();
      adate = dateForm.format(new Date(atime1));
      String mdate = dateForm.format(new Date(mtime1));
      System.out.println("atime on " + file1 + " is " + adate + 
                         " (" + atime1 + ")");
      System.out.println("mtime on " + file1 + " is " + mdate + 
                         " (" + mtime1 + ")");
      assertTrue(atime1 != 0);

      //
      // record dir times
      //
      stat = fileSys.getFileStatus(dir1);
      long mdir1 = stat.getAccessTime();
      assertTrue(mdir1 == 0);

      // set the access time to be one day in the past
      long atime2 = atime1 - (24L * 3600L * 1000L);
      fileSys.setTimes(file1, -1, atime2);

      // check new access time on file
      stat = fileSys.getFileStatus(file1);
      long atime3 = stat.getAccessTime();
      String adate3 = dateForm.format(new Date(atime3));
      System.out.println("new atime on " + file1 + " is " + 
                         adate3 + " (" + atime3 + ")");
      assertTrue(atime2 == atime3);
      assertTrue(mtime1 == stat.getModificationTime());

      // set the modification time to be 1 hour in the past
      long mtime2 = mtime1 - (3600L * 1000L);
      fileSys.setTimes(file1, mtime2, -1);

      // check new modification time on file
      stat = fileSys.getFileStatus(file1);
      long mtime3 = stat.getModificationTime();
      String mdate3 = dateForm.format(new Date(mtime3));
      System.out.println("new mtime on " + file1 + " is " + 
                         mdate3 + " (" + mtime3 + ")");
      assertTrue(atime2 == stat.getAccessTime());
      assertTrue(mtime2 == mtime3);

      long mtime4 = Time.now() - (3600L * 1000L);
      long atime4 = Time.now();
      fileSys.setTimes(dir1, mtime4, atime4);
      // check new modification time on file
      stat = fileSys.getFileStatus(dir1);
      assertTrue("Not matching the modification times", mtime4 == stat
          .getModificationTime());
      assertTrue("Not matching the access times", atime4 == stat
          .getAccessTime());

      Path nonExistingDir = new Path(dir1, "/nonExistingDir/");
      try {
        fileSys.setTimes(nonExistingDir, mtime4, atime4);
        fail("Expecting FileNotFoundException");
      } catch (FileNotFoundException e) {
        assertTrue(e.getMessage().contains(
            "File/Directory " + nonExistingDir.toString() + " does not exist."));
      }
      // shutdown cluster and restart
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport)
                                                .format(false)
                                                .build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();

      // verify that access times and modification times persist after a
      // cluster restart.
      System.out.println("Verifying times after cluster restart");
      stat = fileSys.getFileStatus(file1);
      assertTrue(atime2 == stat.getAccessTime());
      assertTrue(mtime3 == stat.getModificationTime());
    
      cleanupFile(fileSys, file1);
      cleanupFile(fileSys, dir1);
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Tests mod time change at close in DFS.
   */
  @Test
  public void testTimesAtClose() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    int replicas = 1;

    // parameter initialization
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 50);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                     cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    assertTrue(fileSys instanceof DistributedFileSystem);

    try {
      // create a new file and write to it
      Path file1 = new Path("/simple.dat");
      FSDataOutputStream stm = writeFile(fileSys, file1, replicas);
      System.out.println("Created and wrote file simple.dat");
      FileStatus statBeforeClose = fileSys.getFileStatus(file1);
      long mtimeBeforeClose = statBeforeClose.getModificationTime();
      String mdateBeforeClose = dateForm.format(new Date(
                                                     mtimeBeforeClose));
      System.out.println("mtime on " + file1 + " before close is "
                  + mdateBeforeClose + " (" + mtimeBeforeClose + ")");
      assertTrue(mtimeBeforeClose != 0);

      //close file after writing
      stm.close();
      System.out.println("Closed file.");
      FileStatus statAfterClose = fileSys.getFileStatus(file1);
      long mtimeAfterClose = statAfterClose.getModificationTime();
      String mdateAfterClose = dateForm.format(new Date(mtimeAfterClose));
      System.out.println("mtime on " + file1 + " after close is "
                  + mdateAfterClose + " (" + mtimeAfterClose + ")");
      assertTrue(mtimeAfterClose != 0);
      assertTrue(mtimeBeforeClose != mtimeAfterClose);

      cleanupFile(fileSys, file1);
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test that when access time updates are not needed, the FSNamesystem
   * write lock is not taken by getBlockLocations.
   * Regression test for HDFS-3981.
   */
  @Test(timeout=60000)
  public void testGetBlockLocationsOnlyUsesReadLock() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 100*1000);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    ReentrantReadWriteLock spyLock = NameNodeAdapter.spyOnFsLock(cluster.getNamesystem());
    try {
      // Create empty file in the FSN.
      Path p = new Path("/empty-file");
      DFSTestUtil.createFile(cluster.getFileSystem(), p, 0, (short)1, 0L);
      
      // getBlockLocations() should not need the write lock, since we just created
      // the file (and thus its access time is already within the 100-second
      // accesstime precision configured above). 
      MockitoUtil.doThrowWhenCallStackMatches(
          new AssertionError("Should not need write lock"),
          ".*getBlockLocations.*")
          .when(spyLock).writeLock();
      cluster.getFileSystem().getFileBlockLocations(p, 0, 100);
    } finally {
      cluster.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestSetTimes().testTimes();
  }
}
