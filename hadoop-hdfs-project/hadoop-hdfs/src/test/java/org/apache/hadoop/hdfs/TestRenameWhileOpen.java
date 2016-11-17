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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRenameWhileOpen {
  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
  }

  //TODO: un-comment checkFullFile once the lease recovery is done
  private static void checkFullFile(FileSystem fs, Path p) throws IOException {
    //TestFileCreation.checkFullFile(fs, p);
  }

  /**
   * open /user/dir1/file1 /user/dir2/file2
   * mkdir /user/dir3
   * move /user/dir1 /user/dir3
   */
  @Test
  public void testWhileOpenRenameParent() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, TestFileCreation.blockSize);

    // create cluster
    System.out.println("Test 1*****************************");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // Normally, the in-progress edit log would be finalized by
      // FSEditLog#endCurrentLogSegment.  For testing purposes, we
      // disable that here.
      FSEditLog spyLog =
          spy(cluster.getNameNode().getFSImage().getEditLog());
      doNothing().when(spyLog).endCurrentLogSegment(Mockito.anyBoolean());
      DFSTestUtil.setEditLogForTesting(cluster.getNamesystem(), spyLog);

      // create file1.
      Path dir1 = new Path("/user/a+b/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      // create file2.
      Path dir2 = new Path("/user/dir2");
      Path file2 = new Path(dir2, "file2");
      FSDataOutputStream stm2 = TestFileCreation.createFile(fs, file2, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);
      TestFileCreation.writeFile(stm2);
      stm2.hflush();

      // move dir1 while file1 is open
      Path dir3 = new Path("/user/dir3");
      fs.mkdirs(dir3);
      fs.rename(dir1, dir3);

      // create file3
      Path file3 = new Path(dir3, "file3");
      FSDataOutputStream stm3 = fs.create(file3);
      fs.rename(file3, new Path(dir3, "bozo"));
      // Get a new block for the file.
      TestFileCreation.writeFile(stm3, TestFileCreation.blockSize + 1);
      stm3.hflush();

      // Stop the NameNode before closing the files.
      // This will ensure that the write leases are still active and present
      // in the edit log.  Simiarly, there should be a pending ADD_BLOCK_OP
      // for file3, since we just added a block to that file.
      cluster.getNameNode().stop();

      // Restart cluster.
      cluster.shutdown();

      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from the edit log.
      cluster.shutdown();
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      Path newfile = new Path("/user/dir3/dir1", "file1");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(file2));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 /user/dir2/file2
   * move /user/dir1 /user/dir3
   */
  @Test
  public void testWhileOpenRenameParentToNonexistentDir() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    System.out.println("Test 2************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      // create file2.
      Path dir2 = new Path("/user/dir2");
      Path file2 = new Path(dir2, "file2");
      FSDataOutputStream stm2 = TestFileCreation.createFile(fs, file2, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);
      TestFileCreation.writeFile(stm2);
      stm2.hflush();

      // move dir1 while file1 is open
      Path dir3 = new Path("/user/dir3");
      fs.rename(dir1, dir3);

      // restart cluster.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      Path newfile = new Path("/user/dir3", "file1");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(file2));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 
   * mkdir /user/dir2
   * move /user/dir1/file1 /user/dir2/
   */
  @Test
  public void testWhileOpenRenameToExistentDirectory() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    System.out.println("Test 3************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: " +
                         "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      Path dir2 = new Path("/user/dir2");
      fs.mkdirs(dir2);

      fs.rename(file1, dir2);

      // restart cluster.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      Path newfile = new Path("/user/dir2", "file1");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * open /user/dir1/file1 
   * move /user/dir1/file1 /user/dir2/
   */
  @Test
  public void testWhileOpenRenameToNonExistentDirectory() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1);
    System.out.println("Test 4************************************");

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // create file1.
      Path dir1 = new Path("/user/dir1");
      Path file1 = new Path(dir1, "file1");
      FSDataOutputStream stm1 = TestFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      TestFileCreation.writeFile(stm1);
      stm1.hflush();

      Path dir2 = new Path("/user/dir2");

      fs.rename(file1, dir2);

      // restart cluster.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniDFSCluster.Builder(conf).format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      Path newfile = new Path("/user", "dir2");
      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(newfile));
      checkFullFile(fs, newfile);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
