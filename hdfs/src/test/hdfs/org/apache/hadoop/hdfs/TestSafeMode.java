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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

/**
 * Tests to verify safe mode correctness.
 */
public class TestSafeMode {
  Configuration conf; 
  MiniDFSCluster cluster;
  FileSystem fs;
  DistributedFileSystem dfs;

  @Before
  public void startUp() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();      
    fs = cluster.getFileSystem();
    dfs = (DistributedFileSystem)fs;
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * This test verifies that if SafeMode is manually entered, name-node does not
   * come out of safe mode even after the startup safe mode conditions are met.
   * <ol>
   * <li>Start cluster with 1 data-node.</li>
   * <li>Create 2 files with replication 1.</li>
   * <li>Re-start cluster with 0 data-nodes. 
   * Name-node should stay in automatic safe-mode.</li>
   * <li>Enter safe mode manually.</li>
   * <li>Start the data-node.</li>
   * <li>Wait longer than <tt>dfs.namenode.safemode.extension</tt> and 
   * verify that the name-node is still in safe mode.</li>
   * </ol>
   *  
   * @throws IOException
   */
  @Test
  public void testManualSafeMode() throws IOException {      
    fs = (DistributedFileSystem)cluster.getFileSystem();
    Path file1 = new Path("/tmp/testManualSafeMode/file1");
    Path file2 = new Path("/tmp/testManualSafeMode/file2");
    
    // create two files with one block each.
    DFSTestUtil.createFile(fs, file1, 1000, (short)1, 0);
    DFSTestUtil.createFile(fs, file2, 2000, (short)1, 0);
    fs.close();
    cluster.shutdown();
    
    // now bring up just the NameNode.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
    cluster.waitActive();
    dfs = (DistributedFileSystem)cluster.getFileSystem();
    
    assertTrue("No datanode is started. Should be in SafeMode", 
               dfs.setSafeMode(SafeModeAction.SAFEMODE_GET));
    
    // manually set safemode.
    dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    
    // now bring up the datanode and wait for it to be active.
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();
    
    // wait longer than dfs.namenode.safemode.extension
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignored) {}

    assertTrue("should still be in SafeMode",
        dfs.setSafeMode(SafeModeAction.SAFEMODE_GET));
    assertFalse("should not be in SafeMode", 
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE));
  }

  public interface FSRun {
    public abstract void run(FileSystem fs) throws IOException;
  }

  /**
   * Assert that the given function fails to run due to a safe 
   * mode exception.
   */
  public void runFsFun(String msg, FSRun f) {
    try {
      f.run(fs);
      fail(msg);
     } catch (IOException ioe) {
       assertTrue(ioe.getMessage().contains("safe mode"));
     }
  }

  /**
   * Run various fs operations while the NN is in safe mode,
   * assert that they are either allowed or fail as expected.
   */
  @Test
  public void testOperationsWhileInSafeMode() throws IOException {
    final Path file1 = new Path("/file1");

    assertFalse(dfs.setSafeMode(SafeModeAction.SAFEMODE_GET));
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    assertTrue("Could not enter SM", 
        dfs.setSafeMode(SafeModeAction.SAFEMODE_ENTER));

    runFsFun("Set quota while in SM", new FSRun() { 
      public void run(FileSystem fs) throws IOException {
        ((DistributedFileSystem)fs).setQuota(file1, 1, 1); 
      }});

    runFsFun("Set perm while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        fs.setPermission(file1, FsPermission.getDefault());
      }});

    runFsFun("Set owner while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        fs.setOwner(file1, "user", "group");
      }});

    runFsFun("Set repl while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        fs.setReplication(file1, (short)1);
      }});

    runFsFun("Append file while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        DFSTestUtil.appendFile(fs, file1, "new bytes");
      }});

    runFsFun("Delete file while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        fs.delete(file1, false);
      }});

    runFsFun("Rename file while in SM", new FSRun() {
      public void run(FileSystem fs) throws IOException {
        fs.rename(file1, new Path("file2"));
      }});

    try {
      fs.setTimes(file1, 0, 0);
    } catch (IOException ioe) {
      fail("Set times failed while in SM");
    }

    try {
      DFSTestUtil.readFile(fs, file1);
    } catch (IOException ioe) {
      fail("Set times failed while in SM");
    }

    assertFalse("Could not leave SM",
        dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE));
  }
  
}