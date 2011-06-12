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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImage.CheckpointStates;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import junit.framework.TestCase;

public class TestBackupNode extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestBackupNode.class);

  static final String BASE_DIR = MiniDFSCluster.getBaseDirectory();

  protected void setUp() throws Exception {
    super.setUp();
    File baseDir = new File(BASE_DIR);
    if(baseDir.exists())
      if(!(FileUtil.fullyDelete(baseDir)))
        throw new IOException("Cannot remove directory: " + baseDir);
    File dirC = new File(getBackupNodeDir(StartupOption.CHECKPOINT, 1));
    dirC.mkdirs();
    File dirB = new File(getBackupNodeDir(StartupOption.BACKUP, 1));
    dirB.mkdirs();
    dirB = new File(getBackupNodeDir(StartupOption.BACKUP, 2));
    dirB.mkdirs();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    File baseDir = new File(BASE_DIR);
    if(!(FileUtil.fullyDelete(baseDir)))
      throw new IOException("Cannot remove directory: " + baseDir);
  }

  static void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    TestCheckpoint.writeFile(fileSys, name, repl);
  }


  static void checkFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    TestCheckpoint.checkFile(fileSys, name, repl);
  }

  void cleanupFile(FileSystem fileSys, Path name)
  throws IOException {
    TestCheckpoint.cleanupFile(fileSys, name);
  }

  static String getBackupNodeDir(StartupOption t, int i) {
    return BASE_DIR + "name" + t.getName() + i + "/";
  }

  BackupNode startBackupNode(Configuration conf,
                             StartupOption t, int i) throws IOException {
    Configuration c = new HdfsConfiguration(conf);
    String dirs = getBackupNodeDir(t, i);
    c.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dirs);
    c.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, "${dfs.name.dir}");
    return (BackupNode)NameNode.createNameNode(new String[]{t.getName()}, c);
  }

  void waitCheckpointDone(BackupNode backup) {
    do {
      try {
        LOG.info("Waiting checkpoint to complete...");
        Thread.sleep(1000);
      } catch (Exception e) {}
    } while(backup.getCheckpointState() != CheckpointStates.START);
  }

  public void testCheckpoint() throws IOException {
    testCheckpoint(StartupOption.CHECKPOINT);
    testCheckpoint(StartupOption.BACKUP);
  }

  void testCheckpoint(StartupOption op) throws IOException {
    Path file1 = new Path("checkpoint.dat");
    Path file2 = new Path("checkpoint2.dat");

    Configuration conf = new HdfsConfiguration();
    short replication = (short)conf.getInt("dfs.replication", 3);
    conf.set("dfs.blockreport.initialDelay", "0");
    conf.setInt("dfs.datanode.scan.period.hours", -1); // disable block scanner
    int numDatanodes = Math.max(3, replication);
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    BackupNode backup = null;

    try {
      cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
      fileSys = cluster.getFileSystem();
      //
      // verify that 'format' really blew away all pre-existing files
      //
      assertTrue(!fileSys.exists(file1));
      assertTrue(!fileSys.exists(file2));

      //
      // Create file1
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);

      //
      // Take a checkpoint
      //
      backup = startBackupNode(conf, op, 1);
      waitCheckpointDone(backup);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup != null) backup.stop();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
    File imageFileNN = new File(BASE_DIR, "name1/current/fsimage");
    File imageFileBN = new File(getBackupNodeDir(op, 1), "/current/fsimage");
    LOG.info("NameNode fsimage length = " + imageFileNN.length());
    LOG.info("Backup Node fsimage length = " + imageFileBN.length());
    assertTrue(imageFileNN.length() == imageFileBN.length());

    try {
      //
      // Restart cluster and verify that file1 still exist.
      //
      cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
      fileSys = cluster.getFileSystem();
      // check that file1 still exists
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);

      // create new file file2
      writeFile(fileSys, file2, replication);
      checkFile(fileSys, file2, replication);

      //
      // Take a checkpoint
      //
      backup = startBackupNode(conf, op, 1);
      waitCheckpointDone(backup);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup != null) backup.stop();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
    LOG.info("NameNode fsimage length = " + imageFileNN.length());
    LOG.info("Backup Node fsimage length = " + imageFileBN.length());
    assertTrue(imageFileNN.length() == imageFileBN.length());

    try {
      //
      // Restart cluster and verify that file2 exists and
      // file1 does not exist.
      //
      cluster = new MiniDFSCluster(conf, numDatanodes, false, null);
      fileSys = cluster.getFileSystem();

      assertTrue(!fileSys.exists(file1));

      // verify that file2 exists
      checkFile(fileSys, file2, replication);
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that only one backup node can register.
   * @throws IOException
   */
  public void testBackupRegistration() throws IOException {
    Configuration conf1 = new HdfsConfiguration();
    Configuration conf2 = null;
    MiniDFSCluster cluster = null;
    BackupNode backup1 = null;
    BackupNode backup2 = null;
    try {
      // start name-node and backup node 1
      cluster = new MiniDFSCluster(conf1, 0, true, null);
      conf1.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY, "0.0.0.0:7771");
      conf1.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, "0.0.0.0:7775");
      backup1 = startBackupNode(conf1, StartupOption.BACKUP, 1);
      // try to start backup node 2
      conf2 = new HdfsConfiguration(conf1);
      conf2.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY, "0.0.0.0:7772");
      conf2.set(DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, "0.0.0.0:7776");
      try {
        backup2 = startBackupNode(conf2, StartupOption.BACKUP, 2);
        backup2.stop();
        backup2 = null;
        assertTrue("Only one backup node should be able to start", false);
      } catch(IOException e) {
        assertTrue(
            e.getLocalizedMessage().contains("Registration is not allowed"));
        // should fail - doing good
      }
      // stop backup node 1; backup node 2 should be able to start
      backup1.stop();
      backup1 = null;
      try {
        backup2 = startBackupNode(conf2, StartupOption.BACKUP, 2);
      } catch(IOException e) {
        assertTrue("Backup node 2 should be able to start", false);
      }
    } catch(IOException e) {
      LOG.error("Error in TestBackupNode:", e);
      assertTrue(e.getLocalizedMessage(), false);
    } finally {
      if(backup1 != null) backup1.stop();
      if(backup2 != null) backup2.stop();
      if(cluster != null) cluster.shutdown();
    }
  }
}
