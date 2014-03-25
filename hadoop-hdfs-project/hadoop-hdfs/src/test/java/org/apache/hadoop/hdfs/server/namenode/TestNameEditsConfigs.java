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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.test.PathUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * This class tests various combinations of dfs.namenode.name.dir 
 * and dfs.namenode.edits.dir configurations.
 */
public class TestNameEditsConfigs {
  
  private static final Log LOG = LogFactory.getLog(FSEditLog.class);
  
  static final long SEED = 0xDEADBEEFL;
  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 8192;
  static final int NUM_DATA_NODES = 3;
  static final String FILE_IMAGE = "current/fsimage";
  static final String FILE_EDITS = "current/edits";

  short replication = 3;
  private final File base_dir = new File(
      PathUtils.getTestDir(TestNameEditsConfigs.class), "dfs");

  @Before
  public void setUp() throws IOException {
    if(base_dir.exists() && !FileUtil.fullyDelete(base_dir)) {
      throw new IOException("Cannot remove directory " + base_dir);
    }
  }
  
  void checkImageAndEditsFilesExistence(File dir, 
                                        boolean shouldHaveImages,
                                        boolean shouldHaveEdits)
      throws IOException {
    FSImageTransactionalStorageInspector ins = inspect(dir);

    if (shouldHaveImages) {
      assertTrue("Expect images in " + dir, ins.foundImages.size() > 0);
    } else {
      assertTrue("Expect no images in " + dir, ins.foundImages.isEmpty());      
    }

    List<FileJournalManager.EditLogFile> editlogs 
      = FileJournalManager.matchEditLogs(new File(dir, "current").listFiles()); 
    if (shouldHaveEdits) {
      assertTrue("Expect edits in " + dir, editlogs.size() > 0);
    } else {
      assertTrue("Expect no edits in " + dir, editlogs.isEmpty());
    }
  }

  private void checkFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    long size = fileSys.getContentSummary(name).getLength();
    assertEquals("file size for " + name, size, FILE_SIZE);
  }

  private void cleanupFile(FileSystem fileSys, Path name)
      throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  SecondaryNameNode startSecondaryNameNode(Configuration conf
                                          ) throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    return new SecondaryNameNode(conf);
  }

  /**
   * Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
   * The test creates files and restarts cluster with different configs.
   * 1. Starts cluster with shared name and edits dirs
   * 2. Restarts cluster by adding additional (different) name and edits dirs
   * 3. Restarts cluster by removing shared name and edits dirs by allowing to 
   *    start using separate name and edits dirs
   * 4. Restart cluster by adding shared directory again, but make sure we 
   *    do not read any stale image or edits. 
   * All along the test, we create and delete files at reach restart to make
   * sure we are reading proper edits and image.
   * @throws Exception 
   */
  @Test
  public void testNameEditsConfigs() throws Exception {
    Path file1 = new Path("TestNameEditsConfigs1");
    Path file2 = new Path("TestNameEditsConfigs2");
    Path file3 = new Path("TestNameEditsConfigs3");
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    Configuration conf = null;
    FileSystem fileSys = null;
    final File newNameDir = new File(base_dir, "name");
    final File newEditsDir = new File(base_dir, "edits");
    final File nameAndEdits = new File(base_dir, "name_and_edits");
    final File checkpointNameDir = new File(base_dir, "secondname");
    final File checkpointEditsDir = new File(base_dir, "secondedits");
    final File checkpointNameAndEdits = new File(base_dir, "second_name_and_edits");
    
    ImmutableList<File> allCurrentDirs = ImmutableList.of(
        new File(nameAndEdits, "current"),
        new File(newNameDir, "current"),
        new File(newEditsDir, "current"),
        new File(checkpointNameAndEdits, "current"),
        new File(checkpointNameDir, "current"),
        new File(checkpointEditsDir, "current"));
    ImmutableList<File> imageCurrentDirs = ImmutableList.of(
        new File(nameAndEdits, "current"),
        new File(newNameDir, "current"),
        new File(checkpointNameAndEdits, "current"),
        new File(checkpointNameDir, "current"));
    
    
    // Start namenode with same dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointNameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, checkpointNameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Manage our own dfs directories
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .manageNameDfsDirs(false).build();

    cluster.waitActive();
    secondary = startSecondaryNameNode(conf);
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(!fileSys.exists(file1));
      DFSTestUtil.createFile(fileSys, file1, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file1, replication);
      secondary.doCheckpoint();
    } finally {
      fileSys.close();
      cluster.shutdown();
      secondary.shutdown();
    }

    // Start namenode with additional dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf =  new HdfsConfiguration();
    assertTrue(newNameDir.mkdir());
    assertTrue(newEditsDir.mkdir());

    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath() +
              "," + newNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits.getPath() + 
             "," + newEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointNameDir.getPath() +
             "," + checkpointNameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, checkpointEditsDir.getPath() +
             "," + checkpointNameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Manage our own dfs directories. Do not format.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
                                              .format(false)
                                              .manageNameDfsDirs(false)
                                              .build();

    cluster.waitActive();
    secondary = startSecondaryNameNode(conf);
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(fileSys.exists(file1));
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file2, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file2, replication);
      secondary.doCheckpoint();
    } finally {
      fileSys.close();
      cluster.shutdown();
      secondary.shutdown();
    }

    FSImageTestUtil.assertParallelFilesAreIdentical(allCurrentDirs,
        ImmutableSet.of("VERSION"));
    FSImageTestUtil.assertSameNewestImage(imageCurrentDirs);
    
    // Now remove common directory both have and start namenode with 
    // separate name and edits dirs
    conf =  new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, newNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, newEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, checkpointEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .format(false)
                                .manageNameDfsDirs(false)
                                .build();

    cluster.waitActive();
    secondary = startSecondaryNameNode(conf);
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(!fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      checkFile(fileSys, file2, replication);
      cleanupFile(fileSys, file2);
      DFSTestUtil.createFile(fileSys, file3, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file3, replication);
      secondary.doCheckpoint();
    } finally {
      fileSys.close();
      cluster.shutdown();
      secondary.shutdown();
    }
    
    // No edit logs in new name dir
    checkImageAndEditsFilesExistence(newNameDir, true, false);
    checkImageAndEditsFilesExistence(newEditsDir, false, true);
    checkImageAndEditsFilesExistence(checkpointNameDir, true, false);
    checkImageAndEditsFilesExistence(checkpointEditsDir, false, true);

    // Add old name_and_edits dir. File system should not read image or edits
    // from old dir
    assertTrue(FileUtil.fullyDelete(new File(nameAndEdits, "current")));
    assertTrue(FileUtil.fullyDelete(new File(checkpointNameAndEdits, "current")));
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath() +
              "," + newNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits +
              "," + newEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, checkpointNameDir.getPath() +
        "," + checkpointNameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, checkpointEditsDir.getPath() +
        "," + checkpointNameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .format(false)
                                .manageNameDfsDirs(false)
                                .build();
    cluster.waitActive();
    secondary = startSecondaryNameNode(conf);
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(!fileSys.exists(file1));
      assertTrue(!fileSys.exists(file2));
      assertTrue(fileSys.exists(file3));
      checkFile(fileSys, file3, replication);
      secondary.doCheckpoint();
    } finally {
      fileSys.close();
      cluster.shutdown();
      secondary.shutdown();
    }
    checkImageAndEditsFilesExistence(nameAndEdits, true, true);
    checkImageAndEditsFilesExistence(checkpointNameAndEdits, true, true);
  }

  private FSImageTransactionalStorageInspector inspect(File storageDir)
      throws IOException {
    return FSImageTestUtil.inspectStorageDirectory(
        new File(storageDir, "current"), NameNodeDirType.IMAGE_AND_EDITS);
  }

  /**
   * Test edits.dir.required configuration options.
   * 1. Directory present in dfs.namenode.edits.dir.required but not in
   *    dfs.namenode.edits.dir. Expected to fail.
   * 2. Directory present in both dfs.namenode.edits.dir.required and
   *    dfs.namenode.edits.dir. Expected to succeed.
   * 3. Directory present only in dfs.namenode.edits.dir. Expected to
   *    succeed.
   */
  @Test
  public void testNameEditsRequiredConfigs() throws IOException {
    MiniDFSCluster cluster = null;
    File nameAndEditsDir = new File(base_dir, "name_and_edits");
    File nameAndEditsDir2 = new File(base_dir, "name_and_edits2");
    File nameDir = new File(base_dir, "name");

    // 1
    // Bad configuration. Add a directory to dfs.namenode.edits.dir.required
    // without adding it to dfs.namenode.edits.dir.
    try {
      Configuration conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nameDir.getAbsolutePath());
      conf.set(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY,
          nameAndEditsDir2.toURI().toString());
      conf.set(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
          nameAndEditsDir.toURI().toString());
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(NUM_DATA_NODES)
          .manageNameDfsDirs(false)
          .build();
      fail("Successfully started cluster but should not have been able to.");
    } catch (IllegalArgumentException iae) { // expect to fail
      LOG.info("EXPECTED: cluster start failed due to bad configuration" + iae);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }

    // 2
    // Good configuration. Add a directory to both dfs.namenode.edits.dir.required
    // and dfs.namenode.edits.dir.
    try {
      Configuration conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nameDir.getAbsolutePath());
      conf.setStrings(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
          nameAndEditsDir.toURI().toString(),
          nameAndEditsDir2.toURI().toString());
      conf.set(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY,
          nameAndEditsDir2.toURI().toString());
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(NUM_DATA_NODES)
          .manageNameDfsDirs(false)
          .build();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    // 3
    // Good configuration. Adds a directory to dfs.namenode.edits.dir but not to
    // dfs.namenode.edits.dir.required.
    try {
      Configuration conf = new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nameDir.getAbsolutePath());
      conf.setStrings(
          DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
          nameAndEditsDir.toURI().toString(),
          nameAndEditsDir2.toURI().toString());
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(NUM_DATA_NODES)
          .manageNameDfsDirs(false)
          .build();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
   * This test tries to simulate failure scenarios.
   * 1. Start cluster with shared name and edits dir
   * 2. Restart cluster by adding separate name and edits dirs
   * 3. Restart cluster by removing shared name and edits dir
   * 4. Restart cluster with old shared name and edits dir, but only latest 
   *    name dir. This should fail since we don't have latest edits dir
   * 5. Restart cluster with old shared name and edits dir, but only latest
   *    edits dir. This should succeed since the latest edits will have
   *    segments leading all the way from the image in name_and_edits.
   */
  @Test
  public void testNameEditsConfigsFailure() throws IOException {
    Path file1 = new Path("TestNameEditsConfigs1");
    Path file2 = new Path("TestNameEditsConfigs2");
    Path file3 = new Path("TestNameEditsConfigs3");
    MiniDFSCluster cluster = null;
    Configuration conf = null;
    FileSystem fileSys = null;
    File nameOnlyDir = new File(base_dir, "name");
    File editsOnlyDir = new File(base_dir, "edits");
    File nameAndEditsDir = new File(base_dir, "name_and_edits");
    
    // 1
    // Start namenode with same dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    
    try {
      // Manage our own dfs directories
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .manageNameDfsDirs(false)
                                  .build();
      cluster.waitActive();
      
      // Check that the dir has a VERSION file
      assertTrue(new File(nameAndEditsDir, "current/VERSION").exists());
      
      fileSys = cluster.getFileSystem();

      assertTrue(!fileSys.exists(file1));
      DFSTestUtil.createFile(fileSys, file1, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file1, replication);
    } finally  {
      fileSys.close();
      cluster.shutdown();
    }

    // 2
    // Start namenode with additional dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf =  new HdfsConfiguration();
    assertTrue(nameOnlyDir.mkdir());
    assertTrue(editsOnlyDir.mkdir());

    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEditsDir.getPath() +
              "," + nameOnlyDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEditsDir.getPath() +
              "," + editsOnlyDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    
    try {
      // Manage our own dfs directories. Do not format.
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      cluster.waitActive();
  
      // Check that the dirs have a VERSION file
      assertTrue(new File(nameAndEditsDir, "current/VERSION").exists());
      assertTrue(new File(nameOnlyDir, "current/VERSION").exists());
      assertTrue(new File(editsOnlyDir, "current/VERSION").exists());
  
      fileSys = cluster.getFileSystem();

      assertTrue(fileSys.exists(file1));
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      DFSTestUtil.createFile(fileSys, file2, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file2, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    // 3
    // Now remove common directory both have and start namenode with 
    // separate name and edits dirs
    try {
      conf =  new HdfsConfiguration();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameOnlyDir.getPath());
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, editsOnlyDir.getPath());
      replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      cluster.waitActive();
      fileSys = cluster.getFileSystem();

      assertFalse(fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      checkFile(fileSys, file2, replication);
      cleanupFile(fileSys, file2);
      DFSTestUtil.createFile(fileSys, file3, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file3, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    // 4
    // Add old shared directory for name and edits along with latest name
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameOnlyDir.getPath() + "," + 
             nameAndEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      fail("Successfully started cluster but should not have been able to.");
    } catch (IOException e) { // expect to fail
      LOG.info("EXPECTED: cluster start failed due to missing " +
                         "latest edits dir", e);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      cluster = null;
    }

    // 5
    // Add old shared directory for name and edits along with latest edits. 
    // This is OK, since the latest edits will have segments leading all
    // the way from the image in name_and_edits.
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEditsDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, editsOnlyDir.getPath() +
             "," + nameAndEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      
      fileSys = cluster.getFileSystem();
      
      assertFalse(fileSys.exists(file1));
      assertFalse(fileSys.exists(file2));
      assertTrue(fileSys.exists(file3));
      checkFile(fileSys, file3, replication);
      cleanupFile(fileSys, file3);
      DFSTestUtil.createFile(fileSys, file3, FILE_SIZE, FILE_SIZE, BLOCK_SIZE,
          replication, SEED);
      checkFile(fileSys, file3, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Test dfs.namenode.checkpoint.dir and dfs.namenode.checkpoint.edits.dir
   * should tolerate white space between values.
   */
  @Test
  public void testCheckPointDirsAreTrimmed() throws Exception {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    File checkpointNameDir1 = new File(base_dir, "chkptName1");
    File checkpointEditsDir1 = new File(base_dir, "chkptEdits1");
    File checkpointNameDir2 = new File(base_dir, "chkptName2");
    File checkpointEditsDir2 = new File(base_dir, "chkptEdits2");
    File nameDir = new File(base_dir, "name1");
    String whiteSpace = "  \n   \n  ";
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getPath());
    conf.setStrings(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, whiteSpace
        + checkpointNameDir1.getPath() + whiteSpace, whiteSpace
        + checkpointNameDir2.getPath() + whiteSpace);
    conf.setStrings(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
        whiteSpace + checkpointEditsDir1.getPath() + whiteSpace, whiteSpace
            + checkpointEditsDir2.getPath() + whiteSpace);
    cluster = new MiniDFSCluster.Builder(conf).manageNameDfsDirs(false)
        .numDataNodes(3).build();
    try {
      cluster.waitActive();
      secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      assertTrue(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + " must be trimmed ",
          checkpointNameDir1.exists());
      assertTrue(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY + " must be trimmed ",
          checkpointNameDir2.exists());
      assertTrue(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY
          + " must be trimmed ", checkpointEditsDir1.exists());
      assertTrue(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY
          + " must be trimmed ", checkpointEditsDir2.exists());
    } finally {
      secondary.shutdown();
      cluster.shutdown();
    }
  }
}
