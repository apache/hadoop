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

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * This class tests various combinations of dfs.namenode.name.dir 
 * and dfs.namenode.edits.dir configurations.
 */
public class TestNameEditsConfigs extends TestCase {
  static final long SEED = 0xDEADBEEFL;
  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 8192;
  static final int NUM_DATA_NODES = 3;
  static final String FILE_IMAGE = "current/fsimage";
  static final String FILE_EDITS = "current/edits";

  short replication = 3;
  private File base_dir = new File(
      System.getProperty("test.build.data", "build/test/data"), "dfs/");

  protected void setUp() throws java.lang.Exception {
    if(base_dir.exists()) {
      if (!FileUtil.fullyDelete(base_dir)) 
        throw new IOException("Cannot remove directory " + base_dir);
    }
  }

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)BLOCK_SIZE);
    byte[] buffer = new byte[FILE_SIZE];
    Random rand = new Random(SEED);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
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

    if (shouldHaveEdits) {
      assertTrue("Expect edits in " + dir, ins.foundEditLogs.size() > 0);
    } else {
      assertTrue("Expect no edits in " + dir, ins.foundEditLogs.isEmpty());
    }
  }

  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    long size = fileSys.getContentSummary(name).getLength();
    assertEquals("file size for " + name, size, (long)FILE_SIZE);
  }

  private void cleanupFile(FileSystem fileSys, Path name)
    throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  // This deprecation suppress warning does not work due to known Java bug:
  // http://bugs.sun.com/view_bug.do?bug_id=6460147
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
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
      writeFile(fileSys, file1, replication);
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
      writeFile(fileSys, file2, replication);
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
      writeFile(fileSys, file3, replication);
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
   * Test various configuration options of dfs.namenode.name.dir and dfs.namenode.edits.dir
   * This test tries to simulate failure scenarios.
   * 1. Start cluster with shared name and edits dir
   * 2. Restart cluster by adding separate name and edits dirs
   * T3. Restart cluster by removing shared name and edits dir
   * 4. Restart cluster with old shared name and edits dir, but only latest 
   *    name dir. This should fail since we dont have latest edits dir
   * 5. Restart cluster with old shared name and edits dir, but only latest
   *    edits dir. This should fail since we dont have latest name dir
   */
  public void testNameEditsConfigsFailure() throws IOException {
    Path file1 = new Path("TestNameEditsConfigs1");
    Path file2 = new Path("TestNameEditsConfigs2");
    Path file3 = new Path("TestNameEditsConfigs3");
    MiniDFSCluster cluster = null;
    Configuration conf = null;
    FileSystem fileSys = null;
    File newNameDir = new File(base_dir, "name");
    File newEditsDir = new File(base_dir, "edits");
    File nameAndEdits = new File(base_dir, "name_and_edits");
    
    // Start namenode with same dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Manage our own dfs directories
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .manageNameDfsDirs(false)
                                .build();
    cluster.waitActive();
    
    // Check that the dir has a VERSION file
    assertTrue(new File(nameAndEdits, "current/VERSION").exists());
    
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(!fileSys.exists(file1));
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally  {
      fileSys.close();
      cluster.shutdown();
    }

    // Start namenode with additional dfs.namenode.name.dir and dfs.namenode.edits.dir
    conf =  new HdfsConfiguration();
    assertTrue(newNameDir.mkdir());
    assertTrue(newEditsDir.mkdir());

    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath() +
              "," + newNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits.getPath() +
              "," + newEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Manage our own dfs directories. Do not format.
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .format(false)
                                .manageNameDfsDirs(false)
                                .build();
    cluster.waitActive();

    // Check that the dirs have a VERSION file
    assertTrue(new File(nameAndEdits, "current/VERSION").exists());
    assertTrue(new File(newNameDir, "current/VERSION").exists());
    assertTrue(new File(newEditsDir, "current/VERSION").exists());

    fileSys = cluster.getFileSystem();

    try {
      assertTrue(fileSys.exists(file1));
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file2, replication);
      checkFile(fileSys, file2, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    // Now remove common directory both have and start namenode with 
    // separate name and edits dirs
    conf =  new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, newNameDir.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, newEditsDir.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    cluster = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(NUM_DATA_NODES)
                                .format(false)
                                .manageNameDfsDirs(false)
                                .build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();

    try {
      assertTrue(!fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      checkFile(fileSys, file2, replication);
      cleanupFile(fileSys, file2);
      writeFile(fileSys, file3, replication);
      checkFile(fileSys, file3, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
    
    // Add old shared directory for name and edits along with latest name
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, newNameDir.getPath() + "," + 
             nameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      assertTrue(false);
    } catch (IOException e) { // expect to fail
      System.out.println("cluster start failed due to missing " +
                         "latest edits dir");
    } finally {
      cluster = null;
    }

    // Add old shared directory for name and edits along with latest edits. 
    // This is OK, since the latest edits will have segments leading all
    // the way from the image in name_and_edits.
    conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameAndEdits.getPath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, newEditsDir.getPath() +
             "," + nameAndEdits.getPath());
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    try {
      cluster = new MiniDFSCluster.Builder(conf)
                                  .numDataNodes(NUM_DATA_NODES)
                                  .format(false)
                                  .manageNameDfsDirs(false)
                                  .build();
      assertTrue(!fileSys.exists(file1));
      assertTrue(fileSys.exists(file2));
      checkFile(fileSys, file2, replication);
      cleanupFile(fileSys, file2);
      writeFile(fileSys, file3, replication);
      checkFile(fileSys, file3, replication);
    } catch (IOException e) { // expect to fail
      System.out.println("cluster start failed due to missing latest name dir");
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
