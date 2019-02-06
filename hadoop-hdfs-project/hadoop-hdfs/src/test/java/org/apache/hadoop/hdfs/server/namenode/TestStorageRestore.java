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


import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.cli.CLITestCmdDFS;
import org.apache.hadoop.cli.util.CLICommandDFSAdmin;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.util.Shell;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
/**
 * Startup and checkpoint tests
 * 
 */
public class TestStorageRestore {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private static final Logger LOG =
      LoggerFactory.getLogger(TestStorageRestore.class.getName());
  private Configuration config;
  private File hdfsDir=null;
  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  private File path1, path2, path3;
  private MiniDFSCluster cluster;  
  @Before
  public void setUpNameDirs() throws Exception {
    config = new HdfsConfiguration();
    hdfsDir = new File(MiniDFSCluster.getBaseDirectory()).getCanonicalFile();
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    
    hdfsDir.mkdirs();
    path1 = new File(hdfsDir, "name1");
    path2 = new File(hdfsDir, "name2");
    path3 = new File(hdfsDir, "name3");
    
    path1.mkdir(); path2.mkdir(); path3.mkdir();
    if(!path2.exists() ||  !path3.exists() || !path1.exists()) {
      throw new IOException("Couldn't create dfs.name dirs in " + hdfsDir.getAbsolutePath());
    }
    
    String dfs_name_dir = new String(path1.getPath() + "," + path2.getPath());
    System.out.println("configuring hdfsdir is " + hdfsDir.getAbsolutePath() + 
        "; dfs_name_dir = "+ dfs_name_dir + ";dfs_name_edits_dir(only)=" + path3.getPath());
    
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, dfs_name_dir);
    config.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, dfs_name_dir + "," + path3.getPath());

    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,new File(hdfsDir, "secondary").getPath());
 
    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
    
    config.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    
    // set the restore feature on
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, true);
  }
  
  /**
   * invalidate storage by removing the second and third storage directories
   */
  public void invalidateStorage(FSImage fi, Set<File> filesToInvalidate) throws IOException {
    ArrayList<StorageDirectory> al = new ArrayList<StorageDirectory>(2);
    Iterator<StorageDirectory> it = fi.getStorage().dirIterator();
    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      if(filesToInvalidate.contains(sd.getRoot())) {
        LOG.info("causing IO error on " + sd.getRoot());
        al.add(sd);
      }
    }
    // simulate an error
    fi.getStorage().reportErrorsOnDirectories(al);
    
    for (JournalAndStream j : fi.getEditLog().getJournals()) {
      if (j.getManager() instanceof FileJournalManager) {
        FileJournalManager fm = (FileJournalManager)j.getManager();
        if (fm.getStorageDirectory().getRoot().equals(path2)
            || fm.getStorageDirectory().getRoot().equals(path3)) {
          EditLogOutputStream mockStream = spy(j.getCurrentStream());
          j.setCurrentStreamForTests(mockStream);
          doThrow(new IOException("Injected fault: write")).
            when(mockStream).write(any());
        }
      }
    }
  }
  
  /**
   * test
   */
  private void printStorages(FSImage image) {
    FSImageTestUtil.logStorageContents(LOG, image.getStorage());
  }    

  /**
   * test 
   * 1. create DFS cluster with 3 storage directories - 2 EDITS_IMAGE, 1 EDITS
   * 2. create a cluster and write a file
   * 3. corrupt/disable one storage (or two) by removing
   * 4. run doCheckpoint - it will fail on removed dirs (which
   * will invalidate the storages)
   * 5. write another file
   * 6. check that edits and fsimage differ 
   * 7. run doCheckpoint
   * 8. verify that all the image and edits files are the same.
   */
  @Test
  public void testStorageRestore() throws Exception {
    int numDatanodes = 0;
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(numDatanodes)
                                                .manageNameDfsDirs(false)
                                                .build();
    cluster.waitActive();
    
    SecondaryNameNode secondary = new SecondaryNameNode(config);
    System.out.println("****testStorageRestore: Cluster and SNN started");
    printStorages(cluster.getNameNode().getFSImage());
    
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("/", "test");
    assertTrue(fs.mkdirs(path));
    
    System.out.println("****testStorageRestore: dir 'test' created, invalidating storage...");
  
    invalidateStorage(cluster.getNameNode().getFSImage(), ImmutableSet.of(path2, path3));
    printStorages(cluster.getNameNode().getFSImage());
    System.out.println("****testStorageRestore: storage invalidated");

    path = new Path("/", "test1");
    assertTrue(fs.mkdirs(path));

    System.out.println("****testStorageRestore: dir 'test1' created");

    // We did another edit, so the still-active directory at 'path1'
    // should now differ from the others
    FSImageTestUtil.assertFileContentsDifferent(2,
        new File(path1, "current/" + getInProgressEditsFileName(1)),
        new File(path2, "current/" + getInProgressEditsFileName(1)),
        new File(path3, "current/" + getInProgressEditsFileName(1)));
    FSImageTestUtil.assertFileContentsSame(
        new File(path2, "current/" + getInProgressEditsFileName(1)),
        new File(path3, "current/" + getInProgressEditsFileName(1)));
        
    System.out.println("****testStorageRestore: checkfiles(false) run");
    
    secondary.doCheckpoint();  ///should enable storage..
    
    // We should have a checkpoint through txid 4 in the two image dirs
    // (txid=4 for BEGIN, mkdir, mkdir, END)
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getImageFileName(4)),
        new File(path2, "current/" + getImageFileName(4)));
    assertFalse("Should not have any image in an edits-only directory",
        new File(path3, "current/" + getImageFileName(4)).exists());

    // Should have finalized logs in the directory that didn't fail
    assertTrue("Should have finalized logs in the directory that didn't fail",
        new File(path1, "current/" + getFinalizedEditsFileName(1,4)).exists());
    // Should not have finalized logs in the failed directories
    assertFalse("Should not have finalized logs in the failed directories",
        new File(path2, "current/" + getFinalizedEditsFileName(1,4)).exists());
    assertFalse("Should not have finalized logs in the failed directories",
        new File(path3, "current/" + getFinalizedEditsFileName(1,4)).exists());
    
    // The new log segment should be in all of the directories.
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getInProgressEditsFileName(5)),
        new File(path2, "current/" + getInProgressEditsFileName(5)),
        new File(path3, "current/" + getInProgressEditsFileName(5)));
    String md5BeforeEdit = FSImageTestUtil.getFileMD5(
        new File(path1, "current/" + getInProgressEditsFileName(5)));
    
    // The original image should still be the previously failed image
    // directory after it got restored, since it's still useful for
    // a recovery!
    FSImageTestUtil.assertFileContentsSame(
            new File(path1, "current/" + getImageFileName(0)),
            new File(path2, "current/" + getImageFileName(0)));
    
    // Do another edit to verify that all the logs are active.
    path = new Path("/", "test2");
    assertTrue(fs.mkdirs(path));

    // Logs should be changed by the edit.
    String md5AfterEdit =  FSImageTestUtil.getFileMD5(
        new File(path1, "current/" + getInProgressEditsFileName(5)));
    assertFalse(md5BeforeEdit.equals(md5AfterEdit));

    // And all logs should be changed.
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getInProgressEditsFileName(5)),
        new File(path2, "current/" + getInProgressEditsFileName(5)),
        new File(path3, "current/" + getInProgressEditsFileName(5)));

    secondary.shutdown();
    cluster.shutdown();
    
    // All logs should be finalized by clean shutdown
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getFinalizedEditsFileName(5,7)),
        new File(path2, "current/" + getFinalizedEditsFileName(5,7)),        
        new File(path3, "current/" + getFinalizedEditsFileName(5,7)));
  }
  
  /**
   * Test dfsadmin -restoreFailedStorage command
   * @throws Exception
   */
  @Test
  public void testDfsAdminCmd() throws Exception {
    cluster = new MiniDFSCluster.Builder(config).
                                 numDataNodes(2).
                                 manageNameDfsDirs(false).build();
    cluster.waitActive();
    try {

      FSImage fsi = cluster.getNameNode().getFSImage();

      // it is started with dfs.namenode.name.dir.restore set to true (in SetUp())
      boolean restore = fsi.getStorage().getRestoreFailedStorage();
      LOG.info("Restore is " + restore);
      assertEquals(restore, true);

      // now run DFSAdmnin command

      String cmd = "-fs NAMENODE -restoreFailedStorage false";
      String namenode = config.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");
      CommandExecutor executor =
          new CLITestCmdDFS(cmd,
              new CLICommandDFSAdmin()).getExecutor(namenode, config);

      executor.executeCommand(cmd);
      restore = fsi.getStorage().getRestoreFailedStorage();
      assertFalse("After set true call restore is " + restore, restore);

      // run one more time - to set it to true again
      cmd = "-fs NAMENODE -restoreFailedStorage true";
      executor.executeCommand(cmd);
      restore = fsi.getStorage().getRestoreFailedStorage();
      assertTrue("After set false call restore is " + restore, restore);
      
      // run one more time - no change in value
      cmd = "-fs NAMENODE -restoreFailedStorage check";
      CommandExecutor.Result cmdResult = executor.executeCommand(cmd);
      restore = fsi.getStorage().getRestoreFailedStorage();
      assertTrue("After check call restore is " + restore, restore);
      String commandOutput = cmdResult.getCommandOutput();
      commandOutput.trim();
      assertTrue(commandOutput.contains("restoreFailedStorage is set to true"));
      

    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to simulate interleaved checkpointing by 2 2NNs after a storage
   * directory has been taken offline. The first will cause the directory to
   * come back online, but it won't have any valid contents. The second 2NN will
   * then try to perform a checkpoint. The NN should not serve up the image or
   * edits from the restored (empty) dir.
   */
  @Test
  public void testMultipleSecondaryCheckpoint() throws IOException {
    
    SecondaryNameNode secondary = null;
    try {
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
          .manageNameDfsDirs(false).build();
      cluster.waitActive();
      
      secondary = new SecondaryNameNode(config);
  
      FSImage fsImage = cluster.getNameNode().getFSImage();
      printStorages(fsImage);
      
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/", "test");
      assertTrue(fs.mkdirs(testPath));
      
      printStorages(fsImage);
  
      // Take name1 offline
      invalidateStorage(fsImage, ImmutableSet.of(path1));
      
      // Simulate a 2NN beginning a checkpoint, but not finishing. This will
      // cause name1 to be restored.
      cluster.getNameNodeRpc().rollEditLog();
      
      printStorages(fsImage);
      
      // Now another 2NN comes along to do a full checkpoint.
      secondary.doCheckpoint();
      
      printStorages(fsImage);
      
      // The created file should still exist in the in-memory FS state after the
      // checkpoint.
      assertTrue("path exists before restart", fs.exists(testPath));
      
      secondary.shutdown();
      
      // Restart the NN so it reloads the edits from on-disk.
      cluster.restartNameNode();
  
      // The created file should still exist after the restart.
      assertTrue("path should still exist after restart", fs.exists(testPath));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (secondary != null) {
        secondary.shutdown();
      }
    }
  }

  /**
   * 1. create DFS cluster with 3 storage directories
   *    - 2 EDITS_IMAGE(name1, name2), 1 EDITS(name3)
   * 2. create a file
   * 3. corrupt/disable name2 and name3 by removing rwx permission
   * 4. run doCheckpoint
   *    - will fail on removed dirs (which invalidates them)
   * 5. write another file
   * 6. check there is only one healthy storage dir
   * 7. run doCheckpoint - recover should fail but checkpoint should succeed
   * 8. check there is still only one healthy storage dir
   * 9. restore the access permission for name2 and name 3, run checkpoint again
   * 10.verify there are 3 healthy storage dirs.
   */
  @Test
  public void testStorageRestoreFailure() throws Exception {
    SecondaryNameNode secondary = null;

    // On windows, revoking write+execute permission on name2 does not
    // prevent us from creating files in name2\current. Hence we revoke
    // permissions on name2\current for the test.
    String nameDir2 = Shell.WINDOWS ?
        (new File(path2, "current").getAbsolutePath()) : path2.toString();
    String nameDir3 = Shell.WINDOWS ?
        (new File(path3, "current").getAbsolutePath()) : path3.toString();

    try {
      cluster = new MiniDFSCluster.Builder(config)
          .numDataNodes(0)
          .manageNameDfsDirs(false).build();
      cluster.waitActive();

      secondary = new SecondaryNameNode(config);
      printStorages(cluster.getNameNode().getFSImage());

      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/", "test");
      assertTrue(fs.mkdirs(path));

      // invalidate storage by removing rwx permission from name2 and name3
      assertTrue(FileUtil.chmod(nameDir2, "000") == 0);
      assertTrue(FileUtil.chmod(nameDir3, "000") == 0);
      secondary.doCheckpoint(); // should remove name2 and name3

      printStorages(cluster.getNameNode().getFSImage());

      path = new Path("/", "test1");
      assertTrue(fs.mkdirs(path));
      assert (cluster.getNameNode().getFSImage().getStorage()
          .getNumStorageDirs() == 1);

      secondary.doCheckpoint(); // shouldn't be able to restore name 2 and 3
      assert (cluster.getNameNode().getFSImage().getStorage()
          .getNumStorageDirs() == 1);

      assertTrue(FileUtil.chmod(nameDir2, "755") == 0);
      assertTrue(FileUtil.chmod(nameDir3, "755") == 0);
      secondary.doCheckpoint(); // should restore name 2 and 3
      assert (cluster.getNameNode().getFSImage().getStorage()
          .getNumStorageDirs() == 3);

    } finally {
      if (path2.exists()) {
        FileUtil.chmod(nameDir2, "755");
      }
      if (path3.exists()) {
        FileUtil.chmod(nameDir3, "755");
      }
      if (cluster != null) {
        cluster.shutdown();
      }
      if (secondary != null) {
        secondary.shutdown();
      }
    }
  }
}
