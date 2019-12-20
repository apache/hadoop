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

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.test.Whitebox;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test various failure scenarios during saveNamespace() operation.
 * Cases covered:
 * <ol>
 * <li>Recover from failure while saving into the second storage directory</li>
 * <li>Recover from failure while moving current into lastcheckpoint.tmp</li>
 * <li>Recover from failure while moving lastcheckpoint.tmp into
 * previous.checkpoint</li>
 * <li>Recover from failure while rolling edits file</li>
 * </ol>
 */
public class TestSaveNamespace {
  static {
    GenericTestUtils.setLogLevel(FSImage.LOG, Level.TRACE);
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(TestSaveNamespace.class);

  private static class FaultySaveImage implements Answer<Void> {
    private int count = 0;
    private boolean throwRTE = true;

    // generate either a RuntimeException or IOException
    public FaultySaveImage(boolean throwRTE) {
      this.throwRTE = throwRTE;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      StorageDirectory sd = (StorageDirectory)args[1];

      if (count++ == 1) {
        LOG.info("Injecting fault for sd: " + sd);
        if (throwRTE) {
          throw new RuntimeException("Injected fault: saveFSImage second time");
        } else {
          throw new IOException("Injected fault: saveFSImage second time");
        }
      }
      LOG.info("Not injecting fault for sd: " + sd);
      return (Void)invocation.callRealMethod();
    }
  }

  private static class FaultyWriteProperties implements Answer<Void> {
    private int count = 0;
    private Fault faultType;

    FaultyWriteProperties(Fault faultType) {
      this.faultType = faultType;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      StorageDirectory sd = (StorageDirectory)args[0];

      if (faultType == Fault.WRITE_STORAGE_ALL ||
          (faultType==Fault.WRITE_STORAGE_ONE && count++==1)) {
        LOG.info("Injecting fault for sd: " + sd);
        throw new IOException("Injected fault: writeProperties second time");
      }
      LOG.info("Not injecting fault for sd: " + sd);
      return (Void)invocation.callRealMethod();
    }
  }

  private enum Fault {
    SAVE_SECOND_FSIMAGE_RTE,
    SAVE_SECOND_FSIMAGE_IOE,
    SAVE_ALL_FSIMAGES,
    WRITE_STORAGE_ALL,
    WRITE_STORAGE_ONE
  }

  private void saveNamespaceWithInjectedFault(Fault fault) throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.getFSImage();
    NNStorage storage = originalImage.getStorage();

    NNStorage spyStorage = spy(storage);
    originalImage.storage = spyStorage;

    FSImage spyImage = spy(originalImage);
    Whitebox.setInternalState(fsn, "fsImage", spyImage);

    boolean shouldFail = false; // should we expect the save operation to fail
    // inject fault
    switch(fault) {
    case SAVE_SECOND_FSIMAGE_RTE:
      // The spy throws a RuntimeException when writing to the second directory
      doAnswer(new FaultySaveImage(true)).
          when(spyImage).saveFSImage(
          any(),
          any(), any());
      shouldFail = false;
      break;
    case SAVE_SECOND_FSIMAGE_IOE:
      // The spy throws an IOException when writing to the second directory
      doAnswer(new FaultySaveImage(false)).
          when(spyImage).saveFSImage(
          any(),
          any(), any());
      shouldFail = false;
      break;
    case SAVE_ALL_FSIMAGES:
      // The spy throws IOException in all directories
      doThrow(new RuntimeException("Injected")).
          when(spyImage).saveFSImage(
          any(),
          any(), any());
      shouldFail = true;
      break;
    case WRITE_STORAGE_ALL:
      // The spy throws an exception before writing any VERSION files
      doAnswer(new FaultyWriteProperties(Fault.WRITE_STORAGE_ALL))
          .when(spyStorage).writeProperties(any());
      shouldFail = true;
      break;
    case WRITE_STORAGE_ONE:
      // The spy throws on exception on one particular storage directory
      doAnswer(new FaultyWriteProperties(Fault.WRITE_STORAGE_ONE))
        .when(spyStorage).writeProperties(any());
      shouldFail = false;
      break;
    default: fail("Unknown fail type");
      break;
    }

    try {
      doAnEdit(fsn, 1);

      // Save namespace - this may fail, depending on fault injected
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace(0, 0);
        if (shouldFail) {
          fail("Did not fail!");
        }
      } catch (Exception e) {
        if (!shouldFail) {
          throw e;
        } else {
          LOG.info("Test caught expected exception", e);
        }
      }
      
      fsn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      // Should still be able to perform edits
      doAnEdit(fsn, 2);

      // Now shut down and restart the namesystem
      originalImage.close();
      fsn.close();      
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = FSNamesystem.loadFromDisk(conf);

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
      checkEditExists(fsn, 2);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }

  /**
   * Verify that a saveNamespace command brings faulty directories
   * in fs.name.dir and fs.edit.dir back online.
   */
  @Test (timeout=30000)
  public void testReinsertnamedirsInSavenamespace() throws Exception {
    // create a configuration with the key to restore error
    // directories in fs.name.dir
    Configuration conf = getConf();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, true);

    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.getFSImage();
    NNStorage storage = originalImage.getStorage();

    FSImage spyImage = spy(originalImage);
    Whitebox.setInternalState(fsn, "fsImage", spyImage);
    
    FileSystem fs = FileSystem.getLocal(conf);
    File rootDir = storage.getStorageDir(0).getRoot();
    Path rootPath = new Path(rootDir.getPath(), "current");
    final FsPermission permissionNone = new FsPermission((short) 0);
    final FsPermission permissionAll = new FsPermission(
        FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
    fs.setPermission(rootPath, permissionNone);

    try {
      doAnEdit(fsn, 1);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

      // Save namespace - should mark the first storage dir as faulty
      // since it's not traversable.
      LOG.info("Doing the first savenamespace.");
      fsn.saveNamespace(0, 0);
      LOG.info("First savenamespace sucessful.");      
      
      assertTrue("Savenamespace should have marked one directory as bad." +
                 " But found " + storage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                   storage.getRemovedStorageDirs().size() == 1);

      fs.setPermission(rootPath, permissionAll);

      // The next call to savenamespace should try inserting the
      // erroneous directory back to fs.name.dir. This command should
      // be successful.
      LOG.info("Doing the second savenamespace.");
      fsn.saveNamespace(0, 0);
      LOG.warn("Second savenamespace sucessful.");
      assertTrue("Savenamespace should have been successful in removing " +
                 " bad directories from Image."  +
                 " But found " + storage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                 storage.getRemovedStorageDirs().size() == 0);

      // Now shut down and restart the namesystem
      LOG.info("Shutting down fsimage.");
      originalImage.close();
      fsn.close();      
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      LOG.info("Loading new FSmage from disk.");
      fsn = FSNamesystem.loadFromDisk(conf);

      // Make sure the image loaded including our edit.
      LOG.info("Checking reloaded image.");
      checkEditExists(fsn, 1);
      LOG.info("Reloaded image is good.");
    } finally {
      if (rootDir.exists()) {
        fs.setPermission(rootPath, permissionAll);
      }

      if (fsn != null) {
        try {
          fsn.close();
        } catch (Throwable t) {
          LOG.error("Failed to shut down", t);
        }
      }
    }
  }

  @Test (timeout=30000)
  public void testRTEWhileSavingSecondImage() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_SECOND_FSIMAGE_RTE);
  }

  @Test (timeout=30000)
  public void testIOEWhileSavingSecondImage() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_SECOND_FSIMAGE_IOE);
  }

  @Test (timeout=30000)
  public void testCrashInAllImageDirs() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_ALL_FSIMAGES);
  }
  
  @Test (timeout=30000)
  public void testCrashWhenWritingVersionFiles() throws Exception {
    saveNamespaceWithInjectedFault(Fault.WRITE_STORAGE_ALL);
  }
  
  @Test (timeout=30000)
  public void testCrashWhenWritingVersionFileInOneDir() throws Exception {
    saveNamespaceWithInjectedFault(Fault.WRITE_STORAGE_ONE);
  }
 

  /**
   * Test case where savenamespace fails in all directories
   * and then the NN shuts down. Here we should recover from the
   * failed checkpoint since it only affected ".ckpt" files, not
   * valid image files
   */
  @Test (timeout=30000)
  public void testFailedSaveNamespace() throws Exception {
    doTestFailedSaveNamespace(false);
  }

  /**
   * Test case where saveNamespace fails in all directories, but then
   * the operator restores the directories and calls it again.
   * This should leave the NN in a clean state for next start.
   */
  @Test (timeout=30000)
  public void testFailedSaveNamespaceWithRecovery() throws Exception {
    doTestFailedSaveNamespace(true);
  }

  /**
   * Injects a failure on all storage directories while saving namespace.
   *
   * @param restoreStorageAfterFailure if true, will try to save again after
   *   clearing the failure injection
   */
  public void doTestFailedSaveNamespace(boolean restoreStorageAfterFailure)
  throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    // Replace the FSImage with a spy
    final FSImage originalImage = fsn.getFSImage();
    NNStorage storage = originalImage.getStorage();
    // unlock any directories that
    // FSNamesystem's initialization may have locked
    storage.close();

    NNStorage spyStorage = spy(storage);
    originalImage.storage = spyStorage;
    FSImage spyImage = spy(originalImage);
    Whitebox.setInternalState(fsn, "fsImage", spyImage);

    spyImage.storage.setStorageDirectories(FSNamesystem.getNamespaceDirs(conf),
        FSNamesystem.getNamespaceEditsDirs(conf));

    doThrow(new IOException("Injected fault: saveFSImage")).
        when(spyImage).saveFSImage(any(), any(), any());

    try {
      doAnEdit(fsn, 1);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace(0, 0);
        fail("saveNamespace did not fail even when all directories failed!");
      } catch (IOException ioe) {
        LOG.info("Got expected exception", ioe);
      }
      
      // Ensure that, if storage dirs come back online, things work again.
      if (restoreStorageAfterFailure) {
        Mockito.reset(spyImage);
        spyStorage.setRestoreFailedStorage(true);
        fsn.saveNamespace(0, 0);
        checkEditExists(fsn, 1);
      }

      // Now shut down and restart the NN
      originalImage.close();
      fsn.close();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = FSNamesystem.loadFromDisk(conf);

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }

  @Test (timeout=30000)
  public void testSaveWhileEditsRolled() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);
      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace(0, 0);

      // Now shut down and restart the NN
      fsn.close();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = FSNamesystem.loadFromDisk(conf);

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
      checkEditExists(fsn, 2);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }
  
  @Test (timeout=30000)
  public void testTxIdPersistence() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    try {
      // We have a BEGIN_LOG_SEGMENT txn to start
      assertEquals(1, fsn.getEditLog().getLastWrittenTxId());
      doAnEdit(fsn, 1);
      assertEquals(2, fsn.getEditLog().getLastWrittenTxId());
      
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace(0, 0);

      // 2 more txns: END the first segment, BEGIN a new one
      assertEquals(4, fsn.getEditLog().getLastWrittenTxId());
      
      // Shut down and restart
      fsn.getFSImage().close();
      fsn.close();
      
      // 1 more txn to END that segment
      assertEquals(5, fsn.getEditLog().getLastWrittenTxId());
      fsn = null;
      
      fsn = FSNamesystem.loadFromDisk(conf);
      // 1 more txn to start new segment on restart
      assertEquals(6, fsn.getEditLog().getLastWrittenTxId());
      
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }
  
  @Test(timeout=20000)
  public void testCancelSaveNamespace() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);

    // Replace the FSImage with a spy
    final FSImage image = fsn.getFSImage();
    NNStorage storage = image.getStorage();
    // unlock any directories that
    // FSNamesystem's initialization may have locked
    storage.close();
    storage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));

    FSNamesystem spyFsn = spy(fsn);
    final FSNamesystem finalFsn = spyFsn;
    DelayAnswer delayer = new GenericTestUtils.DelayAnswer(LOG);
    BlockIdManager bid = spy(spyFsn.getBlockManager().getBlockIdManager());
    Whitebox.setInternalState(finalFsn.getBlockManager(),
        "blockIdManager", bid);
    doAnswer(delayer).when(bid).getGenerationStamp();

    ExecutorService pool = Executors.newFixedThreadPool(2);
    
    try {
      doAnEdit(fsn, 1);
      final Canceler canceler = new Canceler();
      
      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        Future<Void> saverFuture = pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            image.saveNamespace(finalFsn, NameNodeFile.IMAGE, canceler);
            return null;
          }
        });

        // Wait until saveNamespace calls getGenerationStamp
        delayer.waitForCall();
        // then cancel the saveNamespace
        Future<Void> cancelFuture = pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            canceler.cancel("cancelled");
            return null;
          }
        });
        // give the cancel call time to run
        Thread.sleep(500);
        
        // allow saveNamespace to proceed - it should check the cancel flag
        // after this point and throw an exception
        delayer.proceed();

        cancelFuture.get();
        saverFuture.get();
        fail("saveNamespace did not fail even though cancelled!");
      } catch (Throwable t) {
        GenericTestUtils.assertExceptionContains(
            "SaveNamespaceCancelledException", t);
      }
      LOG.info("Successfully cancelled a saveNamespace");


      // Check that we have only the original image and not any
      // cruft left over from half-finished images
      FSImageTestUtil.logStorageContents(LOG, storage);
      for (StorageDirectory sd : storage.dirIterable(null)) {
        File curDir = sd.getCurrentDir();
        GenericTestUtils.assertGlobEquals(curDir, "fsimage_.*",
            NNStorage.getImageFileName(0),
            NNStorage.getImageFileName(0) + MD5FileUtils.MD5_SUFFIX);
      }      
    } finally {
      fsn.close();
    }
  }

  /**
   * Test for save namespace should succeed when parent directory renamed with
   * open lease and destination directory exist. 
   * This test is a regression for HDFS-2827
   */
  @Test (timeout=30000)
  public void testSaveNamespaceWithRenamedLease() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    OutputStream out = null;
    try {
      fs.mkdirs(new Path("/test-target"));
      out = fs.create(new Path("/test-source/foo")); // don't close
      fs.rename(new Path("/test-source/"), new Path("/test-target/"));

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace(0, 0);
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } finally {
      IOUtils.cleanupWithLogger(LOG, out, fs);
      cluster.shutdown();
    }
  }
  
  @Test (timeout=30000)
  public void testSaveNamespaceWithDanglingLease() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      cluster.getNamesystem().leaseManager.addLease("me",
              INodeId.ROOT_INODE_ID + 1);
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace(0, 0);
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    } finally {
      cluster.shutdown();
    }
  }


  @Test
  public void testSkipSnapshotSection() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration())
        .numDataNodes(1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    OutputStream out = null;
    try {
      String path = "/skipSnapshot";
      out = fs.create(new Path(path));
      out.close();

      // add a bogus filediff
      FSDirectory dir = cluster.getNamesystem().getFSDirectory();
      INodeFile file = dir.getINode(path).asFile();
      file.addSnapshotFeature(null).getDiffs()
          .saveSelf2Snapshot(-1, file, null, false);

      // make sure it has a diff
      assertTrue("Snapshot fileDiff is missing.",
          file.getFileWithSnapshotFeature().getDiffs() != null);

      // saveNamespace
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace(0, 0);
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      // restart namenode
      cluster.restartNameNode(true);
      dir = cluster.getNamesystem().getFSDirectory();
      file = dir.getINode(path).asFile();

      // there should be no snapshot feature for the inode, when there is
      // no snapshot.
      assertTrue("There should be no snapshot feature for this INode.",
          file.getFileWithSnapshotFeature() == null);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSaveNamespaceBeforeShutdown() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    try {
      final FSImage fsimage = cluster.getNameNode().getFSImage();
      final long before = fsimage.getStorage().getMostRecentCheckpointTxId();

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      // set the timewindow to 1 hour and tx gap to 1000, which means that if
      // there is a checkpoint during the past 1 hour or the tx number happening
      // after the latest checkpoint is <= 1000, this saveNamespace request
      // will be ignored
      cluster.getNameNodeRpc().saveNamespace(3600, 1000);

      // make sure no new checkpoint was done
      long after = fsimage.getStorage().getMostRecentCheckpointTxId();
      Assert.assertEquals(before, after);

      Thread.sleep(1000);
      // do another checkpoint. this time set the timewindow to 1s
      // we should see a new checkpoint
      cluster.getNameNodeRpc().saveNamespace(1, 1000);
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      after = fsimage.getStorage().getMostRecentCheckpointTxId();
      Assert.assertTrue(after > before);

      fs.mkdirs(new Path("/foo/bar/baz")); // 3 new tx

      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNodeRpc().saveNamespace(3600, 5); // 3 + end/start segment
      long after2 = fsimage.getStorage().getMostRecentCheckpointTxId();
      // no checkpoint should be made
      Assert.assertEquals(after, after2);
      cluster.getNameNodeRpc().saveNamespace(3600, 3);
      after2 = fsimage.getStorage().getMostRecentCheckpointTxId();
      // a new checkpoint should be done
      Assert.assertTrue(after2 > after);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=30000)
  public void testTxFaultTolerance() throws Exception {
    String baseDir = MiniDFSCluster.getBaseDirectory();
    List<String> nameDirs = new ArrayList<>();
    nameDirs.add(fileAsURI(new File(baseDir, "name1")).toString());
    nameDirs.add(fileAsURI(new File(baseDir, "name2")).toString());

    Configuration conf = new HdfsConfiguration();
    String nameDirsStr = StringUtils.join(",", nameDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDirsStr);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDirsStr);

    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);
    try {
      // We have a BEGIN_LOG_SEGMENT txn to start
      assertEquals(1, fsn.getEditLog().getLastWrittenTxId());

      doAnEdit(fsn, 1);

      assertEquals(2, fsn.getEditLog().getLastWrittenTxId());

      // Shut down
      fsn.close();

      // Corrupt one of the seen_txid files
      File txidFile0 = new File(new URI(nameDirs.get(0) +
          "/current/seen_txid"));
      FileWriter fw = new FileWriter(txidFile0, false);
      try (PrintWriter pw = new PrintWriter(fw)) {
        pw.print("corrupt____!");
      }

      // Restart
      fsn = FSNamesystem.loadFromDisk(conf);
      assertEquals(4, fsn.getEditLog().getLastWrittenTxId());

      // Check seen_txid is same in both dirs
      File txidFile1 = new File(new URI(nameDirs.get(1) +
          "/current/seen_txid"));
      assertTrue(FileUtils.contentEquals(txidFile0, txidFile1));
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }

  private void doAnEdit(FSNamesystem fsn, int id) throws IOException {
    // Make an edit
    fsn.mkdirs("/test" + id, new PermissionStatus("test", "Test",
        new FsPermission((short)0777)), true);
  }

  private void checkEditExists(FSNamesystem fsn, int id) throws IOException {
    // Make sure the image loaded including our edit.
    assertNotNull(fsn.getFileInfo("/test" + id, false, false, false));
  }

  private Configuration getConf() throws IOException {
    String baseDir = MiniDFSCluster.getBaseDirectory();
    String nameDirs = fileAsURI(new File(baseDir, "name1")) + "," + 
                      fileAsURI(new File(baseDir, "name2"));

    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        "0.0.0.0:0");
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    return conf;
  }
}
