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
import static org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.assertNNHasCheckpoints;
import static org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.getNameNodeCurrentDirs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode.CheckpointStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestCheckpoint {

  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
  }

  static final Log LOG = LogFactory.getLog(TestCheckpoint.class); 
  
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;

  private CheckpointFaultInjector faultInjector;
    
  @Before
  public void setUp() throws IOException {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
    
    faultInjector = Mockito.mock(CheckpointFaultInjector.class);
    CheckpointFaultInjector.instance = faultInjector;
  }

  static void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[TestCheckpoint.fileSize];
    Random rand = new Random(TestCheckpoint.seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  @After
  public void checkForSNNThreads() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      LOG.info("Check thread: " + info.getThreadName());
      if (info.getThreadName().contains("SecondaryNameNode")) {
        fail("Leaked thread: " + info + "\n" +
            Joiner.on("\n").join(info.getStackTrace()));
      }
    }
    LOG.info("--------");
  }

  static void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    assertTrue(fileSys.exists(name));
    int replication = fileSys.getFileStatus(name).getReplication();
    assertEquals("replication for " + name, repl, replication);
    //We should probably test for more of the file properties.    
  }
  
  static void cleanupFile(FileSystem fileSys, Path name)
    throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * Verify that namenode does not startup if one namedir is bad.
   */
  @Test
  public void testNameDirError() throws IOException {
    LOG.info("Starting testNameDirError");
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    
    Collection<URI> nameDirs = cluster.getNameDirs(0);
    cluster.shutdown();
    cluster = null;
    
    for (URI nameDirUri : nameDirs) {
      File dir = new File(nameDirUri.getPath());
      
      try {
        // Simulate the mount going read-only
        dir.setWritable(false);
        cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .format(false)
          .build();
        fail("NN should have failed to start with " + dir + " set unreadable");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "storage directory does not exist or is not accessible",
            ioe);
      } finally {
        if (cluster != null) {
          cluster.shutdown();
          cluster = null;
        }
        dir.setWritable(true);
      }
    }
  }

  /**
   * Checks that an IOException in NNStorage.writeTransactionIdFile is handled
   * correctly (by removing the storage directory)
   * See https://issues.apache.org/jira/browse/HDFS-2011
   */
  @Test
  public void testWriteTransactionIdHandlesIOE() throws Exception {
    LOG.info("Check IOException handled correctly by writeTransactionIdFile");
    ArrayList<URI> fsImageDirs = new ArrayList<URI>();
    ArrayList<URI> editsDirs = new ArrayList<URI>();
    File filePath =
      new File(System.getProperty("test.build.data","/tmp"), "storageDirToCheck");
    assertTrue("Couldn't create directory storageDirToCheck",
               filePath.exists() || filePath.mkdirs());
    fsImageDirs.add(filePath.toURI());
    editsDirs.add(filePath.toURI());
    NNStorage nnStorage = new NNStorage(new HdfsConfiguration(),
      fsImageDirs, editsDirs);
    try {
      assertTrue("List of storage directories didn't have storageDirToCheck.",
                 nnStorage.getEditsDirectories().iterator().next().
                 toString().indexOf("storageDirToCheck") != -1);
      assertTrue("List of removed storage directories wasn't empty",
                 nnStorage.getRemovedStorageDirs().isEmpty());
    } finally {
      // Delete storage directory to cause IOException in writeTransactionIdFile 
      assertTrue("Couldn't remove directory " + filePath.getAbsolutePath(),
                 filePath.delete());
    }
    // Just call writeTransactionIdFile using any random number
    nnStorage.writeTransactionIdFileToStorage(1);
    List<StorageDirectory> listRsd = nnStorage.getRemovedStorageDirs();
    assertTrue("Removed directory wasn't what was expected",
               listRsd.size() > 0 && listRsd.get(listRsd.size() - 1).getRoot().
               toString().indexOf("storageDirToCheck") != -1);
  }

  /*
   * Simulate exception during edit replay.
   */
  @Test(timeout=5000)
  public void testReloadOnEditReplayFailure () throws IOException {
    Configuration conf = new HdfsConfiguration();
    FSDataOutputStream fos = null;
    SecondaryNameNode secondary = null;
    MiniDFSCluster cluster = null;
    FileSystem fs = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      secondary = startSecondaryNameNode(conf);
      fos = fs.create(new Path("tmpfile0"));
      fos.write(new byte[] { 0, 1, 2, 3 });
      secondary.doCheckpoint();
      fos.write(new byte[] { 0, 1, 2, 3 });
      fos.hsync();

      // Cause merge to fail in next checkpoint.
      Mockito.doThrow(new IOException(
          "Injecting failure during merge"))
          .when(faultInjector).duringMerge();

      try {
        secondary.doCheckpoint();
        fail("Fault injection failed.");
      } catch (IOException ioe) {
        // This is expected.
      } 
      Mockito.reset(faultInjector);
 
      // The error must be recorded, so next checkpoint will reload image.
      fos.write(new byte[] { 0, 1, 2, 3 });
      fos.hsync();
      
      assertTrue("Another checkpoint should have reloaded image",
          secondary.doCheckpoint());
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
      Mockito.reset(faultInjector);
    }
  }

  /*
   * Simulate 2NN exit due to too many merge failures.
   */
  @Test(timeout=10000)
  public void testTooManyEditReplayFailures() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY, "1");
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, "1");

    FSDataOutputStream fos = null;
    SecondaryNameNode secondary = null;
    MiniDFSCluster cluster = null;
    FileSystem fs = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .checkExitOnShutdown(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      fos = fs.create(new Path("tmpfile0"));
      fos.write(new byte[] { 0, 1, 2, 3 });

      // Cause merge to fail in next checkpoint.
      Mockito.doThrow(new IOException(
          "Injecting failure during merge"))
          .when(faultInjector).duringMerge();

      secondary = startSecondaryNameNode(conf);
      secondary.doWork();
      // Fail if we get here.
      fail("2NN did not exit.");
    } catch (ExitException ee) {
      // ignore
      ExitUtil.resetFirstExitException();
      assertEquals("Max retries", 1, secondary.getMergeErrorCount() - 1);
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
      Mockito.reset(faultInjector);
    }
  }

  /*
   * Simulate namenode crashing after rolling edit log.
   */
  @Test
  public void testSecondaryNamenodeError1()
    throws IOException {
    LOG.info("Starting testSecondaryNamenodeError1");
    Configuration conf = new HdfsConfiguration();
    Path file1 = new Path("checkpointxx.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edits log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      
      Mockito.doThrow(new IOException(
          "Injecting failure after rolling edit logs"))
          .when(faultInjector).afterSecondaryCallsRollEditLog();

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      
      Mockito.reset(faultInjector);
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the rolled edit logs.
    //
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
                                              .format(false).build();
    cluster.waitActive();
    
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /*
   * Simulate a namenode crash after uploading new image
   */
  @Test
  public void testSecondaryNamenodeError2() throws IOException {
    LOG.info("Starting testSecondaryNamenodeError2");
    Configuration conf = new HdfsConfiguration();
    Path file1 = new Path("checkpointyy.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after uploading the new fsimage.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      
      Mockito.doThrow(new IOException(
          "Injecting failure after uploading new image"))
          .when(faultInjector).afterSecondaryUploadsNewImage();

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      Mockito.reset(faultInjector);
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the rolled edit logs.
    //
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /*
   * Simulate a secondary namenode crash after rolling the edit log.
   */
  @Test
  public void testSecondaryNamenodeError3() throws IOException {
    LOG.info("Starting testSecondaryNamenodeError3");
    Configuration conf = new HdfsConfiguration();
    Path file1 = new Path("checkpointzz.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();

    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);

      Mockito.doThrow(new IOException(
          "Injecting failure after rolling edit logs"))
          .when(faultInjector).afterSecondaryCallsRollEditLog();

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      Mockito.reset(faultInjector);
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary and verify that 
      // a new rollEditLog suceedes inspite of the fact that 
      // edits.new already exists.
      //
      secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();  // this should work correctly
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file exists.
    // Then take another checkpoint to verify that the 
    // namenode restart accounted for the twice-rolled edit logs.
    //
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    try {
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Simulate a secondary node failure to transfer image
   * back to the name-node.
   * Used to truncate primary fsimage file.
   */
  @Test
  public void testSecondaryFailsToReturnImage() throws IOException {
    Mockito.doThrow(new IOException("If this exception is not caught by the " +
        "name-node, fs image will be truncated."))
        .when(faultInjector).aboutToSendFile(filePathContaining("secondary"));

    doSecondaryFailsToReturnImage();
  }
  
  /**
   * Similar to above test, but uses an unchecked Error, and causes it
   * before even setting the length header. This used to cause image
   * truncation. Regression test for HDFS-3330.
   */
  @Test
  public void testSecondaryFailsWithErrorBeforeSettingHeaders()
      throws IOException {
    Mockito.doThrow(new Error("If this exception is not caught by the " +
        "name-node, fs image will be truncated."))
        .when(faultInjector).beforeGetImageSetsHeaders();

    doSecondaryFailsToReturnImage();
  }

  private void doSecondaryFailsToReturnImage() throws IOException {
    LOG.info("Starting testSecondaryFailsToReturnImage");
    Configuration conf = new HdfsConfiguration();
    Path file1 = new Path("checkpointRI.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    FSImage image = cluster.getNameNode().getFSImage();
    try {
      assertTrue(!fileSys.exists(file1));
      StorageDirectory sd = image.getStorage().getStorageDir(0);
      
      File latestImageBeforeCheckpoint = FSImageTestUtil.findLatestImageFile(sd);
      long fsimageLength = latestImageBeforeCheckpoint.length();
      //
      // Make the checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);

      try {
        secondary.doCheckpoint();  // this should fail
        fail("Checkpoint succeeded even though we injected an error!");
      } catch (IOException e) {
        // check that it's the injected exception
        GenericTestUtils.assertExceptionContains(
            "If this exception is not caught", e);
      }
      Mockito.reset(faultInjector);

      // Verify that image file sizes did not change.
      for (StorageDirectory sd2 :
        image.getStorage().dirIterable(NameNodeDirType.IMAGE)) {
        
        File thisNewestImage = FSImageTestUtil.findLatestImageFile(sd2);
        long len = thisNewestImage.length();
        assertEquals(fsimageLength, len);
      }

      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  private File filePathContaining(final String substring) {
    return Mockito.<File>argThat(
        new ArgumentMatcher<File>() {
          @Override
          public boolean matches(Object argument) {
            String path = ((File)argument).getAbsolutePath();
            return path.contains(substring);
          }
        });
  }

  /**
   * Simulate 2NN failing to send the whole file (error type 3)
   * The length header in the HTTP transfer should prevent
   * this from corrupting the NN.
   */
  @Test
  public void testNameNodeImageSendFailWrongSize()
      throws IOException {
    LOG.info("Starting testNameNodeImageSendFailWrongSize");
    
    Mockito.doReturn(true).when(faultInjector)
      .shouldSendShortFile(filePathContaining("fsimage"));
    doSendFailTest("is not of the advertised size");
  }

  /**
   * Simulate 2NN sending a corrupt image (error type 4)
   * The digest header in the HTTP transfer should prevent
   * this from corrupting the NN.
   */
  @Test
  public void testNameNodeImageSendFailWrongDigest()
      throws IOException {
    LOG.info("Starting testNameNodeImageSendFailWrongDigest");

    Mockito.doReturn(true).when(faultInjector)
        .shouldCorruptAByte(Mockito.any(File.class));
    doSendFailTest("does not match advertised digest");
  }

  /**
   * Run a test where the 2NN runs into some kind of error when
   * sending the checkpoint back to the NN.
   * @param exceptionSubstring an expected substring of the triggered exception
   */
  private void doSendFailTest(String exceptionSubstring)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    Path file1 = new Path("checkpoint-doSendFailTest-doSendFailTest.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);

      try {
        secondary.doCheckpoint();  // this should fail
        fail("Did not get expected exception");
      } catch (IOException e) {
        // We only sent part of the image. Have to trigger this exception
        GenericTestUtils.assertExceptionContains(exceptionSubstring, e);
      }
      Mockito.reset(faultInjector);
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary and verify that 
      // a new rollEditLog succedes in spite of the fact that we had
      // a partially failed checkpoint previously.
      //
      secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();  // this should work correctly
      secondary.shutdown();

      //
      // Create a new file
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test that the NN locks its storage and edits directories, and won't start up
   * if the directories are already locked
   **/
  @Test
  public void testNameDirLocking() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    
    // Start a NN, and verify that lock() fails in all of the configured
    // directories
    StorageDirectory savedSd = null;
    try {
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      for (StorageDirectory sd : storage.dirIterable(null)) {
        assertLockFails(sd);
        savedSd = sd;
      }
    } finally {
      cluster.shutdown();
    }
    assertNotNull(savedSd);
    
    // Lock one of the saved directories, then start the NN, and make sure it
    // fails to start
    assertClusterStartFailsWhenDirLocked(conf, savedSd);
  }

  /**
   * Test that, if the edits dir is separate from the name dir, it is
   * properly locked.
   **/
  @Test
  public void testSeparateEditsDirLocking() throws IOException {
    Configuration conf = new HdfsConfiguration();
    File editsDir = new File(MiniDFSCluster.getBaseDirectory() +
        "/testSeparateEditsDirLocking");
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        editsDir.getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .manageNameDfsDirs(false)
      .numDataNodes(0)
      .build();
    
    // Start a NN, and verify that lock() fails in all of the configured
    // directories
    StorageDirectory savedSd = null;
    try {
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
        assertEquals(editsDir.getAbsoluteFile(), sd.getRoot());
        assertLockFails(sd);
        savedSd = sd;
      }
    } finally {
      cluster.shutdown();
    }
    assertNotNull(savedSd);
    
    // Lock one of the saved directories, then start the NN, and make sure it
    // fails to start
    assertClusterStartFailsWhenDirLocked(conf, savedSd);
  }
  
  /**
   * Test that the SecondaryNameNode properly locks its storage directories.
   */
  @Test
  public void testSecondaryNameNodeLocking() throws Exception {
    // Start a primary NN so that the secondary will start successfully
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    
    SecondaryNameNode secondary = null;
    try {
      StorageDirectory savedSd = null;
      // Start a secondary NN, then make sure that all of its storage
      // dirs got locked.
      secondary = startSecondaryNameNode(conf);
      
      NNStorage storage = secondary.getFSImage().getStorage();
      for (StorageDirectory sd : storage.dirIterable(null)) {
        assertLockFails(sd);
        savedSd = sd;
      }
      LOG.info("===> Shutting down first 2NN");
      secondary.shutdown();
      secondary = null;

      LOG.info("===> Locking a dir, starting second 2NN");
      // Lock one of its dirs, make sure it fails to start
      LOG.info("Trying to lock" + savedSd);
      savedSd.lock();
      try {
        secondary = startSecondaryNameNode(conf);
        assertFalse("Should fail to start 2NN when " + savedSd + " is locked",
            savedSd.isLockSupported());
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains("already locked", ioe);
      } finally {
        savedSd.unlock();
      }
      
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      cluster.shutdown();
    }
  }
  
  /**
   * Test that, an attempt to lock a storage that is already locked by a nodename,
   * logs error message that includes JVM name of the namenode that locked it.
   */
  @Test
  public void testStorageAlreadyLockedErrorMessage() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    
    StorageDirectory savedSd = null;
    try {
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      for (StorageDirectory sd : storage.dirIterable(null)) {
        assertLockFails(sd);
        savedSd = sd;
      }
      
      LogCapturer logs = GenericTestUtils.LogCapturer.captureLogs(LogFactory.getLog(Storage.class));
      try {
        // try to lock the storage that's already locked
        savedSd.lock();
        fail("Namenode should not be able to lock a storage that is already locked");
      } catch (IOException ioe) {
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        assertTrue("Error message does not include JVM name '" + jvmName 
            + "'", logs.getOutput().contains(jvmName));
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Assert that the given storage directory can't be locked, because
   * it's already locked.
   */
  private static void assertLockFails(StorageDirectory sd) {
    try {
      sd.lock();
      // If the above line didn't throw an exception, then
      // locking must not be supported
      assertFalse(sd.isLockSupported());
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("already locked", ioe);
    }
  }
  
  /**
   * Assert that, if sdToLock is locked, the cluster is not allowed to start up.
   * @param conf cluster conf to use
   * @param sdToLock the storage directory to lock
   */
  private static void assertClusterStartFailsWhenDirLocked(
      Configuration conf, StorageDirectory sdToLock) throws IOException {
    // Lock the edits dir, then start the NN, and make sure it fails to start
    sdToLock.lock();
    try {      
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .format(false)
        .manageNameDfsDirs(false)
        .numDataNodes(0)
        .build();
      assertFalse("cluster should fail to start after locking " +
          sdToLock, sdToLock.isLockSupported());
      cluster.shutdown();
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains("already locked", ioe);
    } finally {
      sdToLock.unlock();
    }
  }

  /**
   * Test the importCheckpoint startup option. Verifies:
   * 1. if the NN already contains an image, it will not be allowed
   *   to import a checkpoint.
   * 2. if the NN does not contain an image, importing a checkpoint
   *    succeeds and re-saves the image
   */
  @Test
  public void testImportCheckpoint() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Path testPath = new Path("/testfile");
    SecondaryNameNode snn = null;
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .build();
    Collection<URI> nameDirs = cluster.getNameDirs(0);
    try {
      // Make an entry in the namespace, used for verifying checkpoint
      // later.
      cluster.getFileSystem().mkdirs(testPath);
      
      // Take a checkpoint
      snn = startSecondaryNameNode(conf);
      snn.doCheckpoint();
    } finally {
      if (snn != null) {
        snn.shutdown();
      }
      cluster.shutdown();
      cluster = null;
    }
    
    LOG.info("Trying to import checkpoint when the NameNode already " +
    		"contains an image. This should fail.");
    try {
      cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .format(false)
      .startupOption(StartupOption.IMPORT)
      .build();
      fail("NameNode did not fail to start when it already contained " +
      		"an image");
    } catch (IOException ioe) {
      // Expected
      GenericTestUtils.assertExceptionContains(
          "NameNode already contains an image", ioe);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
    
    LOG.info("Removing NN storage contents");
    for(URI uri : nameDirs) {
      File dir = new File(uri.getPath());
      LOG.info("Cleaning " + dir);
      removeAndRecreateDir(dir);
    }
    
    LOG.info("Trying to import checkpoint");
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .format(false)
        .numDataNodes(0)
        .startupOption(StartupOption.IMPORT)
        .build();
      
      assertTrue("Path from checkpoint should exist after import",
          cluster.getFileSystem().exists(testPath));

      // Make sure that the image got saved on import
      FSImageTestUtil.assertNNHasCheckpoints(cluster, Ints.asList(3));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private static void removeAndRecreateDir(File dir) throws IOException {
    if(dir.exists())
      if(!(FileUtil.fullyDelete(dir)))
        throw new IOException("Cannot remove directory: " + dir);
    if (!dir.mkdirs())
      throw new IOException("Cannot create directory " + dir);
  }
  
  SecondaryNameNode startSecondaryNameNode(Configuration conf
                                          ) throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    return new SecondaryNameNode(conf);
  }
  
  SecondaryNameNode startSecondaryNameNode(Configuration conf, int index)
      throws IOException {
    Configuration snnConf = new Configuration(conf);
    snnConf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    snnConf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/2nn-" + index);
    return new SecondaryNameNode(snnConf);
  }

  /**
   * Tests checkpoint in HDFS.
   */
  @Test
  public void testCheckpoint() throws IOException {
    Path file1 = new Path("checkpoint.dat");
    Path file2 = new Path("checkpoint2.dat");
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    replication = (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes).build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();

    try {
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
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file1 still exist.
    //
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    Path tmpDir = new Path("/tmp_tmp");
    try {
      // check that file1 still exists
      checkFile(fileSys, file1, replication);
      cleanupFile(fileSys, file1);

      // create new file file2
      writeFile(fileSys, file2, replication);
      checkFile(fileSys, file2, replication);

      //
      // Take a checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      secondary.doCheckpoint();
      
      fileSys.delete(tmpDir, true);
      fileSys.mkdirs(tmpDir);
      secondary.doCheckpoint();
      
      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }

    //
    // Restart cluster and verify that file2 exists and
    // file1 does not exist.
    //
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();

    assertTrue(!fileSys.exists(file1));
    assertTrue(fileSys.exists(tmpDir));

    try {
      // verify that file2 exists
      checkFile(fileSys, file2, replication);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Tests save namespace.
   */
  @Test
  public void testSaveNamespace() throws IOException {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    FileContext fc;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(true).build();
      cluster.waitActive();
      fs = (DistributedFileSystem)(cluster.getFileSystem());
      fc = FileContext.getFileContext(cluster.getURI(0));

      // Saving image without safe mode should fail
      DFSAdmin admin = new DFSAdmin(conf);
      String[] args = new String[]{"-saveNamespace"};
      try {
        admin.run(args);
      } catch(IOException eIO) {
        assertTrue(eIO.getLocalizedMessage().contains("Safe mode should be turned ON"));
      } catch(Exception e) {
        throw new IOException(e);
      }
      // create new file
      Path file = new Path("namespace.dat");
      writeFile(fs, file, replication);
      checkFile(fs, file, replication);

      // create new link
      Path symlink = new Path("file.link");
      fc.createSymlink(file, symlink, false);
      assertTrue(fc.getFileLinkStatus(symlink).isSymlink());

      // verify that the edits file is NOT empty
      Collection<URI> editsDirs = cluster.getNameEditsDirs(0);
      for(URI uri : editsDirs) {
        File ed = new File(uri.getPath());
        assertTrue(new File(ed, "current/"
                            + NNStorage.getInProgressEditsFileName(1))
                   .length() > Integer.SIZE/Byte.SIZE);
      }

      // Saving image in safe mode should succeed
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        admin.run(args);
      } catch(Exception e) {
        throw new IOException(e);
      }
      
      final int EXPECTED_TXNS_FIRST_SEG = 12;
      
      // the following steps should have happened:
      //   edits_inprogress_1 -> edits_1-12  (finalized)
      //   fsimage_12 created
      //   edits_inprogress_13 created
      //
      for(URI uri : editsDirs) {
        File ed = new File(uri.getPath());
        File curDir = new File(ed, "current");
        LOG.info("Files in " + curDir + ":\n  " +
            Joiner.on("\n  ").join(curDir.list()));
        // Verify that the first edits file got finalized
        File originalEdits = new File(curDir,
                                      NNStorage.getInProgressEditsFileName(1));
        assertFalse(originalEdits.exists());
        File finalizedEdits = new File(curDir,
            NNStorage.getFinalizedEditsFileName(1, EXPECTED_TXNS_FIRST_SEG));
        GenericTestUtils.assertExists(finalizedEdits);
        assertTrue(finalizedEdits.length() > Integer.SIZE/Byte.SIZE);

        GenericTestUtils.assertExists(new File(ed, "current/"
                       + NNStorage.getInProgressEditsFileName(
                           EXPECTED_TXNS_FIRST_SEG + 1)));
      }
      
      Collection<URI> imageDirs = cluster.getNameDirs(0);
      for (URI uri : imageDirs) {
        File imageDir = new File(uri.getPath());
        File savedImage = new File(imageDir, "current/"
                                   + NNStorage.getImageFileName(
                                       EXPECTED_TXNS_FIRST_SEG));
        assertTrue("Should have saved image at " + savedImage,
            savedImage.exists());        
      }

      // restart cluster and verify file exists
      cluster.shutdown();
      cluster = null;

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
      cluster.waitActive();
      fs = (DistributedFileSystem)(cluster.getFileSystem());
      checkFile(fs, file, replication);
      fc = FileContext.getFileContext(cluster.getURI(0));
      assertTrue(fc.getFileLinkStatus(symlink).isSymlink());
    } finally {
      try {
        if(fs != null) fs.close();
        if(cluster!= null) cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Failed to shutdown", t);
      }
    }
  }
  
  /* Test case to test CheckpointSignature */
  @SuppressWarnings("deprecation")
  @Test
  public void testCheckpointSignature() throws IOException {

    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
        .format(true).build();
    NameNode nn = cluster.getNameNode();
    NamenodeProtocols nnRpc = nn.getRpcServer();

    SecondaryNameNode secondary = startSecondaryNameNode(conf);
    // prepare checkpoint image
    secondary.doCheckpoint();
    CheckpointSignature sig = nnRpc.rollEditLog();
    // manipulate the CheckpointSignature fields
    sig.setBlockpoolID("somerandomebpid");
    sig.clusterID = "somerandomcid";
    try {
      sig.validateStorageInfo(nn.getFSImage()); // this should fail
      assertTrue("This test is expected to fail.", false);
    } catch (Exception ignored) {
    }

    secondary.shutdown();
    cluster.shutdown();
  }
  
  /**
   * Tests the following sequence of events:
   * - secondary successfully makes a checkpoint
   * - it then fails while trying to upload it
   * - it then fails again for the same reason
   * - it then tries to checkpoint a third time
   */
  @Test
  public void testCheckpointAfterTwoFailedUploads() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .format(true).build();
  
      secondary = startSecondaryNameNode(conf);

      Mockito.doThrow(new IOException(
          "Injecting failure after rolling edit logs"))
          .when(faultInjector).afterSecondaryCallsRollEditLog();
      
      // Fail to checkpoint once
      try {
        secondary.doCheckpoint();
        fail("Should have failed upload");
      } catch (IOException ioe) {
        LOG.info("Got expected failure", ioe);
        assertTrue(ioe.toString().contains("Injecting failure"));
      }

      // Fail to checkpoint again
      try {
        secondary.doCheckpoint();
        fail("Should have failed upload");
      } catch (IOException ioe) {
        LOG.info("Got expected failure", ioe);
        assertTrue(ioe.toString().contains("Injecting failure"));
      } finally {
        Mockito.reset(faultInjector);
      }

      // Now with the cleared error simulation, it should succeed
      secondary.doCheckpoint();
      
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Starts two namenodes and two secondary namenodes, verifies that secondary
   * namenodes are configured correctly to talk to their respective namenodes
   * and can do the checkpoint.
   * 
   * @throws IOException
   */
  @Test
  public void testMultipleSecondaryNamenodes() throws IOException {
    Configuration conf = new HdfsConfiguration();
    String nameserviceId1 = "ns1";
    String nameserviceId2 = "ns2";
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, nameserviceId1
        + "," + nameserviceId2);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .build();
    Configuration snConf1 = new HdfsConfiguration(cluster.getConfiguration(0));
    Configuration snConf2 = new HdfsConfiguration(cluster.getConfiguration(1));
    InetSocketAddress nn1RpcAddress =
      cluster.getNameNode(0).getNameNodeAddress();
    InetSocketAddress nn2RpcAddress =
      cluster.getNameNode(1).getNameNodeAddress();
    String nn1 = nn1RpcAddress.getHostName() + ":" + nn1RpcAddress.getPort();
    String nn2 = nn2RpcAddress.getHostName() + ":" + nn2RpcAddress.getPort();

    // Set the Service Rpc address to empty to make sure the node specific
    // setting works
    snConf1.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "");
    snConf2.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "");

    // Set the nameserviceIds
    snConf1.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nameserviceId1), nn1);
    snConf2.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nameserviceId2), nn2);

    SecondaryNameNode secondary1 = startSecondaryNameNode(snConf1);
    SecondaryNameNode secondary2 = startSecondaryNameNode(snConf2);

    // make sure the two secondary namenodes are talking to correct namenodes.
    assertEquals(secondary1.getNameNodeAddress().getPort(), nn1RpcAddress.getPort());
    assertEquals(secondary2.getNameNodeAddress().getPort(), nn2RpcAddress.getPort());
    assertTrue(secondary1.getNameNodeAddress().getPort() != secondary2
        .getNameNodeAddress().getPort());

    // both should checkpoint.
    secondary1.doCheckpoint();
    secondary2.doCheckpoint();
    secondary1.shutdown();
    secondary2.shutdown();
    cluster.shutdown();
  }
  
  /**
   * Test that the secondary doesn't have to re-download image
   * if it hasn't changed.
   */
  @Test
  public void testSecondaryImageDownload() throws IOException {
    LOG.info("Starting testSecondaryImageDownload");
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    Path dir = new Path("/checkpoint");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(true).build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    FSImage image = cluster.getNameNode().getFSImage();
    try {
      assertTrue(!fileSys.exists(dir));
      //
      // Make the checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);

      File secondaryDir = new File(MiniDFSCluster.getBaseDirectory(), "namesecondary1");
      File secondaryCurrent = new File(secondaryDir, "current");

      long expectedTxIdToDownload = cluster.getNameNode().getFSImage()
      .getStorage().getMostRecentCheckpointTxId();

      File secondaryFsImageBefore = new File(secondaryCurrent,
          NNStorage.getImageFileName(expectedTxIdToDownload));
      File secondaryFsImageAfter = new File(secondaryCurrent,
          NNStorage.getImageFileName(expectedTxIdToDownload + 2));
      
      assertFalse("Secondary should start with empty current/ dir " +
          "but " + secondaryFsImageBefore + " exists",
          secondaryFsImageBefore.exists());

      assertTrue("Secondary should have loaded an image",
          secondary.doCheckpoint());
      
      assertTrue("Secondary should have downloaded original image",
          secondaryFsImageBefore.exists());
      assertTrue("Secondary should have created a new image",
          secondaryFsImageAfter.exists());
      
      long fsimageLength = secondaryFsImageBefore.length();
      assertEquals("Image size should not have changed",
          fsimageLength,
          secondaryFsImageAfter.length());

      // change namespace
      fileSys.mkdirs(dir);
      
      assertFalse("Another checkpoint should not have to re-load image",
          secondary.doCheckpoint());
      
      for (StorageDirectory sd :
        image.getStorage().dirIterable(NameNodeDirType.IMAGE)) {
        File imageFile = NNStorage.getImageFile(sd,
            expectedTxIdToDownload + 5);
        assertTrue("Image size increased",
            imageFile.length() > fsimageLength);
      }

      secondary.shutdown();
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test case where two secondary namenodes are checkpointing the same
   * NameNode. This differs from {@link #testMultipleSecondaryNamenodes()}
   * since that test runs against two distinct NNs.
   * 
   * This case tests the following interleaving:
   * - 2NN A downloads image (up to txid 2)
   * - 2NN A about to save its own checkpoint
   * - 2NN B downloads image (up to txid 4)
   * - 2NN B uploads checkpoint (txid 4)
   * - 2NN A uploads checkpoint (txid 2)
   * 
   * It verifies that this works even though the earlier-txid checkpoint gets
   * uploaded after the later-txid checkpoint.
   */
  @Test
  public void testMultipleSecondaryNNsAgainstSameNN() throws Exception {
    Configuration conf = new HdfsConfiguration();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
    .numDataNodes(0)
    .format(true).build();

    SecondaryNameNode secondary1 = null, secondary2 = null;
    try {
      // Start 2NNs
      secondary1 = startSecondaryNameNode(conf, 1);
      secondary2 = startSecondaryNameNode(conf, 2);
      
      // Make the first 2NN's checkpoint process delayable - we can pause it
      // right before it saves its checkpoint image.
      CheckpointStorage spyImage1 = spyOnSecondaryImage(secondary1);
      DelayAnswer delayer = new DelayAnswer(LOG);
      Mockito.doAnswer(delayer).when(spyImage1)
        .saveFSImageInAllDirs(Mockito.<FSNamesystem>any(), Mockito.anyLong());

      // Set up a thread to do a checkpoint from the first 2NN
      DoCheckpointThread checkpointThread = new DoCheckpointThread(secondary1);
      checkpointThread.start();

      // Wait for the first checkpointer to get to where it should save its image.
      delayer.waitForCall();
      
      // Now make the second checkpointer run an entire checkpoint
      secondary2.doCheckpoint();
      
      // Let the first one finish
      delayer.proceed();
      
      // It should have succeeded even though another checkpoint raced with it.
      checkpointThread.join();
      checkpointThread.propagateExceptions();
      
      // primary should record "last checkpoint" as the higher txid (even though
      // a checkpoint with a lower txid finished most recently)
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      assertEquals(4, storage.getMostRecentCheckpointTxId());

      // Should have accepted both checkpoints
      assertNNHasCheckpoints(cluster, ImmutableList.of(2,4));
      
      // Now have second one checkpoint one more time just to make sure that
      // the NN isn't left in a broken state
      secondary2.doCheckpoint();
      
      // NN should have received new checkpoint
      assertEquals(6, storage.getMostRecentCheckpointTxId());
    } finally {
      cleanup(secondary1);
      cleanup(secondary2);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    
    // Validate invariant that files named the same are the same.
    assertParallelFilesInvariant(cluster, ImmutableList.of(secondary1, secondary2));

    // NN should have removed the checkpoint at txid 2 at this point, but has
    // one at txid 6
    assertNNHasCheckpoints(cluster, ImmutableList.of(4,6));
  }
  
  
  /**
   * Test case where two secondary namenodes are checkpointing the same
   * NameNode. This differs from {@link #testMultipleSecondaryNamenodes()}
   * since that test runs against two distinct NNs.
   * 
   * This case tests the following interleaving:
   * - 2NN A) calls rollEdits()
   * - 2NN B) calls rollEdits()
   * - 2NN A) paused at getRemoteEditLogManifest()
   * - 2NN B) calls getRemoteEditLogManifest() (returns up to txid 4)
   * - 2NN B) uploads checkpoint fsimage_4
   * - 2NN A) allowed to proceed, also returns up to txid 4
   * - 2NN A) uploads checkpoint fsimage_4 as well, should fail gracefully
   * 
   * It verifies that one of the two gets an error that it's uploading a
   * duplicate checkpoint, and the other one succeeds.
   */
  @Test
  public void testMultipleSecondaryNNsAgainstSameNN2() throws Exception {
    Configuration conf = new HdfsConfiguration();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
    .numDataNodes(0)
    .format(true).build();

    SecondaryNameNode secondary1 = null, secondary2 = null;
    try {
      // Start 2NNs
      secondary1 = startSecondaryNameNode(conf, 1);
      secondary2 = startSecondaryNameNode(conf, 2);
      
      // Make the first 2NN's checkpoint process delayable - we can pause it
      // right before it calls getRemoteEditLogManifest.
      // The method to set up a spy on an RPC protocol is a little bit involved
      // since we can't spy directly on a proxy object. This sets up a mock
      // which delegates all its calls to the original object, instead.
      final NamenodeProtocol origNN = secondary1.getNameNode();
      final Answer<Object> delegator = new GenericTestUtils.DelegateAnswer(origNN);
      NamenodeProtocol spyNN = Mockito.mock(NamenodeProtocol.class, delegator);
      DelayAnswer delayer = new DelayAnswer(LOG) {
        @Override
        protected Object passThrough(InvocationOnMock invocation) throws Throwable {
          return delegator.answer(invocation);
        }
      };
      secondary1.setNameNode(spyNN);
      
      Mockito.doAnswer(delayer).when(spyNN)
        .getEditLogManifest(Mockito.anyLong());      
          
      // Set up a thread to do a checkpoint from the first 2NN
      DoCheckpointThread checkpointThread = new DoCheckpointThread(secondary1);
      checkpointThread.start();

      // Wait for the first checkpointer to be about to call getEditLogManifest
      delayer.waitForCall();
      
      // Now make the second checkpointer run an entire checkpoint
      secondary2.doCheckpoint();
      
      // NN should have now received fsimage_4
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      assertEquals(4, storage.getMostRecentCheckpointTxId());
      
      // Let the first one finish
      delayer.proceed();
      
      // Letting the first node continue, it should try to upload the
      // same image, and gracefully ignore it, while logging an
      // error message.
      checkpointThread.join();
      checkpointThread.propagateExceptions();
      
      // primary should still consider fsimage_4 the latest
      assertEquals(4, storage.getMostRecentCheckpointTxId());
      
      // Now have second one checkpoint one more time just to make sure that
      // the NN isn't left in a broken state
      secondary2.doCheckpoint();
      assertEquals(6, storage.getMostRecentCheckpointTxId());
      
      // Should have accepted both checkpoints
      assertNNHasCheckpoints(cluster, ImmutableList.of(4,6));

      // Let the first one also go again on its own to make sure it can
      // continue at next checkpoint
      secondary1.setNameNode(origNN);
      secondary1.doCheckpoint();
      
      // NN should have received new checkpoint
      assertEquals(8, storage.getMostRecentCheckpointTxId());
    } finally {
      cleanup(secondary1);
      cleanup(secondary2);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    
    // Validate invariant that files named the same are the same.
    assertParallelFilesInvariant(cluster, ImmutableList.of(secondary1, secondary2));
    // Validate that the NN received checkpoints at expected txids
    // (i.e that both checkpoints went through)
    assertNNHasCheckpoints(cluster, ImmutableList.of(6,8));
  }
  
  /**
   * Test case where the name node is reformatted while the secondary namenode
   * is running. The secondary should shut itself down if if talks to a NN
   * with the wrong namespace.
   */
  @Test
  public void testReformatNNBetweenCheckpoints() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        1);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
      int origPort = cluster.getNameNodePort();
      int origHttpPort = cluster.getNameNode().getHttpAddress().getPort();
      secondary = startSecondaryNameNode(conf);

      // secondary checkpoints once
      secondary.doCheckpoint();

      // we reformat primary NN
      cluster.shutdown();
      cluster = null;

      // Brief sleep to make sure that the 2NN's IPC connection to the NN
      // is dropped.
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
      }
      
      // Start a new NN with the same host/port.
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .nameNodePort(origPort)
        .nameNodeHttpPort(origHttpPort)
        .format(true).build();

      try {
        secondary.doCheckpoint();
        fail("Should have failed checkpoint against a different namespace");
      } catch (IOException ioe) {
        LOG.info("Got expected failure", ioe);
        assertTrue(ioe.toString().contains("Inconsistent checkpoint"));
      }
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }
  
  /**
   * Test that the primary NN will not serve any files to a 2NN who doesn't
   * share its namespace ID, and also will not accept any files from one.
   */
  @Test
  public void testNamespaceVerifiedOnFileTransfer() throws IOException {
    MiniDFSCluster cluster = null;
    
    Configuration conf = new HdfsConfiguration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
      
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      String fsName = NetUtils.getHostPortString(
          cluster.getNameNode().getHttpAddress());

      // Make a finalized log on the server side. 
      nn.rollEditLog();
      RemoteEditLogManifest manifest = nn.getEditLogManifest(1);
      RemoteEditLog log = manifest.getLogs().get(0);
      
      NNStorage dstImage = Mockito.mock(NNStorage.class);
      Mockito.doReturn(Lists.newArrayList(new File("/wont-be-written")))
        .when(dstImage).getFiles(
            Mockito.<NameNodeDirType>anyObject(), Mockito.anyString());
      
      Mockito.doReturn(new StorageInfo(1, 1, "X", 1).toColonSeparatedString())
        .when(dstImage).toColonSeparatedString();

      try {
        TransferFsImage.downloadImageToStorage(fsName, 0, dstImage, false);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }

      try {
        TransferFsImage.downloadEditsToStorage(fsName, log, dstImage);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }

      try {
        InetSocketAddress fakeAddr = new InetSocketAddress(1);
        TransferFsImage.uploadImageFromStorage(fsName, fakeAddr, dstImage, 0);
        fail("Storage info was not verified");
      } catch (IOException ioe) {
        String msg = StringUtils.stringifyException(ioe);
        assertTrue(msg, msg.contains("but the secondary expected"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }

  /**
   * Test that, if a storage directory is failed when a checkpoint occurs,
   * the non-failed storage directory receives the checkpoint.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCheckpointWithFailedStorageDir() throws Exception {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    File currentDir = null;
    
    Configuration conf = new HdfsConfiguration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
  
      secondary = startSecondaryNameNode(conf);

      // Checkpoint once
      secondary.doCheckpoint();

      // Now primary NN experiences failure of a volume -- fake by
      // setting its current dir to a-x permissions
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      StorageDirectory sd0 = storage.getStorageDir(0);
      StorageDirectory sd1 = storage.getStorageDir(1);
      
      currentDir = sd0.getCurrentDir();
      currentDir.setExecutable(false);

      // Upload checkpoint when NN has a bad storage dir. This should
      // succeed and create the checkpoint in the good dir.
      secondary.doCheckpoint();
      
      GenericTestUtils.assertExists(
          new File(sd1.getCurrentDir(), NNStorage.getImageFileName(2)));
      
      // Restore the good dir
      currentDir.setExecutable(true);
      nn.restoreFailedStorage("true");
      nn.rollEditLog();

      // Checkpoint again -- this should upload to both dirs
      secondary.doCheckpoint();
      
      assertNNHasCheckpoints(cluster, ImmutableList.of(8));
      assertParallelFilesInvariant(cluster, ImmutableList.of(secondary));
    } finally {
      if (currentDir != null) {
        currentDir.setExecutable(true);
      }
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test case where the NN is configured with a name-only and an edits-only
   * dir, with storage-restore turned on. In this case, if the name-only dir
   * disappears and comes back, a new checkpoint after it has been restored
   * should function correctly.
   * @throws Exception
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCheckpointWithSeparateDirsAfterNameFails() throws Exception {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    File currentDir = null;
    
    Configuration conf = new HdfsConfiguration();

    File base_dir = new File(MiniDFSCluster.getBaseDirectory());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, true);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/name-only");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/edits-only");
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        fileAsURI(new File(base_dir, "namesecondary1")).toString());

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true)
          .manageNameDfsDirs(false)
          .build();
  
      secondary = startSecondaryNameNode(conf);

      // Checkpoint once
      secondary.doCheckpoint();

      // Now primary NN experiences failure of its only name dir -- fake by
      // setting its current dir to a-x permissions
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      NNStorage storage = cluster.getNameNode().getFSImage().getStorage();
      StorageDirectory sd0 = storage.getStorageDir(0);
      assertEquals(NameNodeDirType.IMAGE, sd0.getStorageDirType());
      currentDir = sd0.getCurrentDir();
      currentDir.setExecutable(false);

      // Try to upload checkpoint -- this should fail since there are no
      // valid storage dirs
      try {
        secondary.doCheckpoint();
        fail("Did not fail to checkpoint when there are no valid storage dirs");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains(
            "No targets in destination storage", ioe);
      }
      
      // Restore the good dir
      currentDir.setExecutable(true);
      nn.restoreFailedStorage("true");
      nn.rollEditLog();

      // Checkpoint again -- this should upload to the restored name dir
      secondary.doCheckpoint();
      
      assertNNHasCheckpoints(cluster, ImmutableList.of(8));
      assertParallelFilesInvariant(cluster, ImmutableList.of(secondary));
    } finally {
      if (currentDir != null) {
        currentDir.setExecutable(true);
      }
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test that the 2NN triggers a checkpoint after the configurable interval
   */
  @Test(timeout=30000)
  public void testCheckpointTriggerOnTxnCount() throws Exception {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    Configuration conf = new HdfsConfiguration();

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 10);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 1);
    
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
      FileSystem fs = cluster.getFileSystem();
      secondary = startSecondaryNameNode(conf);
      secondary.startCheckpointThread();
      final NNStorage storage = secondary.getFSImage().getStorage();

      // 2NN should checkpoint at startup
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          LOG.info("Waiting for checkpoint txn id to go to 2");
          return storage.getMostRecentCheckpointTxId() == 2;
        }
      }, 200, 15000);

      // If we make 10 transactions, it should checkpoint again
      for (int i = 0; i < 10; i++) {
        fs.mkdirs(new Path("/test" + i));
      }
      
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          LOG.info("Waiting for checkpoint txn id to go > 2");
          return storage.getMostRecentCheckpointTxId() > 2;
        }
      }, 200, 15000);
    } finally {
      cleanup(secondary);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  /**
   * Test case where the secondary does a checkpoint, then stops for a while.
   * In the meantime, the NN saves its image several times, so that the
   * logs that connect the 2NN's old checkpoint to the current txid
   * get archived. Then, the 2NN tries to checkpoint again.
   */
  @Test
  public void testSecondaryHasVeryOutOfDateImage() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .format(true).build();
  
      secondary = startSecondaryNameNode(conf);

      // Checkpoint once
      secondary.doCheckpoint();

      // Now primary NN saves namespace 3 times
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      nn.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      for (int i = 0; i < 3; i++) {
        nn.saveNamespace();
      }
      nn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
      
      // Now the secondary tries to checkpoint again with its
      // old image in memory.
      secondary.doCheckpoint();
      
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Regression test for HDFS-3678 "Edit log files are never being purged from 2NN"
   */
  @Test
  public void testSecondaryPurgesEditLogs() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 0);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
      
      FileSystem fs = cluster.getFileSystem();
      fs.mkdirs(new Path("/foo"));
  
      secondary = startSecondaryNameNode(conf);
      
      // Checkpoint a few times. Doing this will cause a log roll, and thus
      // several edit log segments on the 2NN.
      for (int i = 0; i < 5; i++) {
        secondary.doCheckpoint();
      }
      
      // Make sure there are no more edit log files than there should be.
      List<File> checkpointDirs = getCheckpointCurrentDirs(secondary);
      for (File checkpointDir : checkpointDirs) {
        List<EditLogFile> editsFiles = FileJournalManager.matchEditLogs(
            checkpointDir);
        assertEquals("Edit log files were not purged from 2NN", 1,
            editsFiles.size());
      }
      
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Regression test for HDFS-3835 - "Long-lived 2NN cannot perform a
   * checkpoint if security is enabled and the NN restarts without outstanding
   * delegation tokens"
   */
  @Test
  public void testSecondaryNameNodeWithDelegationTokens() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .format(true).build();
      
      assertNotNull(cluster.getNamesystem().getDelegationToken(new Text("atm")));
  
      secondary = startSecondaryNameNode(conf);

      // Checkpoint once, so the 2NN loads the DT into its in-memory sate.
      secondary.doCheckpoint();
      
      // Perform a saveNamespace, so that the NN has a new fsimage, and the 2NN
      // therefore needs to download a new fsimage the next time it performs a
      // checkpoint.
      cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      cluster.getNameNodeRpc().saveNamespace();
      cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
      
      // Ensure that the 2NN can still perform a checkpoint.
      secondary.doCheckpoint();
    } finally {
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Regression test for HDFS-3849.  This makes sure that when we re-load the
   * FSImage in the 2NN, we clear the existing leases.
   */
  @Test
  public void testSecondaryNameNodeWithSavedLeases() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    FSDataOutputStream fos = null;
    Configuration conf = new HdfsConfiguration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .format(true).build();
      FileSystem fs = cluster.getFileSystem();
      fos = fs.create(new Path("tmpfile"));
      fos.write(new byte[] { 0, 1, 2, 3 });
      fos.hflush();
      assertEquals(1, cluster.getNamesystem().getLeaseManager().countLease());

      secondary = startSecondaryNameNode(conf);
      assertEquals(0, secondary.getFSNamesystem().getLeaseManager().countLease());

      // Checkpoint once, so the 2NN loads the lease into its in-memory sate.
      secondary.doCheckpoint();
      assertEquals(1, secondary.getFSNamesystem().getLeaseManager().countLease());
      fos.close();
      fos = null;

      // Perform a saveNamespace, so that the NN has a new fsimage, and the 2NN
      // therefore needs to download a new fsimage the next time it performs a
      // checkpoint.
      cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      cluster.getNameNodeRpc().saveNamespace();
      cluster.getNameNodeRpc().setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
      
      // Ensure that the 2NN can still perform a checkpoint.
      secondary.doCheckpoint();
      
      // And the leases have been cleared...
      assertEquals(0, secondary.getFSNamesystem().getLeaseManager().countLease());
    } finally {
      if (fos != null) {
        fos.close();
      }
      if (secondary != null) {
        secondary.shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testCommandLineParsing() throws ParseException {
    SecondaryNameNode.CommandLineOpts opts =
      new SecondaryNameNode.CommandLineOpts();
    opts.parse();
    assertNull(opts.getCommand());

    opts.parse("-checkpoint");
    assertEquals(SecondaryNameNode.CommandLineOpts.Command.CHECKPOINT,
        opts.getCommand());
    assertFalse(opts.shouldForceCheckpoint());

    opts.parse("-checkpoint", "force");
    assertEquals(SecondaryNameNode.CommandLineOpts.Command.CHECKPOINT,
        opts.getCommand());
    assertTrue(opts.shouldForceCheckpoint());

    opts.parse("-geteditsize");
    assertEquals(SecondaryNameNode.CommandLineOpts.Command.GETEDITSIZE,
        opts.getCommand());
    
    opts.parse("-format");
    assertTrue(opts.shouldFormat());
    
    try {
      opts.parse("-geteditsize", "-checkpoint");
      fail("Should have failed bad parsing for two actions");
    } catch (ParseException e) {}
    
    try {
      opts.parse("-checkpoint", "xx");
      fail("Should have failed for bad checkpoint arg");
    } catch (ParseException e) {}
  }

  private void cleanup(SecondaryNameNode snn) {
    if (snn != null) {
      try {
        snn.shutdown();
      } catch (Exception e) {
        LOG.warn("Could not shut down secondary namenode", e);
      }
    }
  }


  /**
   * Assert that if any two files have the same name across the 2NNs
   * and NN, they should have the same content too.
   */
  private void assertParallelFilesInvariant(MiniDFSCluster cluster,
      ImmutableList<SecondaryNameNode> secondaries) throws Exception {
    List<File> allCurrentDirs = Lists.newArrayList();
    allCurrentDirs.addAll(getNameNodeCurrentDirs(cluster, 0));
    for (SecondaryNameNode snn : secondaries) {
      allCurrentDirs.addAll(getCheckpointCurrentDirs(snn));
    }
    FSImageTestUtil.assertParallelFilesAreIdentical(allCurrentDirs,
        ImmutableSet.of("VERSION"));    
  }
  
  private static List<File> getCheckpointCurrentDirs(SecondaryNameNode secondary) {
    List<File> ret = Lists.newArrayList();
    for (URI u : secondary.getCheckpointDirs()) {
      File checkpointDir = new File(u.getPath());
      ret.add(new File(checkpointDir, "current"));
    }
    return ret;
  }

  private static CheckpointStorage spyOnSecondaryImage(SecondaryNameNode secondary1) {
    CheckpointStorage spy = Mockito.spy((CheckpointStorage)secondary1.getFSImage());;
    secondary1.setFSImage(spy);
    return spy;
  }
  
  /**
   * A utility class to perform a checkpoint in a different thread.
   */
  private static class DoCheckpointThread extends Thread {
    private final SecondaryNameNode snn;
    private volatile Throwable thrown = null;
    
    DoCheckpointThread(SecondaryNameNode snn) {
      this.snn = snn;
    }
    
    @Override
    public void run() {
      try {
        snn.doCheckpoint();
      } catch (Throwable t) {
        thrown = t;
      }
    }
    
    void propagateExceptions() {
      if (thrown != null) {
        throw new RuntimeException(thrown);
      }
    }
  }

}
