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
import junit.framework.TestCase;
import java.net.InetSocketAddress;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil.ErrorSimulator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode.CheckpointStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestCheckpoint extends TestCase {

  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
  }

  static final Log LOG = LogFactory.getLog(TestCheckpoint.class); 
  
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  static final int numDatanodes = 3;
  short replication = 3;
  
  @Override
  public void setUp() throws IOException {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  static void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[TestCheckpoint.fileSize];
    Random rand = new Random(TestCheckpoint.seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
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

  /**
   * put back the old namedir
   */
  private void resurrectNameDir(File namedir) 
    throws IOException {
    String parentdir = namedir.getParent();
    String name = namedir.getName();
    File oldname =  new File(parentdir, name + ".old");
    if (!oldname.renameTo(namedir)) {
      assertTrue(false);
    }
  }

  /**
   * remove one namedir
   */
  private void removeOneNameDir(File namedir) 
    throws IOException {
    String parentdir = namedir.getParent();
    String name = namedir.getName();
    File newname =  new File(parentdir, name + ".old");
    if (!namedir.renameTo(newname)) {
      assertTrue(false);
    }
  }

  /*
   * Verify that namenode does not startup if one namedir is bad.
   */
  private void testNamedirError(Configuration conf, Collection<URI> namedirs) 
    throws IOException {
    System.out.println("Starting testNamedirError");
    MiniDFSCluster cluster = null;

    if (namedirs.size() <= 1) {
      return;
    }
    
    //
    // Remove one namedir & Restart cluster. This should fail.
    //
    File first = new File(namedirs.iterator().next().getPath());
    removeOneNameDir(first);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).format(false).build();
      cluster.shutdown();
      assertTrue(false);
    } catch (Throwable t) {
      // no nothing
    }
    resurrectNameDir(first); // put back namedir
  }

  /*
   * Simulate namenode crashing after rolling edit log.
   */
  @SuppressWarnings("deprecation")
  private void testSecondaryNamenodeError1(Configuration conf)
    throws IOException {
    System.out.println("Starting testSecondaryNamenodeError 1");
    Path file1 = new Path("checkpointxx.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(false).build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edits log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      ErrorSimulator.setErrorSimulation(0);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      ErrorSimulator.clearErrorSimulation(0);
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
    System.out.println("Starting testSecondaryNamenodeError 2");
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
  @SuppressWarnings("deprecation")
  private void testSecondaryNamenodeError2(Configuration conf)
    throws IOException {
    System.out.println("Starting testSecondaryNamenodeError 21");
    Path file1 = new Path("checkpointyy.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(false).build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after uploading the new fsimage.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      ErrorSimulator.setErrorSimulation(1);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      ErrorSimulator.clearErrorSimulation(1);
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
    System.out.println("Starting testSecondaryNamenodeError 22");
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
  @SuppressWarnings("deprecation")
  private void testSecondaryNamenodeError3(Configuration conf)
    throws IOException {
    System.out.println("Starting testSecondaryNamenodeError 31");
    Path file1 = new Path("checkpointzz.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(false).build();

    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      ErrorSimulator.setErrorSimulation(0);

      try {
        secondary.doCheckpoint();  // this should fail
        assertTrue(false);
      } catch (IOException e) {
      }
      ErrorSimulator.clearErrorSimulation(0);
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
    System.out.println("Starting testSecondaryNamenodeError 32");
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
  @SuppressWarnings("deprecation")
  void testSecondaryFailsToReturnImage(Configuration conf)
    throws IOException {
    System.out.println("Starting testSecondaryFailsToReturnImage");
    Path file1 = new Path("checkpointRI.dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(false).build();
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
      ErrorSimulator.setErrorSimulation(2);

      try {
        secondary.doCheckpoint();  // this should fail
        fail("Checkpoint succeeded even though we injected an error!");
      } catch (IOException e) {
        System.out.println("testSecondaryFailsToReturnImage: doCheckpoint() " +
            "failed predictably - " + e);
      }
      ErrorSimulator.clearErrorSimulation(2);

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

  /**
   * Simulate 2NN failing to send the whole file (error type 3)
   * and secondary sending a corrupt file (error type 4). The
   * length and digest headers in the HTTP transfer should prevent
   * these from corrupting the NN.
   */
  void testNameNodeImageSendFail(Configuration conf)
    throws IOException {
    System.out.println("Starting testNameNodeImageSendFail");
    doSendFailTest(conf, 3, "is not of the advertised size");
    doSendFailTest(conf, 4, "does not match advertised digest");
  }
  
  /**
   * Run a test where the 2NN runs into some kind of error when
   * sending the checkpoint back to the NN.
   * @param errorType the ErrorSimulator type to trigger
   * @param exceptionSubstring an expected substring of the triggered exception
   */
  @SuppressWarnings("deprecation")
  private void doSendFailTest(Configuration conf,
      int errorType, String exceptionSubstring)
      throws IOException {
    
    Path file1 = new Path("checkpoint-doSendFailTest-" + errorType + ".dat");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(numDatanodes)
                                               .format(false).build();
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    try {
      assertTrue(!fileSys.exists(file1));
      //
      // Make the checkpoint fail after rolling the edit log.
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      ErrorSimulator.setErrorSimulation(errorType);

      try {
        secondary.doCheckpoint();  // this should fail
        fail("Did not get expected exception");
      } catch (IOException e) {
        // We only sent part of the image. Have to trigger this exception
        assertTrue(e.getMessage().contains(exceptionSubstring));
      }
      ErrorSimulator.clearErrorSimulation(errorType);
      secondary.shutdown(); // secondary namenode crash!

      // start new instance of secondary and verify that 
      // a new rollEditLog suceedes inspite of the fact that 
      // edits.new already exists. TODO update this comment!
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
   * Test different startup scenarios.
   * <p><ol>
   * <li> Start of primary name-node in secondary directory must succeed. 
   * <li> Start of secondary node when the primary is already running in 
   *      this directory must fail.
   * <li> Start of primary name-node if secondary node is already running in 
   *      this directory must fail.
   * <li> Start of two secondary nodes in the same directory must fail.
   * <li> Import of a checkpoint must fail if primary 
   * directory contains a valid image.
   * <li> Import of the secondary image directory must succeed if primary 
   * directory does not exist.
   * <li> Recover failed checkpoint for secondary node.
   * <li> Complete failed checkpoint for secondary node.
   * </ol>
   */
  @SuppressWarnings("deprecation")
  void testStartup(Configuration conf) throws IOException {
    System.out.println("Startup of the name-node in the checkpoint directory.");
    String primaryDirs = conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    String primaryEditsDirs = conf.get(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
    String checkpointDirs = conf.get(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    String checkpointEditsDirs = conf.get(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    NameNode nn = startNameNode(conf, checkpointDirs, checkpointEditsDirs,
                                 StartupOption.REGULAR);

    // Starting secondary node in the same directory as the primary
    System.out.println("Startup of secondary in the same dir as the primary.");
    SecondaryNameNode secondary = null;
    try {
      secondary = startSecondaryNameNode(conf);
      assertFalse(secondary.getFSImage().getStorage().isLockSupported(0));
      secondary.shutdown();
    } catch (IOException e) { // expected to fail
      assertTrue(secondary == null);
    }
    nn.stop(); nn = null;

    // Starting primary node in the same directory as the secondary
    System.out.println("Startup of primary in the same dir as the secondary.");
    // secondary won't start without primary
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.REGULAR);
    boolean succeed = false;
    do {
      try {
        secondary = startSecondaryNameNode(conf);
        succeed = true;
      } catch(IOException ie) { // keep trying
        System.out.println("Try again: " + ie.getLocalizedMessage());
      }
    } while(!succeed);
    nn.stop(); nn = null;
    try {
      nn = startNameNode(conf, checkpointDirs, checkpointEditsDirs,
                          StartupOption.REGULAR);
      assertFalse(nn.getFSImage().getStorage().isLockSupported(0));
      nn.stop(); nn = null;
    } catch (IOException e) { // expected to fail
      assertTrue(nn == null);
    }

    // Try another secondary in the same directory
    System.out.println("Startup of two secondaries in the same dir.");
    // secondary won't start without primary
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.REGULAR);
    SecondaryNameNode secondary2 = null;
    try {
      secondary2 = startSecondaryNameNode(conf);
      assertFalse(secondary2.getFSImage().getStorage().isLockSupported(0));
      secondary2.shutdown();
    } catch (IOException e) { // expected to fail
      assertTrue(secondary2 == null);
    }
    nn.stop(); nn = null;
    secondary.shutdown();

    // Import a checkpoint with existing primary image.
    System.out.println("Import a checkpoint with existing primary image.");
    try {
      nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                          StartupOption.IMPORT);
      fail("Importing a checkpoint with existing primary image did not fail");
    } catch (IOException e) { // expected to fail
      assertNull(nn);
    }
    
    // Remove current image and import a checkpoint.
    System.out.println("Import a checkpoint with existing primary image.");
    List<URI> nameDirs = (List<URI>)FSNamesystem.getNamespaceDirs(conf);
    List<URI> nameEditsDirs = (List<URI>)FSNamesystem.
                                  getNamespaceEditsDirs(conf);
    File newestImageFile = FSImageTestUtil.findNewestImageFile(
        nameDirs.get(0).getPath() + "/current");
    long fsimageLength = newestImageFile.length();
    assertTrue(fsimageLength > 0);
    
    for(URI uri : nameDirs) {
      File dir = new File(uri.getPath());
      if(dir.exists())
        if(!(FileUtil.fullyDelete(dir)))
          throw new IOException("Cannot remove directory: " + dir);
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }

    for(URI uri : nameEditsDirs) {
      File dir = new File(uri.getPath());
      if(dir.exists())
        if(!(FileUtil.fullyDelete(dir)))
          throw new IOException("Cannot remove directory: " + dir);
      if (!dir.mkdirs())
        throw new IOException("Cannot create directory " + dir);
    }
    
    nn = startNameNode(conf, primaryDirs, primaryEditsDirs,
                        StartupOption.IMPORT);
    // Verify that image file sizes did not change.
    FSImage image = nn.getFSImage();
    for (StorageDirectory sd : image.getStorage().dirIterable(NameNodeDirType.IMAGE)) {
      // TODO it would make more sense if the newest had the same txid, but
      // on import, it actually re-saves with a 1-higher txid
      File recreatedImageFile = FSImageTestUtil.findNewestImageFile(
          sd.getCurrentDir().toString());
      assertEquals(fsimageLength, recreatedImageFile.length());
    }
    nn.stop();

    // TODO: need tests that make sure that partially completed checkpoints
    // don't leave an fsimage_ckpt file around, and that the 2NN cleans up
    // any such files at startup
    
    // Check that everything starts ok now.
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes).format(false).build();
    cluster.waitActive();
    cluster.shutdown();
  }

  NameNode startNameNode( Configuration conf,
                          String imageDirs,
                          String editsDirs,
                          StartupOption start) throws IOException {
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");  
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, imageDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, editsDirs);
    String[] args = new String[]{start.getName()};
    NameNode nn = NameNode.createNameNode(args, conf);
    assertTrue(nn.isInSafeMode());
    return nn;
  }

  // This deprecation suppress warning does not work due to known Java bug:
  // http://bugs.sun.com/view_bug.do?bug_id=6460147
  @SuppressWarnings("deprecation")
  SecondaryNameNode startSecondaryNameNode(Configuration conf
                                          ) throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    return new SecondaryNameNode(conf);
  }
  
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
  public void testCheckpoint() throws IOException {
    Path file1 = new Path("checkpoint.dat");
    Path file2 = new Path("checkpoint2.dat");
    Collection<URI> namedirs = null;

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
      namedirs = cluster.getNameDirs(0);

      //
      // Create file1
      //
      writeFile(fileSys, file1, replication);
      checkFile(fileSys, file1, replication);

      //
      // Take a checkpoint
      //
      SecondaryNameNode secondary = startSecondaryNameNode(conf);
      ErrorSimulator.initializeErrorSimulationEvent(5);
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

    // file2 is left behind.

    testNameNodeImageSendFail(conf);
    testSecondaryNamenodeError1(conf);
    testSecondaryNamenodeError2(conf);
    testSecondaryNamenodeError3(conf);
    testNamedirError(conf, namedirs);
    testSecondaryFailsToReturnImage(conf);
    testStartup(conf);
  }

  /**
   * Tests save namepsace.
   */
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
        assertTrue(new File(ed, "current/edits_inprogress_1").length() > Integer.SIZE/Byte.SIZE);
      }

      // Saving image in safe mode should succeed
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        admin.run(args);
      } catch(Exception e) {
        throw new IOException(e);
      }
      
      // the following steps should have happened:
      //   edits_inprogress_1 -> edits_1-8  (finalized)
      //   fsimage_8 created
      //   edits_inprogress_9 created
      //
      for(URI uri : editsDirs) {
        File ed = new File(uri.getPath());
        File curDir = new File(ed, "current");
        System.err.println("Files in " + curDir + ":\n  " +
            Joiner.on("\n  ").join(curDir.list()));
        // Verify that the first edits file got finalized
        File originalEdits = new File(curDir, "edits_inprogress_1");
        assertFalse(originalEdits.exists());
        File finalizedEdits = new File(curDir, "edits_1-8");
        assertTrue(finalizedEdits.exists());
        assertTrue(finalizedEdits.length() > Integer.SIZE/Byte.SIZE);

        assertTrue(new File(ed, "current/edits_inprogress_9").exists());
      }
      
      Collection<URI> imageDirs = cluster.getNameDirs(0);
      for (URI uri : imageDirs) {
        File imageDir = new File(uri.getPath());
        File savedImage = new File(imageDir, "current/fsimage_8");
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
  public void testCheckpointSignature() throws IOException {

    MiniDFSCluster cluster = null;
    Configuration conf = new HdfsConfiguration();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
        .format(true).build();
    NameNode nn = cluster.getNameNode();

    SecondaryNameNode secondary = startSecondaryNameNode(conf);
    // prepare checkpoint image
    secondary.doCheckpoint();
    CheckpointSignature sig = nn.rollEditLog();
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
  @SuppressWarnings("deprecation")
  public void testCheckpointAfterTwoFailedUploads() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    
    Configuration conf = new HdfsConfiguration();

    ErrorSimulator.initializeErrorSimulationEvent(5);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
          .format(true).build();
  
      secondary = startSecondaryNameNode(conf);

      ErrorSimulator.setErrorSimulation(1);
      
      // Fail to checkpoint once
      try {
        secondary.doCheckpoint();
        fail("Should have failed upload");
      } catch (IOException ioe) {
        LOG.info("Got expected failure", ioe);
        assertTrue(ioe.toString().contains("Simulating error1"));
      }

      // Fail to checkpoint again
      try {
        secondary.doCheckpoint();
        fail("Should have failed upload");
      } catch (IOException ioe) {
        LOG.info("Got expected failure", ioe);
        assertTrue(ioe.toString().contains("Simulating error1"));
      } finally {
        ErrorSimulator.clearErrorSimulation(1);
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
  @SuppressWarnings("deprecation")
  public void testMultipleSecondaryNamenodes() throws IOException {
    Configuration conf = new HdfsConfiguration();
    String nameserviceId1 = "ns1";
    String nameserviceId2 = "ns2";
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMESERVICES, nameserviceId1
        + "," + nameserviceId2);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numNameNodes(2)
        .nameNodePort(9928).build();
    Configuration snConf1 = new HdfsConfiguration(cluster.getConfiguration(0));
    Configuration snConf2 = new HdfsConfiguration(cluster.getConfiguration(1));
    InetSocketAddress nn1RpcAddress = cluster.getNameNode(0).rpcAddress;
    InetSocketAddress nn2RpcAddress = cluster.getNameNode(1).rpcAddress;
    String nn1 = nn1RpcAddress.getHostName() + ":" + nn1RpcAddress.getPort();
    String nn2 = nn2RpcAddress.getHostName() + ":" + nn2RpcAddress.getPort();

    // Set the Service Rpc address to empty to make sure the node specific
    // setting works
    snConf1.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "");
    snConf2.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "");

    // Set the nameserviceIds
    snConf1.set(DFSUtil.getNameServiceIdKey(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nameserviceId1), nn1);
    snConf2.set(DFSUtil.getNameServiceIdKey(
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
  @SuppressWarnings("deprecation")
  public void testSecondaryImageDownload()
    throws IOException {
    Configuration conf = new HdfsConfiguration();
    System.out.println("Starting testSecondaryImageDownload");
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
          "fsimage_" + expectedTxIdToDownload);
      File secondaryFsImageAfter = new File(secondaryCurrent,
          "fsimage_" + (expectedTxIdToDownload + 2));
      
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
  @SuppressWarnings("deprecation")
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
        .saveFSImageInAllDirs(Mockito.anyLong());

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
  @SuppressWarnings("deprecation")
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
      
      // Letting the first node continue should catch an exception
      checkpointThread.join();
      try {
        checkpointThread.propagateExceptions();
        fail("Didn't throw!");
      } catch (Exception ioe) {
        assertTrue("Unexpected exception: " +
            StringUtils.stringifyException(ioe),
            ioe.toString().contains("Another checkpointer already uploaded"));
        LOG.info("Caught expected exception", ioe);
      }
      
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
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
  public void testNamespaceVerifiedOnFileTransfer() throws IOException {
    MiniDFSCluster cluster = null;
    
    Configuration conf = new HdfsConfiguration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
          .format(true).build();
      
      NameNode nn = cluster.getNameNode();
      String fsName = NameNode.getHostPortString(nn.getHttpAddress());


      // Make a finalized log on the server side. 
      nn.rollEditLog();
      RemoteEditLogManifest manifest = nn.getEditLogManifest(0);
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
      NameNode nn = cluster.getNameNode();
      NNStorage storage = nn.getFSImage().getStorage();
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
      NameNode nn = cluster.getNameNode();
      NNStorage storage = nn.getFSImage().getStorage();
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
            "Unable to download to any storage dir", ioe);
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

  @SuppressWarnings("deprecation")
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
   * Assert that the NameNode has checkpoints at the expected
   * transaction IDs.
   */
  private void assertNNHasCheckpoints(MiniDFSCluster cluster,
      List<Integer> txids) {

    for (File nameDir : getNameNodeCurrentDirs(cluster)) {
      // Should have fsimage_N for the three checkpoints
      for (long checkpointTxId : txids) {
        File image = new File(nameDir, "fsimage_" + checkpointTxId);
        assertTrue("Expected non-empty " + image, image.length() > 0);
      }
    }
  }

  /**
   * Assert that if any two files have the same name across the 2NNs
   * and NN, they should have the same content too.
   */
  @SuppressWarnings("deprecation")
  private void assertParallelFilesInvariant(MiniDFSCluster cluster,
      ImmutableList<SecondaryNameNode> secondaries) throws Exception {
    List<File> allCurrentDirs = Lists.newArrayList();
    allCurrentDirs.addAll(getNameNodeCurrentDirs(cluster));
    for (SecondaryNameNode snn : secondaries) {
      allCurrentDirs.addAll(getCheckpointCurrentDirs(snn));
    }
    FSImageTestUtil.assertParallelFilesAreIdentical(allCurrentDirs,
        ImmutableSet.of("VERSION"));    
  }
  
  private List<File> getNameNodeCurrentDirs(MiniDFSCluster cluster) {
    List<File> nameDirs = Lists.newArrayList();
    for (URI u : cluster.getNameDirs(0)) {
      nameDirs.add(new File(u.getPath(), "current"));
    }
    return nameDirs;
  }

  @SuppressWarnings("deprecation")
  private List<File> getCheckpointCurrentDirs(SecondaryNameNode secondary) {
    List<File> ret = Lists.newArrayList();
    for (URI u : secondary.getCheckpointDirs()) {
      File checkpointDir = new File(u.getPath());
      ret.add(new File(checkpointDir, "current"));
    }
    return ret;
  }

  @SuppressWarnings("deprecation")
  private CheckpointStorage spyOnSecondaryImage(SecondaryNameNode secondary1) {
    CheckpointStorage spy = Mockito.spy((CheckpointStorage)secondary1.getFSImage());;
    secondary1.setFSImage(spy);
    return spy;
  }
  
  /**
   * A utility class to perform a checkpoint in a different thread.
   */
  @SuppressWarnings("deprecation")
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
