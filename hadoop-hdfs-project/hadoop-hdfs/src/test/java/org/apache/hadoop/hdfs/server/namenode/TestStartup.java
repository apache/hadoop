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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption.IMPORT;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Startup and checkpoint tests
 * 
 */
public class TestStartup {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String WILDCARD_HTTP_HOST = "0.0.0.0:";
  private static final Log LOG =
    LogFactory.getLog(TestStartup.class.getName());
  private Configuration config;
  private File hdfsDir=null;
  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  private long editsLength=0, fsimageLength=0;

  @Before
  public void setUp() throws Exception {
    config = new HdfsConfiguration();
    hdfsDir = new File(MiniDFSCluster.getBaseDirectory());

    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    LOG.info("--hdfsdir is " + hdfsDir.getAbsolutePath());
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name")).toString());
    config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        new File(hdfsDir, "data").getPath());
    config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
    config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        fileAsURI(new File(hdfsDir, "secondary")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
	       WILDCARD_HTTP_HOST + "0");
    
    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
  }

  /**
   * clean up
   */
  @After
  public void tearDown() throws Exception {
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '" + hdfsDir + "'");
    }	
  }

  /**
   * Create a number of fsimage checkpoints
   * @param count number of checkpoints to create
   * @throws IOException
   */
  public void createCheckPoint(int count) throws IOException {
    LOG.info("--starting mini cluster");
    // manage dirs parameter set to false 
    MiniDFSCluster cluster = null;
    SecondaryNameNode sn = null;
    
    try {
      cluster = new MiniDFSCluster.Builder(config)
                                  .manageDataDfsDirs(false)
                                  .manageNameDfsDirs(false).build();
      cluster.waitActive();

      LOG.info("--starting Secondary Node");

      // start secondary node
      sn = new SecondaryNameNode(config);
      assertNotNull(sn);

      // Create count new files and checkpoints
      for (int i=0; i<count; i++) {
        // create a file
        FileSystem fileSys = cluster.getFileSystem();
        Path p = new Path("t" + i);
        DFSTestUtil.createFile(fileSys, p, fileSize, fileSize,
            blockSize, (short) 1, seed);
        LOG.info("--file " + p.toString() + " created");
        LOG.info("--doing checkpoint");
        sn.doCheckpoint();  // this shouldn't fail
        LOG.info("--done checkpoint");
      }
    } catch (IOException e) {
      fail(StringUtils.stringifyException(e));
      System.err.println("checkpoint failed");
      throw e;
    }  finally {
      if(sn!=null)
        sn.shutdown();
      if(cluster!=null) 
        cluster.shutdown();
      LOG.info("--cluster shutdown");
    }
  }

  /**
   * Corrupts the MD5 sum of the fsimage.
   * 
   * @param corruptAll
   *          whether to corrupt one or all of the MD5 sums in the configured
   *          namedirs
   * @throws IOException
   */
  private void corruptFSImageMD5(boolean corruptAll) throws IOException {
    List<URI> nameDirs = (List<URI>)FSNamesystem.getNamespaceDirs(config);
    // Corrupt the md5 files in all the namedirs
    for (URI uri: nameDirs) {
      // Directory layout looks like:
      // test/data/dfs/nameN/current/{fsimage,edits,...}
      File nameDir = new File(uri.getPath());
      File dfsDir = nameDir.getParentFile();
      assertEquals(dfsDir.getName(), "dfs"); // make sure we got right dir
      // Set the md5 file to all zeros
      File imageFile = new File(nameDir,
          Storage.STORAGE_DIR_CURRENT + "/"
          + NNStorage.getImageFileName(0));
      MD5FileUtils.saveMD5File(imageFile, new MD5Hash(new byte[16]));
      // Only need to corrupt one if !corruptAll
      if (!corruptAll) {
        break;
      }
    }
  }

  /*
   * corrupt files by removing and recreating the directory
   */
  private void corruptNameNodeFiles() throws IOException {
    // now corrupt/delete the directrory
    List<URI> nameDirs = (List<URI>)FSNamesystem.getNamespaceDirs(config);
    List<URI> nameEditsDirs = FSNamesystem.getNamespaceEditsDirs(config);

    // get name dir and its length, then delete and recreate the directory
    File dir = new File(nameDirs.get(0).getPath()); // has only one
    this.fsimageLength = new File(new File(dir, Storage.STORAGE_DIR_CURRENT), 
        NameNodeFile.IMAGE.getName()).length();

    if(dir.exists() && !(FileUtil.fullyDelete(dir)))
      throw new IOException("Cannot remove directory: " + dir);

    LOG.info("--removed dir "+dir + ";len was ="+ this.fsimageLength);

    if (!dir.mkdirs())
      throw new IOException("Cannot create directory " + dir);

    dir = new File( nameEditsDirs.get(0).getPath()); //has only one

    this.editsLength = new File(new File(dir, Storage.STORAGE_DIR_CURRENT), 
        NameNodeFile.EDITS.getName()).length();

    if(dir.exists() && !(FileUtil.fullyDelete(dir)))
      throw new IOException("Cannot remove directory: " + dir);
    if (!dir.mkdirs())
      throw new IOException("Cannot create directory " + dir);

    LOG.info("--removed dir and recreated "+dir + ";len was ="+ this.editsLength);


  }

  /**
   * start with -importCheckpoint option and verify that the files are in separate directories and of the right length
   * @throws IOException
   */
  private void checkNameNodeFiles() throws IOException{

    // start namenode with import option
    LOG.info("-- about to start DFS cluster");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(config)
                                  .format(false)
                                  .manageDataDfsDirs(false)
                                  .manageNameDfsDirs(false)
                                  .startupOption(IMPORT).build();
      cluster.waitActive();
      LOG.info("--NN started with checkpoint option");
      NameNode nn = cluster.getNameNode();
      assertNotNull(nn);	
      // Verify that image file sizes did not change.
      FSImage image = nn.getFSImage();
      verifyDifferentDirs(image, this.fsimageLength, this.editsLength);
    } finally {
      if(cluster != null)
        cluster.shutdown();
    }
  }

  /**
   * verify that edits log and fsimage are in different directories and of a correct size
   */
  private void verifyDifferentDirs(FSImage img, long expectedImgSize, long expectedEditsSize) {
    StorageDirectory sd =null;
    for (Iterator<StorageDirectory> it = img.getStorage().dirIterator(); it.hasNext();) {
      sd = it.next();

      if(sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        img.getStorage();
        File imf = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, 0);
        LOG.info("--image file " + imf.getAbsolutePath() + "; len = " + imf.length() + "; expected = " + expectedImgSize);
        assertEquals(expectedImgSize, imf.length());	
      } else if(sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        img.getStorage();
        File edf = NNStorage.getStorageFile(sd, NameNodeFile.EDITS, 0);
        LOG.info("-- edits file " + edf.getAbsolutePath() + "; len = " + edf.length()  + "; expected = " + expectedEditsSize);
        assertEquals(expectedEditsSize, edf.length());	
      } else {
        fail("Image/Edits directories are not different");
      }
    }

  }
  /**
   * secnn-6
   * checkpoint for edits and image is the same directory
   * @throws IOException
   */
  @Test
  public void testChkpointStartup2() throws IOException{
    LOG.info("--starting checkpointStartup2 - same directory for checkpoint");
    // different name dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "edits")).toString());
    // same checkpoint dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt")).toString());

    createCheckPoint(1);

    corruptNameNodeFiles();
    checkNameNodeFiles();

  }

  /**
   * seccn-8
   * checkpoint for edits and image are different directories 
   * @throws IOException
   */
  @Test
  public void testChkpointStartup1() throws IOException{
    //setUpConfig();
    LOG.info("--starting testStartup Recovery");
    // different name dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "edits")).toString());
    // same checkpoint dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt_edits")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt")).toString());

    createCheckPoint(1);
    corruptNameNodeFiles();
    checkNameNodeFiles();
  }

  /**
   * secnn-7
   * secondary node copies fsimage and edits into correct separate directories.
   * @throws IOException
   */
  @Test
  public void testSNNStartup() throws IOException{
    //setUpConfig();
    LOG.info("--starting SecondNN startup test");
    // different name dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "name")).toString());
    // same checkpoint dirs
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt_edits")).toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
        fileAsURI(new File(hdfsDir, "chkpt")).toString());

    LOG.info("--starting NN ");
    MiniDFSCluster cluster = null;
    SecondaryNameNode sn = null;
    NameNode nn = null;
    try {
      cluster = new MiniDFSCluster.Builder(config).manageDataDfsDirs(false)
                                                  .manageNameDfsDirs(false)
                                                  .build();
      cluster.waitActive();
      nn = cluster.getNameNode();
      assertNotNull(nn);

      // start secondary node
      LOG.info("--starting SecondNN");
      sn = new SecondaryNameNode(config);
      assertNotNull(sn);

      LOG.info("--doing checkpoint");
      sn.doCheckpoint();  // this shouldn't fail
      LOG.info("--done checkpoint");



      // now verify that image and edits are created in the different directories
      FSImage image = nn.getFSImage();
      StorageDirectory sd = image.getStorage().getStorageDir(0); //only one
      assertEquals(sd.getStorageDirType(), NameNodeDirType.IMAGE_AND_EDITS);
      image.getStorage();
      File imf = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, 0);
      image.getStorage();
      File edf = NNStorage.getStorageFile(sd, NameNodeFile.EDITS, 0);
      LOG.info("--image file " + imf.getAbsolutePath() + "; len = " + imf.length());
      LOG.info("--edits file " + edf.getAbsolutePath() + "; len = " + edf.length());

      FSImage chkpImage = sn.getFSImage();
      verifyDifferentDirs(chkpImage, imf.length(), edf.length());

    } catch (IOException e) {
      fail(StringUtils.stringifyException(e));
      System.err.println("checkpoint failed");
      throw e;
    } finally {
      if(sn!=null)
        sn.shutdown();
      if(cluster!=null)
        cluster.shutdown();
    }
  }
  
  @Test
  public void testCompression() throws IOException {
    LOG.info("Test compressing image.");
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    File base_dir = new File(PathUtils.getTestDir(getClass()), "dfs/");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(base_dir, "name").getPath());
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);

    DFSTestUtil.formatNameNode(conf);

    // create an uncompressed image
    LOG.info("Create an uncompressed fsimage");
    NameNode namenode = new NameNode(conf);
    namenode.getNamesystem().mkdirs("/test",
        new PermissionStatus("hairong", null, FsPermission.getDefault()), true);
    NamenodeProtocols nnRpc = namenode.getRpcServer();
    assertTrue(nnRpc.getFileInfo("/test").isDir());
    nnRpc.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    nnRpc.saveNamespace();
    namenode.stop();
    namenode.join();

    // compress image using default codec
    LOG.info("Read an uncomressed image and store it compressed using default codec.");
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    checkNameSpace(conf);

    // read image compressed using the default and compress it using Gzip codec
    LOG.info("Read a compressed image and store it using a different codec.");
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
        "org.apache.hadoop.io.compress.GzipCodec");
    checkNameSpace(conf);

    // read an image compressed in Gzip and store it uncompressed
    LOG.info("Read an compressed iamge and store it as uncompressed.");
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, false);
    checkNameSpace(conf);

    // read an uncomrpessed image and store it uncompressed
    LOG.info("Read an uncompressed image and store it as uncompressed.");
    checkNameSpace(conf);
  }

  private void checkNameSpace(Configuration conf) throws IOException {
    NameNode namenode = new NameNode(conf);
    NamenodeProtocols nnRpc = namenode.getRpcServer();
    assertTrue(nnRpc.getFileInfo("/test").isDir());
    nnRpc.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
    nnRpc.saveNamespace();
    namenode.stop();
    namenode.join();
  }
  
  @Test
  public void testImageChecksum() throws Exception {
    LOG.info("Test uncompressed image checksum");
    testImageChecksum(false);
    LOG.info("Test compressed image checksum");
    testImageChecksum(true);
  }

  private void testImageChecksum(boolean compress) throws Exception {
    MiniDFSCluster cluster = null;
    if (compress) {
      config.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY, true);
    }

    try {
        LOG.info("\n===========================================\n" +
                 "Starting empty cluster");
        
        cluster = new MiniDFSCluster.Builder(config)
          .numDataNodes(0)
          .format(true)
          .build();
        cluster.waitActive();
        
        FileSystem fs = cluster.getFileSystem();
        fs.mkdirs(new Path("/test"));
        
        LOG.info("Shutting down cluster #1");
        cluster.shutdown();
        cluster = null;

        // Corrupt the md5 files in all the namedirs
        corruptFSImageMD5(true);

        // Attach our own log appender so we can verify output
        final LogVerificationAppender appender = new LogVerificationAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);

        // Try to start a new cluster
        LOG.info("\n===========================================\n" +
        "Starting same cluster after simulated crash");
        try {
          cluster = new MiniDFSCluster.Builder(config)
            .numDataNodes(0)
            .format(false)
            .build();
          fail("Should not have successfully started with corrupt image");
        } catch (IOException ioe) {
          GenericTestUtils.assertExceptionContains(
              "Failed to load an FSImage file!", ioe);
          int md5failures = appender.countExceptionsWithMessage(
              " is corrupt with MD5 checksum of ");
          // Two namedirs, so should have seen two failures
          assertEquals(2, md5failures);
        }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test(timeout=30000)
  public void testCorruptImageFallback() throws IOException {
    // Create two checkpoints
    createCheckPoint(2);
    // Delete a single md5sum
    corruptFSImageMD5(false);
    // Should still be able to start
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .format(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .build();
    try {
      cluster.waitActive();
    } finally {
      cluster.shutdown();
    }
}

  /**
   * This test tests hosts include list contains host names.  After namenode
   * restarts, the still alive datanodes should not have any trouble in getting
   * registrant again.
   */
  @Test
  public void testNNRestart() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    FileSystem localFileSys;
    Path hostsFile;
    Path excludeFile;
    int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(config);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, "build/test/data/work-dir/restartnn");
    hostsFile = new Path(dir, "hosts");
    excludeFile = new Path(dir, "exclude");

    // Setup conf
    config.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    writeConfigFile(localFileSys, excludeFile, null);
    config.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    // write into hosts file
    ArrayList<String>list = new ArrayList<String>();
    byte b[] = {127, 0, 0, 1};
    InetAddress inetAddress = InetAddress.getByAddress(b);
    list.add(inetAddress.getHostName());
    writeConfigFile(localFileSys, hostsFile, list);
    int numDatanodes = 1;
    
    try {
      cluster = new MiniDFSCluster.Builder(config)
      .numDataNodes(numDatanodes).setupHostsFile(true).build();
      cluster.waitActive();
  
      cluster.restartNameNode();
      NamenodeProtocols nn = cluster.getNameNodeRpc();
      assertNotNull(nn);
      assertTrue(cluster.isDataNodeUp());
      
      DatanodeInfo[] info = nn.getDatanodeReport(DatanodeReportType.LIVE);
      for (int i = 0 ; i < 5 && info.length != numDatanodes; i++) {
        Thread.sleep(HEARTBEAT_INTERVAL * 1000);
        info = nn.getDatanodeReport(DatanodeReportType.LIVE);
      }
      assertEquals("Number of live nodes should be "+numDatanodes, numDatanodes, 
          info.length);
      
    } catch (IOException e) {
      fail(StringUtils.stringifyException(e));
      throw e;
    } finally {
      cleanupFile(localFileSys, excludeFile.getParent());
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void writeConfigFile(FileSystem localFileSys, Path name,
      ArrayList<String> nodes) throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);
    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }


  @Test(timeout = 120000)
  public void testXattrConfiguration() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;

    try {
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, -1);
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(0).format(true).build();
      fail("Expected exception with negative xattr size");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Cannot set a negative value for the maximum size of an xattr", e);
    } finally {
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    try {
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, -1);
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(0).format(true).build();
      fail("Expected exception with negative # xattrs per inode");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Cannot set a negative limit on the number of xattrs per inode", e);
    } finally {
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    try {
      // Set up a logger to check log message
      final LogVerificationAppender appender = new LogVerificationAppender();
      final Logger logger = Logger.getRootLogger();
      logger.addAppender(appender);
      int count = appender.countLinesWithMessage(
          "Maximum size of an xattr: 0 (unlimited)");
      assertEquals("Expected no messages about unlimited xattr size", 0, count);

      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, 0);
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(0).format(true).build();

      count = appender.countLinesWithMessage(
          "Maximum size of an xattr: 0 (unlimited)");
      // happens twice because we format then run
      assertEquals("Expected unlimited xattr size", 2, count);
    } finally {
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  /**
   * Verify the following scenario.
   * 1. NN restarts.
   * 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
   * 3. After reregistration completes, DN will send Heartbeat, followed by
   *    Blockreport.
   * 4. NN will mark DatanodeStorageInfo#blockContentsStale to false.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testStorageBlockContentsStaleAfterNNRestart() throws Exception {
    MiniDFSCluster dfsCluster = null;
    try {
      Configuration config = new Configuration();
      dfsCluster = new MiniDFSCluster.Builder(config).numDataNodes(1).build();
      dfsCluster.waitActive();
      dfsCluster.restartNameNode(true);
      BlockManagerTestUtil.checkHeartbeat(
          dfsCluster.getNamesystem().getBlockManager());
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      Integer numStaleStorages = (Integer) (mbs.getAttribute(
          mxbeanNameFsns, "NumStaleStorages"));
      assertEquals(0, numStaleStorages.intValue());
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }

    return;
  }

}
