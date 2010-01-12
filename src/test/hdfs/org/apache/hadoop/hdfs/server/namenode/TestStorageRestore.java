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


import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cli.CmdFactoryDFS;
import org.apache.hadoop.cli.util.CLITestData;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.hdfs.DFSConfigKeys;


/**
 * Startup and checkpoint tests
 * 
 */
public class TestStorageRestore extends TestCase {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private static final Log LOG =
    LogFactory.getLog(TestStorageRestore.class.getName());
  private Configuration config;
  private File hdfsDir=null;
  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  private File path1, path2, path3;
  private MiniDFSCluster cluster;

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt("io.file.buffer.size", 4096),
        (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
 
  protected void setUp() throws Exception {
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
   * clean up
   */
  public void tearDown() throws Exception {
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '" + hdfsDir + "'");
    } 
  }
  
  /**
   * invalidate storage by removing storage directories
   */
  public void invalidateStorage(FSImage fi) throws IOException {
    ArrayList<StorageDirectory> al = new ArrayList<StorageDirectory>(2);
    Iterator<StorageDirectory> it = fi.dirIterator();
    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      if(sd.getRoot().equals(path2) || sd.getRoot().equals(path3)) {
        al.add(sd);
      }
    }
    // simulate an error
    fi.processIOError(al, true);
  }
  
  /**
   * test
   */
  public void printStorages(FSImage fs) {
    LOG.info("current storages and corresoponding sizes:");
    for(Iterator<StorageDirectory> it = fs.dirIterator(); it.hasNext(); ) {
      StorageDirectory sd = it.next();
      
      if(sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        File imf = FSImage.getImageFile(sd, NameNodeFile.IMAGE);
        LOG.info("  image file " + imf.getAbsolutePath() + "; len = " + imf.length());  
      }
      if(sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        File edf = FSImage.getImageFile(sd, NameNodeFile.EDITS);
        LOG.info("  edits file " + edf.getAbsolutePath() + "; len = " + edf.length()); 
      }
    }
  }
  
  
  /**
   * This function returns a md5 hash of a file.
   * 
   * @param file input file
   * @return The md5 string
   */
  public String getFileMD5(File file) throws Exception {
    String res = new String();
    MessageDigest mD = MessageDigest.getInstance("MD5");
    DataInputStream dis = new DataInputStream(new FileInputStream(file));

    try {
      while(true) {
        mD.update(dis.readByte());
      }
    } catch (EOFException eof) {}

    BigInteger bigInt = new BigInteger(1, mD.digest());
    res = bigInt.toString(16);
    dis.close();

    return res;
  }

  
  /**
   * read currentCheckpointTime directly from the file
   * @param currDir
   * @return the checkpoint time
   * @throws IOException
   */
  long readCheckpointTime(File currDir) throws IOException {
    File timeFile = new File(currDir, NameNodeFile.TIME.getName()); 
    long timeStamp = 0L;
    if (timeFile.exists() && timeFile.canRead()) {
      DataInputStream in = new DataInputStream(new FileInputStream(timeFile));
      try {
        timeStamp = in.readLong();
      } finally {
        in.close();
      }
    }
    return timeStamp;
  }
  
  /**
   *  check if files exist/not exist
   * @throws IOException 
   */
  public void checkFiles(boolean valid) throws IOException {
    //look at the valid storage
    File fsImg1 = new File(path1, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.IMAGE.getName());
    File fsImg2 = new File(path2, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.IMAGE.getName());
    File fsImg3 = new File(path3, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.IMAGE.getName());

    File fsEdits1 = new File(path1, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS.getName());
    File fsEdits2 = new File(path2, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS.getName());
    File fsEdits3 = new File(path3, Storage.STORAGE_DIR_CURRENT + "/" + NameNodeFile.EDITS.getName());
    
    long chkPt1 = readCheckpointTime(new File(path1, Storage.STORAGE_DIR_CURRENT));
    long chkPt2 = readCheckpointTime(new File(path2, Storage.STORAGE_DIR_CURRENT));
    long chkPt3 = readCheckpointTime(new File(path3, Storage.STORAGE_DIR_CURRENT));
    
    String md5_1 = null,md5_2 = null,md5_3 = null;
    try {
      md5_1 = getFileMD5(fsEdits1);
      md5_2 = getFileMD5(fsEdits2);
      md5_3 = getFileMD5(fsEdits3);
    } catch (Exception e) {
      System.err.println("md 5 calculation failed:" + e.getLocalizedMessage());
    }
    this.printStorages(cluster.getNameNode().getFSImage());
    
    LOG.info("++++ image files = "+fsImg1.getAbsolutePath() + "," + fsImg2.getAbsolutePath() + ","+ fsImg3.getAbsolutePath());
    LOG.info("++++ edits files = "+fsEdits1.getAbsolutePath() + "," + fsEdits2.getAbsolutePath() + ","+ fsEdits3.getAbsolutePath());
    LOG.info("checkFiles compares lengths: img1=" + fsImg1.length()  + ",img2=" + fsImg2.length()  + ",img3=" + fsImg3.length());
    LOG.info("checkFiles compares lengths: edits1=" + fsEdits1.length()  + ",edits2=" + fsEdits2.length()  + ",edits3=" + fsEdits3.length());
    LOG.info("checkFiles compares chkPts: name1=" + chkPt1  + ",name2=" + chkPt2  + ",name3=" + chkPt3);
    LOG.info("checkFiles compares md5s: " + fsEdits1.getAbsolutePath() + 
        "="+ md5_1  + "," + fsEdits2.getAbsolutePath() + "=" + md5_2  + "," +
        fsEdits3.getAbsolutePath() + "=" + md5_3);  
    
    if(valid) {
      // should be the same
      assertTrue(fsImg1.length() == fsImg2.length());
      assertTrue(0 == fsImg3.length()); //shouldn't be created
      assertTrue(fsEdits1.length() == fsEdits2.length());
      assertTrue(fsEdits1.length() == fsEdits3.length());
      assertTrue(md5_1.equals(md5_2));
      assertTrue(md5_1.equals(md5_3));
      
      // checkpoint times
      assertTrue(chkPt1 == chkPt2);
      assertTrue(chkPt1 == chkPt3);
    } else {
      // should be different
      //assertTrue(fsImg1.length() != fsImg2.length());
      //assertTrue(fsImg1.length() != fsImg3.length());
      assertTrue("edits1 = edits2", fsEdits1.length() != fsEdits2.length());
      assertTrue("edits1 = edits3", fsEdits1.length() != fsEdits3.length());
      
      assertTrue(!md5_1.equals(md5_2));
      assertTrue(!md5_1.equals(md5_3));
      
      
   // checkpoint times
      assertTrue(chkPt1 > chkPt2);
      assertTrue(chkPt1 > chkPt3);
    }
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
  @SuppressWarnings("deprecation")
  public void testStorageRestore() throws Exception {
    int numDatanodes = 2;
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, false, true,  null, null, null, null);
    cluster.waitActive();
    
    SecondaryNameNode secondary = new SecondaryNameNode(config);
    System.out.println("****testStorageRestore: Cluster and SNN started");
    printStorages(cluster.getNameNode().getFSImage());
    
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("/", "test");
    writeFile(fs, path, 2);
    
    System.out.println("****testStorageRestore: file test written, invalidating storage...");
  
    invalidateStorage(cluster.getNameNode().getFSImage());
    //secondary.doCheckpoint(); // this will cause storages to be removed.
    printStorages(cluster.getNameNode().getFSImage());
    System.out.println("****testStorageRestore: storage invalidated + doCheckpoint");

    path = new Path("/", "test1");
    writeFile(fs, path, 2);
    System.out.println("****testStorageRestore: file test1 written");
    
    checkFiles(false); // SHOULD BE FALSE
    
    System.out.println("****testStorageRestore: checkfiles(false) run");
    
    secondary.doCheckpoint();  ///should enable storage..
    
    checkFiles(true);
    System.out.println("****testStorageRestore: second Checkpoint done and checkFiles(true) run");
    
    // verify that all the logs are active
    path = new Path("/", "test2");
    writeFile(fs, path, 2);
    System.out.println("****testStorageRestore: wrote a file and checkFiles(true) run");
    checkFiles(true);
    
    secondary.shutdown();
    cluster.shutdown();
  }
  
  /**
   * Test dfsadmin -restoreFailedStorage command
   * @throws Exception
   */
  public void testDfsAdminCmd() throws Exception {
    int numDatanodes = 2;
    
    
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, false, true,  null, null, null, null);
    cluster.waitActive();
    try {

      FSImage fsi = cluster.getNameNode().getFSImage();

      // it is started with dfs.name.dir.restore set to true (in SetUp())
      boolean restore = fsi.getRestoreFailedStorage();
      LOG.info("Restore is " + restore);
      assertEquals(restore, true);

      // now run DFSAdmnin command

      String cmd = "-fs NAMENODE -restoreFailedStorage false";
      String namenode = config.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");
      CommandExecutor executor = 
        CmdFactoryDFS.getCommandExecutor(
          new CLITestData.TestCmd(cmd, CLITestData.TestCmd.CommandType.DFSADMIN),
          namenode);
      executor.executeCommand(cmd);
      restore = fsi.getRestoreFailedStorage();
      assertFalse("After set true call restore is " + restore, restore);

      // run one more time - to set it to true again
      cmd = "-fs NAMENODE -restoreFailedStorage true";
      executor.executeCommand(cmd);
      restore = fsi.getRestoreFailedStorage();
      assertTrue("After set false call restore is " + restore, restore);
      
   // run one more time - no change in value
      cmd = "-fs NAMENODE -restoreFailedStorage check";
      CommandExecutor.Result cmdResult = executor.executeCommand(cmd);
      restore = fsi.getRestoreFailedStorage();
      assertTrue("After check call restore is " + restore, restore);
      String commandOutput = cmdResult.getCommandOutput();
      commandOutput.trim();
      assertTrue(commandOutput.contains("restoreFailedStorage is set to true"));
      

    } finally {
      cluster.shutdown();
    }
  }
}
