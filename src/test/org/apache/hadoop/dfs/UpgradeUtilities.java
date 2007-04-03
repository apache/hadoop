/*
 * UpgradeUtilities.java
 *
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

package org.apache.hadoop.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.FSConstants.NodeType;
import static org.apache.hadoop.dfs.FSConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.dfs.FSConstants.NodeType.DATA_NODE;
import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.dfs.Storage.StorageDirectory;

/**
 * This class defines a number of static helper methods used by the
 * DFS Upgrade unit tests.  By default, a singleton master populated storage
 * directory is created for a Namenode (contains edits, fsimage,
 * version, and time files) and a Datanode (contains version and
 * block files).  The master directories are lazily created.  They are then
 * copied by the createStorageDirs() method to create new storage
 * directories of the appropriate type (Namenode or Datanode).
 * 
 * @author Nigel Daley
 */
public class UpgradeUtilities {

  // The fs.default.name configuration host:port value for the Namenode
  private static final String NAMENODE_HOST = "localhost:0";
  // Root scratch directory on local filesystem 
  private static File TEST_ROOT_DIR = new File(
    System.getProperty("test.build.data","/tmp").toString().replace(' ', '+'));
  // The singleton master storage directory for Namenode
  private static File namenodeStorage = new File(TEST_ROOT_DIR, "namenodeMaster");
  // A checksum of the contents in namenodeStorage directory
  private static long namenodeStorageChecksum;
  // The namespaceId of the namenodeStorage directory
  private static int namenodeStorageNamespaceID;
  // The fsscTime of the namenodeStorage directory
  private static long namenodeStorageFsscTime;
  // The singleton master storage directory for Datanode
  private static File datanodeStorage = new File(TEST_ROOT_DIR, "datanodeMaster");
  // A checksum of the contents in datanodeStorage directory
  private static long datanodeStorageChecksum;
  // The NameNode started by this Utility class
  private static NameNode namenode = null;
  // The DataNode started by this Utility class
  private static DataNode datanode = null;
  
  /**
   * Initialize the data structures used by this class.  
   * IMPORTANT NOTE: This method must be called once before calling 
   *                 any other public method on this class.  
   */
  public static void initialize() throws Exception {
    createEmptyDirs(new String[] {TEST_ROOT_DIR.toString()});
    initializeStorage();
  }
  
  /**
   * Initialize dfs.name.dir and dfs.data.dir with the specified number of
   * directory entries. Also initialize fs.default.name and 
   * dfs.blockreport.intervalMsec.
   */
  public static Configuration initializeStorageStateConf(int numDirs) {
    StringBuffer nameNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "name1").toString());
    StringBuffer dataNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "data1").toString());
    for (int i = 2; i <= numDirs; i++) {
      nameNodeDirs.append("," + new File(TEST_ROOT_DIR, "name"+i));
      dataNodeDirs.append("," + new File(TEST_ROOT_DIR, "data"+i));
    }
    Configuration conf = new Configuration();
    conf.set("dfs.name.dir", nameNodeDirs.toString());
    conf.set("dfs.data.dir", dataNodeDirs.toString());
    conf.set("dfs.blockreport.intervalMsec", 10000);
    return conf;
  }
  
  /**
   * Starts the given type of node or all nodes.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   *
   * @param nodeType
   *    The type of node to start.  If NAME_NODE, then one
   *    Namenode is started.  If DATA_NODE, then one Datanode
   *    is started.
   * @param operation
   *    The operation with which to startup the given type
   *    node. FORMAT and null are treated as a REGULAR startup. If nodeType
   *    if DATA_NODE, then UPGRADE is also treated as REGULAR.
   * @param conf
   *    The configuration to be used in starting the node.
   *
   * @throw IllegalStateException
   *    If this method is called to start a
   *    node that is already running.
   */
  public static void startCluster(NodeType nodeType, StartupOption operation, Configuration conf) throws Exception {
    if (isNodeRunning(nodeType)) {
      throw new IllegalStateException("Attempting to start "
        + nodeType + " but it is already running");
    }
    if (nodeType == DATA_NODE && operation == StartupOption.UPGRADE) {
      operation = StartupOption.REGULAR;
    }
    String[] args = (operation == null ||
      operation == StartupOption.FORMAT ||
      operation == StartupOption.REGULAR) ?
        new String[] {} : new String[] {"-"+operation.toString()};
    switch (nodeType) {
      case NAME_NODE:
        // Set up the right ports for the datanodes
        conf.set("fs.default.name",NAMENODE_HOST);
        namenode = NameNode.createNameNode(args, conf);
        break;
      case DATA_NODE:
        if (namenode == null) {
          throw new IllegalStateException("Attempting to start DATA_NODE "
            + "but NAME_NODE is not running");
        }
        // Set up the right ports for the datanodes
        InetSocketAddress nnAddr = namenode.getNameNodeAddress(); 
        conf.set("fs.default.name", nnAddr.getHostName()+ ":" + nnAddr.getPort());
        conf.setInt("dfs.info.port", 0);
        conf.setInt("dfs.datanode.info.port", 0);
        datanode = DataNode.createDataNode(args, conf);
        break;
    }
  }
  
  /**
   * Stops the given type of node or all nodes.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   *
   * @param nodeType
   *    The type of node to stop if it is running. If null, then both
   *    Namenode and Datanodes are stopped if they are running.
   */
  public static void stopCluster(NodeType nodeType) {
    if (nodeType == NAME_NODE || nodeType == null) {
      if (namenode != null) {
        namenode.stop();
      }
      namenode = null;
    }
    if (nodeType == DATA_NODE || nodeType == null) {
      if (datanode != null) {
        datanode.shutdown(); 
      }
      DataNode.shutdownAll();
      datanode = null;
    }
  }
  
  /**
   * If the Namenode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throw IllegalStateException if the Namenode is not running.
   */
  public static void finalizeCluster(Configuration conf) throws Exception {
    if (! isNodeRunning(NAME_NODE)) {
      throw new IllegalStateException("Attempting to finalize "
        + "Namenode but it is not running");
    }
    new DFSAdmin().doMain(conf, new String[] {"-finalizeUpgrade"});
  }
  
  /**
   * Determines if the given node type is currently running.
   * If the node type is DATA_NODE, then all started Datanodes
   * must be running in-order for this method to return
   * <code>true</code>.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static boolean isNodeRunning(NodeType nodeType) {
    switch( nodeType ) {
      case NAME_NODE:
        return namenode != null;
      case DATA_NODE:
        return datanode != null;
      default:
        assert false : "Invalid node type: " + nodeType;
    }
    return false;
  }
  
  /**
   * Format the given directories.  This is equivalent to the Namenode
   * formatting the given directories.  If a given directory already exists,
   * it is first deleted; otherwise if it does not exist, it is first created.
   *
   * @throw IOException if unable to format one of the given dirs
   */
  public static void format(File... dirs) throws IOException {
    String imageDirs = "";
    for (int i = 0; i < dirs.length; i++) {
      if( i == 0 )
        imageDirs = dirs[i].getCanonicalPath();
      else
        imageDirs += "," + dirs[i].getCanonicalPath();
    }
    Configuration conf = new Configuration();
    conf.set("dfs.name.dir", imageDirs);
    NameNode.format(conf);
  }
  
  /**
   * Create empty directories.  If a specified directory already exists
   * then it is first removed.
   */
  public static void createEmptyDirs(String[] dirs) {
    for (String d : dirs) {
      File dir = new File(d);
      if (dir.exists()) {
        remove(dir);
      }
      dir.mkdirs();
    }
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * of the given node type.
   */
  public static long checksumMasterContents(NodeType nodeType) throws IOException {
    if (nodeType == NAME_NODE) {
      return namenodeStorageChecksum;
    } else {
      return datanodeStorageChecksum;
    }
  }
  
  /**
   * Compute the checksum of all the files in the specified directory.
   * The contents of subdirectories are not included. This method provides
   * an easy way to ensure equality between the contents of two directories.
   *
   * @param nodeType if DATA_NODE then any file named "VERSION" is ignored.
   *    This is because this file file is changed every time
   *    the Datanode is started.
   * @param dir must be a directory. Subdirectories are ignored.
   *
   * @throw IllegalArgumentException if specified directory is not a directory
   * @throw IOException if an IOException occurs while reading the files
   * @return the computed checksum value
   */
  public static long checksumContents(NodeType nodeType, File dir) throws IOException {
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(
        "Given argument is not a directory:" + dir);
    }
    File[] list = dir.listFiles();
    CRC32 checksum = new CRC32();
    for (int i = 0; i < list.length; i++) {
      if (list[i].isFile()) {
        // skip VERSION file for DataNodes
        if (nodeType == DATA_NODE &&
          list[i].getName().equals("VERSION")) 
        {
          continue; 
        }
        FileInputStream fis = new FileInputStream(list[i]);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          checksum.update(buffer,0,bytesRead);
        }
        fis.close();
      }
    }
    return checksum.getValue();
  }
  
  /**
   * Simulate the <code>dfs.name.dir</code> or <code>dfs.data.dir</code>
   * of a populated DFS filesystem.
   *
   * This method creates and populates the directory specified by
   *  <code>parent/dirName</code>, for each parent directory.
   * The contents of the new directories will be
   * appropriate for the given node type.  If the directory does not
   * exist, it will be created.  If the directory already exists, it
   * will first be deleted.
   *
   * By default, a singleton master populated storage
   * directory is created for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  These directories are then
   * copied by this method to create new storage
   * directories of the appropriate type (Namenode or Datanode).
   *
   * @return the array of created directories
   */
  public static File[] createStorageDirs(NodeType nodeType, String[] parents, String dirName) throws Exception {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i], dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      populateDir(nodeType, newDir);
      retVal[i] = newDir;
    }
    return retVal;
  }
  
  /**
   * Create a <code>version</code> file inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. If null, then the current layout version will be used.
   * The parent and nodeType parameters must not be null.
   *
   * @param version
   *
   * @return the created version file
   */
  public static File[] createVersionFile(NodeType nodeType, File[] parent,
    StorageInfo version) throws IOException 
  {
    if (version == null)
      version = getCurrentNamespaceInfo();
    Storage storage = null;
    File[] versionFiles = new File[parent.length];
    for (int i = 0; i < parent.length; i++) {
      File versionFile = new File(parent[i], "VERSION");
      remove(versionFile);
      switch (nodeType) {
        case NAME_NODE:
          System.out.println("HERE");
          storage = new FSImage( version );
          break;
        case DATA_NODE:
                  System.out.println("HERE2");
          storage = new DataStorage( version, "doNotCare" );
          break;
      }
      StorageDirectory sd = storage.new StorageDirectory(parent[i].getParentFile());
      sd.write(versionFile);
      versionFiles[i] = versionFile;
    }
    return versionFiles;
  }
  
  /**
   * Remove the specified file.  If the given file is a directory,
   * then the directory and all its contents will be removed.
   */
  public static boolean remove(File file) {
    try {
      boolean retVal = FileUtil.fullyDelete(file);
      return retVal;
    } catch (IOException ioe) {
      // this should never happen
      throw new IllegalStateException(
        "WHAT? FileUtil.fullyDelete threw and IOException?",ioe);
    }
  }
  
  /**
   * Corrupt the specified file.  Some random bytes within the file
   * will be changed to some random values.
   *
   * @throw IllegalArgumentException if the given file is not a file
   * @throw IOException if an IOException occurs while reading or writing the file
   */
  public static void corruptFile(File file) throws IOException {
    if (!file.isFile()) {
      throw new IllegalArgumentException(
        "Given argument is not a file:" + file);
    }
    RandomAccessFile raf = new RandomAccessFile(file,"rws");
    Random random = new Random();
    for (long i = 0; i < raf.length(); i++) {
      raf.seek(i);
      if (random.nextBoolean()) {
        raf.writeByte(random.nextInt());
      }
    }
    raf.close();
  }
  
  /**
   * Retrieve the current NamespaceInfo object from a running Namenode.
   */
  public static NamespaceInfo getCurrentNamespaceInfo() throws IOException {
    if (isNodeRunning(NAME_NODE))
      return namenode.versionRequest();
    return null;
  }
  
  /**
   * Return the layout version inherent in the current version
   * of the Namenode, whether it is running or not.
   */
  public static int getCurrentLayoutVersion() {
    return FSConstants.LAYOUT_VERSION;
  }
  
  /**
   * Return the namespace ID inherent in the currently running
   * Namenode.  If no Namenode is running, return the namespace ID of
   * the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static int getCurrentNamespaceID() throws IOException {
    if (isNodeRunning(NAME_NODE)) {
      return namenode.versionRequest().getNamespaceID();
    }
    return namenodeStorageNamespaceID;
  }
  
  /**
   * Return the File System State Creation Timestamp (FSSCTime) inherent
   * in the currently running Namenode.  If no Namenode is running,
   * return the FSSCTime of the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static long getCurrentFsscTime() throws IOException {
    if (isNodeRunning(NAME_NODE)) {
      return namenode.versionRequest().getCTime();
    }
    return namenodeStorageFsscTime;
  }
  
  /**********************************************************************
   ********************* PRIVATE METHODS ********************************
   *********************************************************************/
  
  /**
   * Populates the given directory with valid version, edits, and fsimage
   * files.  The version file will contain the current layout version.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   *
   * @throw IllegalArgumentException if dir does not already exist
   */
  private static void populateDir(NodeType nodeType, File dir) throws Exception {
    if (!dir.exists()) {
      throw new IllegalArgumentException(
        "Given argument is not an existing directory:" + dir);
    }
    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
    switch (nodeType) {
      case NAME_NODE:
        localFS.copyToLocalFile(
          new Path(namenodeStorage.toString(), "current"),
          new Path(dir.toString()),
          false);
        break;
      case DATA_NODE:
        localFS.copyToLocalFile(
          new Path(datanodeStorage.toString(), "current"),
          new Path(dir.toString()),
          false);
        break;
    }
  }
  
  static void writeFile(FileSystem fs,
    Path path,
    byte[] buffer,
    int bufferSize ) throws IOException {
    OutputStream out;
    out = fs.create(path, true, bufferSize, (short) 1, 1024);
    out.write( buffer, 0, bufferSize );
    out.close();
  }
  
  /**
   * Creates a singleton master populated storage
   * directory for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  This can be a lengthy operation.
   *
   * @param conf must not be null.  These properties will be set:
   *    fs.default.name
   *    dfs.name.dir
   *    dfs.data.dir
   */
  private static void initializeStorage() throws Exception {
    Configuration config = new Configuration();
    config.set("fs.default.name",NAMENODE_HOST);
    config.set("dfs.name.dir", namenodeStorage.toString());
    config.set("dfs.data.dir", datanodeStorage.toString());

    try {
      // format data-node
      createEmptyDirs(new String[] {datanodeStorage.toString()});

      // format name-node
      NameNode.format(config);
      
      // start name-node
      startCluster(NAME_NODE, null, config);
      namenodeStorageNamespaceID = namenode.versionRequest().getNamespaceID();
      namenodeStorageFsscTime = namenode.versionRequest().getCTime();
      
      // start data-node
      startCluster(DATA_NODE, null, config);
      
      FileSystem fs = FileSystem.get(config);
      Path baseDir = new Path("/TestUpgrade");
      fs.mkdirs( baseDir );
      
      // write some files
      int bufferSize = 4096;
      byte[] buffer = new byte[bufferSize];
      for( int i=0; i < bufferSize; i++ )
        buffer[i] = (byte)('0' + i % 50);
      writeFile(fs, new Path(baseDir, "file1"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file2"), buffer, bufferSize);
      
      // save image
      namenode.getFSImage().saveFSImage();
      namenode.getFSImage().getEditLog().open();
      
      // write more files
      writeFile(fs, new Path(baseDir, "file3"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file4"), buffer, bufferSize);
    } finally {
      // shutdown
      stopCluster(null);
      remove(new File(namenodeStorage,"in_use.lock"));
      remove(new File(datanodeStorage,"in_use.lock"));
    }
    namenodeStorageChecksum = checksumContents(
      NAME_NODE, new File(namenodeStorage,"current"));
    datanodeStorageChecksum = checksumContents(
      DATA_NODE, new File(datanodeStorage,"current"));
  }

}

