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

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.DATA_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType.NAME_NODE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;

/**
 * This class defines a number of static helper methods used by the
 * DFS Upgrade unit tests.  By default, a singleton master populated storage
 * directory is created for a Namenode (contains edits, fsimage,
 * version, and time files) and a Datanode (contains version and
 * block files).  The master directories are lazily created.  They are then
 * copied by the createStorageDirs() method to create new storage
 * directories of the appropriate type (Namenode or Datanode).
 */
public class UpgradeUtilities {

  // Root scratch directory on local filesystem 
  private static final File TEST_ROOT_DIR =
                      new File(MiniDFSCluster.getBaseDirectory());
  // The singleton master storage directory for Namenode
  private static final File namenodeStorage = new File(TEST_ROOT_DIR, "namenodeMaster");
  // A checksum of the contents in namenodeStorage directory
  private static long namenodeStorageChecksum;
  // The namespaceId of the namenodeStorage directory
  private static int namenodeStorageNamespaceID;
  // The clusterId of the namenodeStorage directory
  private static String namenodeStorageClusterID;
  // The blockpoolId of the namenodeStorage directory
  private static String namenodeStorageBlockPoolID;
  // The fsscTime of the namenodeStorage directory
  private static long namenodeStorageFsscTime;
  // The singleton master storage directory for Datanode
  private static final File datanodeStorage = new File(TEST_ROOT_DIR, "datanodeMaster");
  // A checksum of the contents in datanodeStorage directory
  private static long datanodeStorageChecksum;
  // A checksum of the contents in blockpool storage directory
  private static long blockPoolStorageChecksum;
  // A checksum of the contents in blockpool finalize storage directory
  private static long blockPoolFinalizedStorageChecksum;
  // A checksum of the contents in blockpool rbw storage directory
  private static long blockPoolRbwStorageChecksum;

  /**
   * Initialize the data structures used by this class.  
   * IMPORTANT NOTE: This method must be called once before calling 
   *                 any other public method on this class.  
   * <p>
   * Creates a singleton master populated storage
   * directory for a Namenode (contains edits, fsimage,
   * version, and time files) and a Datanode (contains version and
   * block files).  This can be a lengthy operation.
   */
  public static void initialize() throws Exception {
    createEmptyDirs(new String[] {TEST_ROOT_DIR.toString()});
    Configuration config = new HdfsConfiguration();
    config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeStorage.toString());
    config.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeStorage.toString());
    config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, datanodeStorage.toString());
    MiniDFSCluster cluster = null;
    String bpid = null;
    try {
      // format data-node
      createEmptyDirs(new String[] {datanodeStorage.toString()});
      
      // format and start NameNode and start DataNode
      DFSTestUtil.formatNameNode(config);
      cluster =  new MiniDFSCluster.Builder(config)
                                   .numDataNodes(1)
                                   .startupOption(StartupOption.REGULAR)
                                   .format(false)
                                   .manageDataDfsDirs(false)
                                   .manageNameDfsDirs(false)
                                   .build();
        
      NamenodeProtocols namenode = cluster.getNameNodeRpc();
      namenodeStorageNamespaceID = namenode.versionRequest().getNamespaceID();
      namenodeStorageFsscTime = namenode.versionRequest().getCTime();
      namenodeStorageClusterID = namenode.versionRequest().getClusterID();
      namenodeStorageBlockPoolID = namenode.versionRequest().getBlockPoolID();
      
      FileSystem fs = FileSystem.get(config);
      Path baseDir = new Path("/TestUpgrade");
      fs.mkdirs(baseDir);
      
      // write some files
      int bufferSize = 4096;
      byte[] buffer = new byte[bufferSize];
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
      writeFile(fs, new Path(baseDir, "file1"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file2"), buffer, bufferSize);
      
      // save image
      namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      namenode.saveNamespace(0, 0);
      namenode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
      
      // write more files
      writeFile(fs, new Path(baseDir, "file3"), buffer, bufferSize);
      writeFile(fs, new Path(baseDir, "file4"), buffer, bufferSize);
      bpid = cluster.getNamesystem(0).getBlockPoolId();
    } finally {
      // shutdown
      if (cluster != null) cluster.shutdown();
      FileUtil.fullyDelete(new File(namenodeStorage,"in_use.lock"));
      FileUtil.fullyDelete(new File(datanodeStorage,"in_use.lock"));
    }
    namenodeStorageChecksum = checksumContents(NAME_NODE, 
        new File(namenodeStorage, "current"), false);
    File dnCurDir = new File(datanodeStorage, "current");
    datanodeStorageChecksum = checksumContents(DATA_NODE, dnCurDir, false);
    
    File bpCurDir = new File(BlockPoolSliceStorage.getBpRoot(bpid, dnCurDir),
        "current");
    blockPoolStorageChecksum = checksumContents(DATA_NODE, bpCurDir, false);
    
    File bpCurFinalizeDir = new File(BlockPoolSliceStorage.getBpRoot(bpid, dnCurDir),
        "current/"+DataStorage.STORAGE_DIR_FINALIZED);
    blockPoolFinalizedStorageChecksum = checksumContents(DATA_NODE,
        bpCurFinalizeDir, true);
    
    File bpCurRbwDir = new File(BlockPoolSliceStorage.getBpRoot(bpid, dnCurDir),
        "current/"+DataStorage.STORAGE_DIR_RBW);
    blockPoolRbwStorageChecksum = checksumContents(DATA_NODE, bpCurRbwDir,
        false);
  }
  
  // Private helper method that writes a file to the given file system.
  private static void writeFile(FileSystem fs, Path path, byte[] buffer,
                                int bufferSize) throws IOException 
  {
    OutputStream out;
    out = fs.create(path, true, bufferSize, (short) 1, 1024);
    out.write(buffer, 0, bufferSize);
    out.close();
  }
  
  /**
   * Initialize {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} and 
   * {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} with the specified 
   * number of directory entries. Also initialize dfs.blockreport.intervalMsec.
   */
  public static Configuration initializeStorageStateConf(int numDirs,
                                                         Configuration conf) {
    StringBuffer nameNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "name1").toString());
    StringBuffer dataNodeDirs =
      new StringBuffer(new File(TEST_ROOT_DIR, "data1").toString());
    for (int i = 2; i <= numDirs; i++) {
      nameNodeDirs.append("," + new File(TEST_ROOT_DIR, "name"+i));
      dataNodeDirs.append("," + new File(TEST_ROOT_DIR, "data"+i));
    }
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameNodeDirs.toString());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameNodeDirs.toString());
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataNodeDirs.toString());
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000);
    return conf;
  }
  
  /**
   * Create empty directories.  If a specified directory already exists
   * then it is first removed.
   */
  public static void createEmptyDirs(String[] dirs) throws IOException {
    for (String d : dirs) {
      File dir = new File(d);
      if (dir.exists()) {
        FileUtil.fullyDelete(dir);
      }
      dir.mkdirs();
    }
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * for namenode
   */
  public static long checksumMasterNameNodeContents() {
    return namenodeStorageChecksum;
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * for datanode
   */
  public static long checksumMasterDataNodeContents() {
    return datanodeStorageChecksum;
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * for block pool.
   */
  public static long checksumMasterBlockPoolContents() {
    return blockPoolStorageChecksum;
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * for finalized dir under block pool.
   */
  public static long checksumMasterBlockPoolFinalizedContents() {
    return blockPoolFinalizedStorageChecksum;
  }
  
  /**
   * Return the checksum for the singleton master storage directory
   * for rbw dir under block pool.
   */
  public static long checksumMasterBlockPoolRbwContents() {
    return blockPoolRbwStorageChecksum;
  }
  
  /**
   * Compute the checksum of all the files in the specified directory.
   * This method provides an easy way to ensure equality between the contents
   * of two directories.
   *
   * @param nodeType if DATA_NODE then any file named "VERSION" is ignored.
   *    This is because this file file is changed every time
   *    the Datanode is started.
   * @param dir must be a directory
   * @param recursive whether or not to consider subdirectories
   *
   * @throws IllegalArgumentException if specified directory is not a directory
   * @throws IOException if an IOException occurs while reading the files
   * @return the computed checksum value
   */
  public static long checksumContents(NodeType nodeType, File dir,
      boolean recursive) throws IOException {
    CRC32 checksum = new CRC32();
    checksumContentsHelper(nodeType, dir, checksum, recursive);
    return checksum.getValue();
  }

  public static void checksumContentsHelper(NodeType nodeType, File dir,
      CRC32 checksum, boolean recursive) throws IOException {
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(
          "Given argument is not a directory:" + dir);
    }
    File[] list = dir.listFiles();
    Arrays.sort(list);
    for (int i = 0; i < list.length; i++) {
      if (!list[i].isFile()) {
        if (recursive) {
          checksumContentsHelper(nodeType, list[i], checksum, recursive);
        }
        continue;
      }

      // skip VERSION and dfsUsed and replicas file for DataNodes
      if (nodeType == DATA_NODE &&
          (list[i].getName().equals("VERSION") ||
              list[i].getName().equals("dfsUsed") ||
              list[i].getName().equals("replicas"))) {
        continue;
      }

      FileInputStream fis = null;
      try {
        fis = new FileInputStream(list[i]);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          checksum.update(buffer, 0, bytesRead);
        }
      } finally {
        if(fis != null) {
          fis.close();
        }
      }
    }
  }
  
  /**
   * Simulate the {@link DFSConfigKeys#DFS_NAMENODE_NAME_DIR_KEY} of a populated 
   * DFS filesystem.
   * This method populates for each parent directory, <code>parent/dirName</code>
   * with the content of namenode storage directory that comes from a singleton
   * namenode master (that contains edits, fsimage, version and time files). 
   * If the destination directory does not exist, it will be created.  
   * If the directory already exists, it will first be deleted.
   *
   * @param parents parent directory where {@code dirName} is created
   * @param dirName directory under which storage directory is created
   * @return the array of created directories
   */
  public static File[] createNameNodeStorageDirs(String[] parents,
      String dirName) throws Exception {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i], dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      LocalFileSystem localFS = FileSystem.getLocal(new HdfsConfiguration());
      localFS.copyToLocalFile(new Path(namenodeStorage.toString(), "current"),
                              new Path(newDir.toString()),
                              false);
      retVal[i] = newDir;
    }
    return retVal;
  }  
  
  /**
   * Simulate the {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} of a 
   * populated DFS filesystem.
   * This method populates for each parent directory, <code>parent/dirName</code>
   * with the content of datanode storage directory that comes from a singleton
   * datanode master (that contains version and block files). If the destination
   * directory does not exist, it will be created.  If the directory already 
   * exists, it will first be deleted.
   * 
   * @param parents parent directory where {@code dirName} is created
   * @param dirName directory under which storage directory is created
   * @return the array of created directories
   */
  public static File[] createDataNodeStorageDirs(String[] parents,
      String dirName) throws Exception {
    File[] retVal = new File[parents.length];
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i], dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      LocalFileSystem localFS = FileSystem.getLocal(new HdfsConfiguration());
      localFS.copyToLocalFile(new Path(datanodeStorage.toString(), "current"),
                              new Path(newDir.toString()),
                              false);
      // Change the storage UUID to avoid conflicts when DN starts up.
      StorageDirectory sd = new StorageDirectory(
          new File(datanodeStorage.toString()));
      sd.setStorageUuid(DatanodeStorage.generateUuid());
      Properties properties = Storage.readPropertiesFile(sd.getVersionFile());
      properties.setProperty("storageID", sd.getStorageUuid());
      Storage.writeProperties(sd.getVersionFile(), properties);

      retVal[i] = newDir;
    }
    return retVal;
  }
  
  /**
   * Simulate the {@link DFSConfigKeys#DFS_DATANODE_DATA_DIR_KEY} of a 
   * populated DFS filesystem.
   * This method populates for each parent directory, <code>parent/dirName</code>
   * with the content of block pool storage directory that comes from a singleton
   * datanode master (that contains version and block files). If the destination
   * directory does not exist, it will be created.  If the directory already 
   * exists, it will first be deleted.
   * 
   * @param parents parent directory where {@code dirName} is created
   * @param dirName directory under which storage directory is created
   * @param bpid block pool id for which the storage directory is created.
   * @return the array of created directories
   */
  public static File[] createBlockPoolStorageDirs(String[] parents,
      String dirName, String bpid) throws Exception {
    File[] retVal = new File[parents.length];
    Path bpCurDir = new Path(MiniDFSCluster.getBPDir(datanodeStorage,
        bpid, Storage.STORAGE_DIR_CURRENT));
    for (int i = 0; i < parents.length; i++) {
      File newDir = new File(parents[i] + "/current/" + bpid, dirName);
      createEmptyDirs(new String[] {newDir.toString()});
      LocalFileSystem localFS = FileSystem.getLocal(new HdfsConfiguration());
      localFS.copyToLocalFile(bpCurDir,
                              new Path(newDir.toString()),
                              false);
      retVal[i] = newDir;
    }
    return retVal;
  }
  
  /**
   * Create a <code>version</code> file for namenode inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. None of the parameters may be null.
   *
   * @param parent directory where namenode VERSION file is stored
   * @param version StorageInfo to create VERSION file from
   * @param bpid Block pool Id
   *
   * @return the created version file
   */
  public static File[] createNameNodeVersionFile(Configuration conf,
      File[] parent, StorageInfo version, String bpid) throws IOException {
    Storage storage = new NNStorage(conf, 
                              Collections.<URI>emptyList(), 
                              Collections.<URI>emptyList());
    storage.setStorageInfo(version);
    File[] versionFiles = new File[parent.length];
    for (int i = 0; i < parent.length; i++) {
      versionFiles[i] = new File(parent[i], "VERSION");
      StorageDirectory sd = new StorageDirectory(parent[i].getParentFile());
      storage.writeProperties(versionFiles[i], sd);
    }
    return versionFiles;
  }
  
  /**
   * Create a <code>version</code> file for datanode inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. None of the parameters may be null.
   *
   * @param parent directory where namenode VERSION file is stored
   * @param version StorageInfo to create VERSION file from
   * @param bpid Block pool Id
   */
  public static void createDataNodeVersionFile(File[] parent,
      StorageInfo version, String bpid) throws IOException {
    createDataNodeVersionFile(parent, version, bpid, bpid);
  }
  
  /**
   * Create a <code>version</code> file for datanode inside the specified parent
   * directory.  If such a file already exists, it will be overwritten.
   * The given version string will be written to the file as the layout
   * version. None of the parameters may be null.
   *
   * @param parent directory where namenode VERSION file is stored
   * @param version StorageInfo to create VERSION file from
   * @param bpid Block pool Id
   * @param bpidToWrite Block pool Id to write into the version file
   */
  public static void createDataNodeVersionFile(File[] parent,
      StorageInfo version, String bpid, String bpidToWrite) throws IOException {
    DataStorage storage = new DataStorage(version);
    storage.setDatanodeUuid("FixedDatanodeUuid");

    File[] versionFiles = new File[parent.length];
    for (int i = 0; i < parent.length; i++) {
      File versionFile = new File(parent[i], "VERSION");
      StorageDirectory sd = new StorageDirectory(parent[i].getParentFile());
      DataStorage.createStorageID(sd, false);
      storage.writeProperties(versionFile, sd);
      versionFiles[i] = versionFile;
      File bpDir = BlockPoolSliceStorage.getBpRoot(bpid, parent[i]);
      createBlockPoolVersionFile(bpDir, version, bpidToWrite);
    }
  }
  
  public static void createBlockPoolVersionFile(File bpDir,
      StorageInfo version, String bpid) throws IOException {
    // Create block pool version files
    if (DataNodeLayoutVersion.supports(
        LayoutVersion.Feature.FEDERATION, version.layoutVersion)) {
      File bpCurDir = new File(bpDir, Storage.STORAGE_DIR_CURRENT);
      BlockPoolSliceStorage bpStorage = new BlockPoolSliceStorage(version,
          bpid);
      File versionFile = new File(bpCurDir, "VERSION");
      StorageDirectory sd = new StorageDirectory(bpDir);
      bpStorage.writeProperties(versionFile, sd);
    }
  }
  
  /**
   * Corrupt the specified file.  Some random bytes within the file
   * will be changed to some random values.
   *
   * @throws IllegalArgumentException if the given file is not a file
   * @throws IOException if an IOException occurs while reading or writing the file
   */
  public static void corruptFile(File file,
      byte[] stringToCorrupt,
      byte[] replacement) throws IOException {
    Preconditions.checkArgument(replacement.length == stringToCorrupt.length);
    if (!file.isFile()) {
      throw new IllegalArgumentException(
          "Given argument is not a file:" + file);
    }
    byte[] data = Files.toByteArray(file);
    int index = Bytes.indexOf(data, stringToCorrupt);
    if (index == -1) {
      throw new IOException(
          "File " + file + " does not contain string " +
          new String(stringToCorrupt));
    }

    for (int i = 0; i < stringToCorrupt.length; i++) {
      data[index + i] = replacement[i];
    }
    Files.write(data, file);
  }
  
  /**
   * Return the layout version inherent in the current version
   * of the Namenode, whether it is running or not.
   */
  public static int getCurrentNameNodeLayoutVersion() {
    return HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
  }
  
  /**
   * Return the namespace ID inherent in the currently running
   * Namenode.  If no Namenode is running, return the namespace ID of
   * the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static int getCurrentNamespaceID(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNodeRpc().versionRequest().getNamespaceID();
    }
    return namenodeStorageNamespaceID;
  }
  
  /**
   * Return the cluster ID inherent in the currently running
   * Namenode. 
   */
  public static String getCurrentClusterID(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNodeRpc().versionRequest().getClusterID();
    }
    return namenodeStorageClusterID;
  }
  
  /**
   * Return the blockpool ID inherent in the currently running
   * Namenode. 
   */
  public static String getCurrentBlockPoolID(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNodeRpc().versionRequest().getBlockPoolID();
    }
    return namenodeStorageBlockPoolID;
  }
  
  /**
   * Return the File System State Creation Timestamp (FSSCTime) inherent
   * in the currently running Namenode.  If no Namenode is running,
   * return the FSSCTime of the master Namenode storage directory.
   *
   * The UpgradeUtilities.initialize() method must be called once before
   * calling this method.
   */
  public static long getCurrentFsscTime(MiniDFSCluster cluster) throws IOException {
    if (cluster != null) {
      return cluster.getNameNodeRpc().versionRequest().getCTime();
    }
    return namenodeStorageFsscTime;
  }

  /**
   * Create empty block pool directories
   * @return array of block pool directories
   */
  public static String[] createEmptyBPDirs(String[] baseDirs, String bpid)
      throws IOException {
    String[] bpDirs = new String[baseDirs.length];
    for (int i = 0; i < baseDirs.length; i++) {
      bpDirs[i] = MiniDFSCluster.getBPDir(new File(baseDirs[i]), bpid);
    }
    createEmptyDirs(bpDirs);
    return bpDirs;
  }
}

