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

package org.apache.hadoop.hdfs;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.TestTransferRbw;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha
        .ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Assume;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Utilities for HDFS tests */
public class DFSTestUtil {

  private static final Log LOG = LogFactory.getLog(DFSTestUtil.class);
  
  private static final Random gen = new Random();
  private static final String[] dirNames = {
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
  };
  
  private final int maxLevels;
  private final int maxSize;
  private final int minSize;
  private final int nFiles;
  private MyFile[] files;
  
  /** Creates a new instance of DFSTestUtil
   *
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   * @param minSize Minimum size for file
   */
  private DFSTestUtil(int nFiles, int maxLevels, int maxSize, int minSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
    this.minSize = minSize;
  }

  /** Creates a new instance of DFSTestUtil
   *
   * @param testName Name of the test from where this utility is used
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   * @param minSize Minimum size for file
   */
  public DFSTestUtil(String testName, int nFiles, int maxLevels, int maxSize,
      int minSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
    this.minSize = minSize;
  }
  
  /**
   * when formatting a namenode - we must provide clusterid.
   * @param conf
   * @throws IOException
   */
  public static void formatNameNode(Configuration conf) throws IOException {
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.isEmpty())
      StartupOption.FORMAT.setClusterId("testClusterID");

    NameNode.format(conf);
  }

  /**
   * Create a new HA-enabled configuration.
   */
  public static Configuration newHAConfiguration(final String logicalName) {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, logicalName);
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            logicalName), "nn1,nn2");
    conf.set(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "" +
            "." + logicalName,
            ConfiguredFailoverProxyProvider.class.getName());
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    return conf;
  }

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private class MyFile {
    
    private String name = "";
    private final int size;
    private final long seed;
    
    MyFile() {
      int nLevels = gen.nextInt(maxLevels);
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        name = sb.toString();
      }
      long fidx = -1;
      while (fidx < 0) { fidx = gen.nextLong(); }
      name = name + Long.toString(fidx);
      size = minSize + gen.nextInt(maxSize - minSize);
      seed = gen.nextLong();
    }
    
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  public void createFiles(FileSystem fs, String topdir) throws IOException {
    createFiles(fs, topdir, (short)3);
  }
  
  /** create nFiles with random names and directory hierarchies
   *  with random (but reproducible) data in them.
   */
  public void createFiles(FileSystem fs, String topdir,
                   short replicationFactor) throws IOException {
    files = new MyFile[nFiles];
    
    for (int idx = 0; idx < nFiles; idx++) {
      files[idx] = new MyFile();
    }
    
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      createFile(fs, new Path(root, files[idx].getName()), files[idx].getSize(),
          replicationFactor, files[idx].getSeed());
    }
  }

  public static String readFile(FileSystem fs, Path fileName)
      throws IOException {
    byte buf[] = readFileBuffer(fs, fileName);
	return new String(buf, 0, buf.length);
  }

  public static byte[] readFileBuffer(FileSystem fs, Path fileName) 
      throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      FSDataInputStream in = fs.open(fileName);
      try {
        IOUtils.copyBytes(fs.open(fileName), os, 1024, true);
        return os.toByteArray();
      } finally {
        in.close();
      }
    } finally {
      os.close();
    }
  }
  
  public static void createFile(FileSystem fs, Path fileName, long fileLen, 
      short replFactor, long seed) throws IOException {
    createFile(fs, fileName, 1024, fileLen, fs.getDefaultBlockSize(fileName),
        replFactor, seed);
  }
  
  public static void createFile(FileSystem fs, Path fileName, int bufferLen,
      long fileLen, long blockSize, short replFactor, long seed)
      throws IOException {
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " + 
                            fileName.getParent().toString());
    }
    FSDataOutputStream out = null;
    try {
      out = fs.create(fileName, true, fs.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        replFactor, blockSize);
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(seed);
        long bytesToWrite = fileLen;
        while (bytesToWrite>0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
              : (int) bytesToWrite;
  
          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
  
  public static byte[] calculateFileContentsFromSeed(long seed, int length) {
    Random rb = new Random(seed);
    byte val[] = new byte[length];
    rb.nextBytes(val);
    return val;
  }
  
  /** check if the files have been copied correctly. */
  public boolean checkFiles(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      FSDataInputStream in = fs.open(fPath);
      byte[] toRead = new byte[files[idx].getSize()];
      byte[] toCompare = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toCompare);
      in.readFully(0, toRead);
      in.close();
      for (int i = 0; i < toRead.length; i++) {
        if (toRead[i] != toCompare[i]) {
          return false;
        }
      }
      toRead = null;
      toCompare = null;
    }
    
    return true;
  }

  void setReplication(FileSystem fs, String topdir, short value) 
                                              throws IOException {
    Path root = new Path(topdir);
    for (int idx = 0; idx < nFiles; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      fs.setReplication(fPath, value);
    }
  }

  /*
   * Waits for the replication factor of all files to reach the
   * specified target.
   */
  public void waitReplication(FileSystem fs, String topdir, short value) 
      throws IOException, InterruptedException, TimeoutException {
    Path root = new Path(topdir);

    /** wait for the replication factor to settle down */
    for (int idx = 0; idx < nFiles; idx++) {
      waitReplication(fs, new Path(root, files[idx].getName()), value);
    }
  }

  /*
   * Check if the given block in the given file is corrupt.
   */
  public static boolean allBlockReplicasCorrupt(MiniDFSCluster cluster,
      Path file, int blockNo) throws IOException {
    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), cluster.getConfiguration(0));
    LocatedBlocks blocks;
    try {
       blocks = client.getNamenode().getBlockLocations(
           file.toString(), 0, Long.MAX_VALUE);
    } finally {
      client.close();
    }
    return blocks.get(blockNo).isCorrupt();
  }

  /*
   * Wait up to 20s for the given block to be replicated across
   * the requested number of racks, with the requested number of
   * replicas, and the requested number of replicas still needed.
   */
  public static void waitForReplication(MiniDFSCluster cluster, ExtendedBlock b,
      int racks, int replicas, int neededReplicas)
      throws IOException, TimeoutException, InterruptedException {
    int curRacks = 0;
    int curReplicas = 0;
    int curNeededReplicas = 0;
    int count = 0;
    final int ATTEMPTS = 20;

    do {
      Thread.sleep(1000);
      int[] r = BlockManagerTestUtil.getReplicaInfo(cluster.getNamesystem(),
          b.getLocalBlock());
      curRacks = r[0];
      curReplicas = r[1];
      curNeededReplicas = r[2];
      count++;
    } while ((curRacks != racks ||
              curReplicas != replicas ||
              curNeededReplicas != neededReplicas) && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for replication."
          + " Needed replicas = "+neededReplicas
          + " Cur needed replicas = "+curNeededReplicas
          + " Replicas = "+replicas+" Cur replicas = "+curReplicas
          + " Racks = "+racks+" Cur racks = "+curRacks);
    }
  }

  /**
   * Keep accessing the given file until the namenode reports that the
   * given block in the file contains the given number of corrupt replicas.
   */
  public static void waitCorruptReplicas(FileSystem fs, FSNamesystem ns,
      Path file, ExtendedBlock b, int corruptRepls)
      throws IOException, TimeoutException, InterruptedException {
    int count = 0;
    final int ATTEMPTS = 50;
    int repls = ns.getBlockManager().numCorruptReplicas(b.getLocalBlock());
    while (repls != corruptRepls && count < ATTEMPTS) {
      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(),
            512, true);
      } catch (IOException e) {
        // Swallow exceptions
      }
      System.out.println("Waiting for "+corruptRepls+" corrupt replicas");
      count++;
      // check more often so corrupt block reports are not easily missed
      for (int i = 0; i < 10; i++) {
        repls = ns.getBlockManager().numCorruptReplicas(b.getLocalBlock());
        Thread.sleep(100);
        if (repls == corruptRepls) {
          break;
        }
      }
    }
    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for corrupt replicas."
          + " Waiting for "+corruptRepls+", but only found "+repls);
    }
  }

  /*
   * Wait up to 20s for the given DN (IP:port) to be decommissioned
   */
  public static void waitForDecommission(FileSystem fs, String name) 
      throws IOException, InterruptedException, TimeoutException {
    DatanodeInfo dn = null;
    int count = 0;
    final int ATTEMPTS = 20;

    do {
      Thread.sleep(1000);
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      for (DatanodeInfo info : dfs.getDataNodeStats()) {
        if (name.equals(info.getXferAddr())) {
          dn = info;
        }
      }
      count++;
    } while ((dn == null ||
              dn.isDecommissionInProgress() ||
              !dn.isDecommissioned()) && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for datanode "
          + name + " to decommission.");
    }
  }

  /*
   * Returns the index of the first datanode which has a copy
   * of the given block, or -1 if no such datanode exists.
   */
  public static int firstDnWithBlock(MiniDFSCluster cluster, ExtendedBlock b)
      throws IOException {
    int numDatanodes = cluster.getDataNodes().size();
    for (int i = 0; i < numDatanodes; i++) {
      String blockContent = cluster.readBlockOnDataNode(i, b);
      if (blockContent != null) {
        return i;
      }
    }
    return -1;
  }

  /*
   * Return the total capacity of all live DNs.
   */
  public static long getLiveDatanodeCapacity(DatanodeManager dm) {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    dm.fetchDatanodes(live, null, false);
    long capacity = 0;
    for (final DatanodeDescriptor dn : live) {
      capacity += dn.getCapacity();
    }
    return capacity;
  }

  /*
   * Return the capacity of the given live DN.
   */
  public static long getDatanodeCapacity(DatanodeManager dm, int index) {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    dm.fetchDatanodes(live, null, false);
    return live.get(index).getCapacity();
  }

  /*
   * Wait for the given # live/dead DNs, total capacity, and # vol failures. 
   */
  public static void waitForDatanodeStatus(DatanodeManager dm, int expectedLive, 
      int expectedDead, long expectedVolFails, long expectedTotalCapacity, 
      long timeout) throws InterruptedException, TimeoutException {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    final int ATTEMPTS = 10;
    int count = 0;
    long currTotalCapacity = 0;
    int volFails = 0;

    do {
      Thread.sleep(timeout);
      live.clear();
      dead.clear();
      dm.fetchDatanodes(live, dead, false);
      currTotalCapacity = 0;
      volFails = 0;
      for (final DatanodeDescriptor dd : live) {
        currTotalCapacity += dd.getCapacity();
        volFails += dd.getVolumeFailures();
      }
      count++;
    } while ((expectedLive != live.size() ||
              expectedDead != dead.size() ||
              expectedTotalCapacity != currTotalCapacity ||
              expectedVolFails != volFails)
             && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for capacity."
          + " Live = "+live.size()+" Expected = "+expectedLive
          + " Dead = "+dead.size()+" Expected = "+expectedDead
          + " Total capacity = "+currTotalCapacity
          + " Expected = "+expectedTotalCapacity
          + " Vol Fails = "+volFails+" Expected = "+expectedVolFails);
    }
  }

  /*
   * Wait for the given DN to consider itself dead.
   */
  public static void waitForDatanodeDeath(DataNode dn) 
      throws InterruptedException, TimeoutException {
    final int ATTEMPTS = 10;
    int count = 0;
    do {
      Thread.sleep(1000);
      count++;
    } while (dn.isDatanodeUp() && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for DN to die");
    }
  }
  
  /** return list of filenames created as part of createFiles */
  public String[] getFileNames(String topDir) {
    if (nFiles == 0)
      return new String[]{};
    else {
      String[] fileNames =  new String[nFiles];
      for (int idx=0; idx < nFiles; idx++) {
        fileNames[idx] = topDir + "/" + files[idx].getName();
      }
      return fileNames;
    }
  }

  /**
   * Wait for the given file to reach the given replication factor.
   * @throws TimeoutException if we fail to sufficiently replicate the file
   */
  public static void waitReplication(FileSystem fs, Path fileName, short replFactor)
      throws IOException, InterruptedException, TimeoutException {
    boolean correctReplFactor;
    final int ATTEMPTS = 40;
    int count = 0;

    do {
      correctReplFactor = true;
      BlockLocation locs[] = fs.getFileBlockLocations(
        fs.getFileStatus(fileName), 0, Long.MAX_VALUE);
      count++;
      for (int j = 0; j < locs.length; j++) {
        String[] hostnames = locs[j].getNames();
        if (hostnames.length != replFactor) {
          correctReplFactor = false;
          System.out.println("Block " + j + " of file " + fileName
              + " has replication factor " + hostnames.length
              + " (desired " + replFactor + "); locations "
              + Joiner.on(' ').join(hostnames));
          Thread.sleep(1000);
          break;
        }
      }
      if (correctReplFactor) {
        System.out.println("All blocks of file " + fileName
            + " verified to have replication factor " + replFactor);
      }
    } while (!correctReplFactor && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for " + fileName +
          " to reach " + replFactor + " replicas");
    }
  }
  
  /** delete directory and everything underneath it.*/
  public void cleanup(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    fs.delete(root, true);
    files = null;
  }
  
  public static ExtendedBlock getFirstBlock(FileSystem fs, Path path) throws IOException {
    HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path);
    try {
      in.readByte();
      return in.getCurrentBlock();
    } finally {
      in.close();
    }
  }  

  public static List<LocatedBlock> getAllBlocks(FSDataInputStream in)
      throws IOException {
    return ((HdfsDataInputStream) in).getAllBlocks();
  }

  public static List<LocatedBlock> getAllBlocks(FileSystem fs, Path path)
      throws IOException {
    HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path);
    return in.getAllBlocks();
  }

  public static Token<BlockTokenIdentifier> getBlockToken(
      FSDataOutputStream out) {
    return ((DFSOutputStream) out.getWrappedStream()).getBlockToken();
  }

  static void setLogLevel2All(org.apache.commons.logging.Log log) {
    ((org.apache.commons.logging.impl.Log4JLogger)log
        ).getLogger().setLevel(org.apache.log4j.Level.ALL);
  }

  public static String readFile(File f) throws IOException {
    StringBuilder b = new StringBuilder();
    BufferedReader in = new BufferedReader(new FileReader(f));
    for(int c; (c = in.read()) != -1; b.append((char)c));
    in.close();      
    return b.toString();
  }

  /* Write the given string to the given file */
  public static void writeFile(FileSystem fs, Path p, String s) 
      throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    InputStream is = new ByteArrayInputStream(s.getBytes());
    FSDataOutputStream os = fs.create(p);
    IOUtils.copyBytes(is, os, s.length(), true);
  }

  /* Append the given string to the given file */
  public static void appendFile(FileSystem fs, Path p, String s) 
      throws IOException {
    assert fs.exists(p);
    InputStream is = new ByteArrayInputStream(s.getBytes());
    FSDataOutputStream os = fs.append(p);
    IOUtils.copyBytes(is, os, s.length(), true);
  }
  
  /**
   * Append specified length of bytes to a given file
   * @param fs The file system
   * @param p Path of the file to append
   * @param length Length of bytes to append to the file
   * @throws IOException
   */
  public static void appendFile(FileSystem fs, Path p, int length)
      throws IOException {
    assert fs.exists(p);
    assert length >= 0;
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    FSDataOutputStream out = fs.append(p);
    out.write(toAppend);
    out.close();
  }
  
  /**
   * @return url content as string (UTF-8 encoding assumed)
   */
  public static String urlGet(URL url) throws IOException {
    return new String(urlGetBytes(url), Charsets.UTF_8);
  }
  
  /**
   * @return URL contents as a byte array
   */
  public static byte[] urlGetBytes(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    HttpURLConnection hc = (HttpURLConnection)conn;
    
    assertEquals(HttpURLConnection.HTTP_OK, hc.getResponseCode());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    return out.toByteArray();
  }
  
  /**
   * mock class to get group mapping for fake users
   * 
   */
  static class MockUnixGroupsMapping extends ShellBasedUnixGroupsMapping {
    static Map<String, String []> fakeUser2GroupsMap;
    private static final List<String> defaultGroups;
    static {
      defaultGroups = new ArrayList<String>(1);
      defaultGroups.add("supergroup");
      fakeUser2GroupsMap = new HashMap<String, String[]>();
    }
  
    @Override
    public List<String> getGroups(String user) throws IOException {
      boolean found = false;
      
      // check to see if this is one of fake users
      List<String> l = new ArrayList<String>();
      for(String u : fakeUser2GroupsMap.keySet()) {  
        if(user.equals(u)) {
          found = true;
          for(String gr : fakeUser2GroupsMap.get(u)) {
            l.add(gr);
          }
        }
      }
      
      // default
      if(!found) {
        l =  super.getGroups(user);
        if(l.size() == 0) {
          System.out.println("failed to get real group for " + user + 
              "; using default");
          return defaultGroups;
        }
      }
      return l;
    }
  }
  
  /**
   * update the configuration with fake class for mapping user to groups
   * @param conf
   * @param map - user to groups mapping
   */
  static public void updateConfWithFakeGroupMapping
    (Configuration conf, Map<String, String []> map) {
    if(map!=null) {
      MockUnixGroupsMapping.fakeUser2GroupsMap = map;
    }
    
    // fake mapping user to groups
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        DFSTestUtil.MockUnixGroupsMapping.class,
        ShellBasedUnixGroupsMapping.class);
    
  }
  
  /**
   * Get a FileSystem instance as specified user in a doAs block.
   */
  static public FileSystem getFileSystemAs(UserGroupInformation ugi, 
                                   final Configuration conf) throws IOException, 
                                                        InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });
  }

  public static byte[] generateSequentialBytes(int start, int length) {
    byte[] result = new byte[length];

    for (int i = 0; i < length; i++) {
      result[i] = (byte) ((start + i) % 127);
    }

    return result;
  }
  
  public static Statistics getStatistics(FileSystem fs) {
    return FileSystem.getStatistics(fs.getUri().getScheme(), fs.getClass());
  }

  /**
   * Load file into byte[]
   */
  public static byte[] loadFile(String filename) throws IOException {
    File file = new File(filename);
    DataInputStream in = new DataInputStream(new FileInputStream(file));
    byte[] content = new byte[(int)file.length()];
    try {
      in.readFully(content);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return content;
  }

  /** For {@link TestTransferRbw} */
  public static BlockOpResponseProto transferRbw(final ExtendedBlock b, 
      final DFSClient dfsClient, final DatanodeInfo... datanodes) throws IOException {
    assertEquals(2, datanodes.length);
    final Socket s = DFSOutputStream.createSocketForPipeline(datanodes[0],
        datanodes.length, dfsClient);
    final long writeTimeout = dfsClient.getDatanodeWriteTimeout(datanodes.length);
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        NetUtils.getOutputStream(s, writeTimeout),
        HdfsConstants.SMALL_BUFFER_SIZE));
    final DataInputStream in = new DataInputStream(NetUtils.getInputStream(s));

    // send the request
    new Sender(out).transferBlock(b, new Token<BlockTokenIdentifier>(),
        dfsClient.clientName, new DatanodeInfo[]{datanodes[1]});
    out.flush();

    return BlockOpResponseProto.parseDelimitedFrom(in);
  }
  
  public static void setFederatedConfiguration(MiniDFSCluster cluster,
      Configuration conf) {
    Set<String> nameservices = new HashSet<String>();
    for (NameNodeInfo info : cluster.getNameNodeInfos()) {
      assert info.nameserviceId != null;
      nameservices.add(info.nameserviceId);
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
          info.nameserviceId), DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
              info.nameNode.getNameNodeAddress()).toString());
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          info.nameserviceId), DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
              info.nameNode.getNameNodeAddress()).toString());
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, Joiner.on(",")
        .join(nameservices));
  }
  
  private static DatanodeID getDatanodeID(String ipAddr) {
    return new DatanodeID(ipAddr, "localhost",
        UUID.randomUUID().toString(),
        DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
  }

  public static DatanodeID getLocalDatanodeID() {
    return getDatanodeID("127.0.0.1");
  }

  public static DatanodeID getLocalDatanodeID(int port) {
    return new DatanodeID("127.0.0.1", "localhost",
        UUID.randomUUID().toString(),
        port, port, port, port);
  }

  public static DatanodeDescriptor getLocalDatanodeDescriptor() {
    return new DatanodeDescriptor(getLocalDatanodeID());
  }

  public static DatanodeInfo getLocalDatanodeInfo() {
    return new DatanodeInfo(getLocalDatanodeID());
  }

  public static DatanodeInfo getDatanodeInfo(String ipAddr) {
    return new DatanodeInfo(getDatanodeID(ipAddr));
  }
  
  public static DatanodeInfo getLocalDatanodeInfo(int port) {
    return new DatanodeInfo(getLocalDatanodeID(port));
  }

  public static DatanodeInfo getDatanodeInfo(String ipAddr, 
      String host, int port) {
    return new DatanodeInfo(new DatanodeID(ipAddr, host,
        UUID.randomUUID().toString(), port,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT));
  }

  public static DatanodeInfo getLocalDatanodeInfo(String ipAddr,
      String hostname, AdminStates adminState) {
    return new DatanodeInfo(ipAddr, hostname, "",
        DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT,
        1l, 2l, 3l, 4l, 0l, 0l, 5, 6, "local", adminState);
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation) {
    return getDatanodeDescriptor(ipAddr, DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
        rackLocation);
  }

  public static DatanodeStorageInfo createDatanodeStorageInfo(
      String storageID, String ip) {
    return createDatanodeStorageInfo(storageID, ip, "defaultRack");
  }
  public static DatanodeStorageInfo[] createDatanodeStorageInfos(String[] racks) {
    return createDatanodeStorageInfos(racks.length, racks);
  }
  public static DatanodeStorageInfo[] createDatanodeStorageInfos(int n, String... racks) {
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[n];
    for(int i = storages.length; i > 0; ) {
      final String storageID = "s" + i;
      final String ip = i + "." + i + "." + i + "." + i;
      i--;
      final String rack = i < racks.length? racks[i]: "defaultRack";
      storages[i] = createDatanodeStorageInfo(storageID, ip, rack);
    }
    return storages;
  }
  public static DatanodeStorageInfo createDatanodeStorageInfo(
      String storageID, String ip, String rack) {
    final DatanodeStorage storage = new DatanodeStorage(storageID);
    final DatanodeDescriptor dn = BlockManagerTestUtil.getDatanodeDescriptor(ip, rack, storage);
    return BlockManagerTestUtil.newDatanodeStorageInfo(dn, storage);
  }
  public static DatanodeDescriptor[] toDatanodeDescriptor(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for(int i = 0; i < datanodes.length; i++) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      int port, String rackLocation) {
    DatanodeID dnId = new DatanodeID(ipAddr, "host",
        UUID.randomUUID().toString(), port,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    return new DatanodeDescriptor(dnId, rackLocation);
  }
  
  public static DatanodeRegistration getLocalDatanodeRegistration() {
    return new DatanodeRegistration(getLocalDatanodeID(), new StorageInfo(
        NodeType.DATA_NODE), new ExportedBlockKeys(), VersionInfo.getVersion());
  }
  
  /** Copy one file's contents into the other **/
  public static void copyFile(File src, File dest) throws IOException {
    FileUtils.copyFile(src, dest);
  }

  public static class Builder {
    private int maxLevels = 3;
    private int maxSize = 8*1024;
    private int minSize = 1;
    private int nFiles = 1;
    
    public Builder() {
    }
    
    public Builder setName(String string) {
      return this;
    }

    public Builder setNumFiles(int nFiles) {
      this.nFiles = nFiles;
      return this;
    }
    
    public Builder setMaxLevels(int maxLevels) {
      this.maxLevels = maxLevels;
      return this;
    }

    public Builder setMaxSize(int maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder setMinSize(int minSize) {
      this.minSize = minSize;
      return this;
    }
    
    public DFSTestUtil build() {
      return new DFSTestUtil(nFiles, maxLevels, maxSize, minSize);
    }
  }
  
  /**
   * Run a set of operations and generate all edit logs
   */
  public static void runOperations(MiniDFSCluster cluster,
      DistributedFileSystem filesystem, Configuration conf, long blockSize, 
      int nnIndex) throws IOException {
    // create FileContext for rename2
    FileContext fc = FileContext.getFileContext(cluster.getURI(0), conf);
    
    // OP_ADD 0
    final Path pathFileCreate = new Path("/file_create");
    FSDataOutputStream s = filesystem.create(pathFileCreate);
    // OP_CLOSE 9
    s.close();
    // OP_RENAME_OLD 1
    final Path pathFileMoved = new Path("/file_moved");
    filesystem.rename(pathFileCreate, pathFileMoved);
    // OP_DELETE 2
    filesystem.delete(pathFileMoved, false);
    // OP_MKDIR 3
    Path pathDirectoryMkdir = new Path("/directory_mkdir");
    filesystem.mkdirs(pathDirectoryMkdir);
    // OP_ALLOW_SNAPSHOT 29
    filesystem.allowSnapshot(pathDirectoryMkdir);
    // OP_DISALLOW_SNAPSHOT 30
    filesystem.disallowSnapshot(pathDirectoryMkdir);
    // OP_CREATE_SNAPSHOT 26
    String ssName = "snapshot1";
    filesystem.allowSnapshot(pathDirectoryMkdir);
    filesystem.createSnapshot(pathDirectoryMkdir, ssName);
    // OP_RENAME_SNAPSHOT 28
    String ssNewName = "snapshot2";
    filesystem.renameSnapshot(pathDirectoryMkdir, ssName, ssNewName);
    // OP_DELETE_SNAPSHOT 27
    filesystem.deleteSnapshot(pathDirectoryMkdir, ssNewName);
    // OP_SET_REPLICATION 4
    s = filesystem.create(pathFileCreate);
    s.close();
    filesystem.setReplication(pathFileCreate, (short)1);
    // OP_SET_PERMISSIONS 7
    Short permission = 0777;
    filesystem.setPermission(pathFileCreate, new FsPermission(permission));
    // OP_SET_OWNER 8
    filesystem.setOwner(pathFileCreate, new String("newOwner"), null);
    // OP_CLOSE 9 see above
    // OP_SET_GENSTAMP 10 see above
    // OP_SET_NS_QUOTA 11 obsolete
    // OP_CLEAR_NS_QUOTA 12 obsolete
    // OP_TIMES 13
    long mtime = 1285195527000L; // Wed, 22 Sep 2010 22:45:27 GMT
    long atime = mtime;
    filesystem.setTimes(pathFileCreate, mtime, atime);
    // OP_SET_QUOTA 14
    filesystem.setQuota(pathDirectoryMkdir, 1000L, 
        HdfsConstants.QUOTA_DONT_SET);
    // OP_RENAME 15
    fc.rename(pathFileCreate, pathFileMoved, Rename.NONE);
    // OP_CONCAT_DELETE 16
    Path   pathConcatTarget = new Path("/file_concat_target");
    Path[] pathConcatFiles  = new Path[2];
    pathConcatFiles[0]      = new Path("/file_concat_0");
    pathConcatFiles[1]      = new Path("/file_concat_1");

    long length = blockSize * 3; // multiple of blocksize for concat
    short replication = 1;
    long seed = 1;
    DFSTestUtil.createFile(filesystem, pathConcatTarget, length, replication,
        seed);
    DFSTestUtil.createFile(filesystem, pathConcatFiles[0], length, replication,
        seed);
    DFSTestUtil.createFile(filesystem, pathConcatFiles[1], length, replication,
        seed);
    filesystem.concat(pathConcatTarget, pathConcatFiles);
    
    // OP_SYMLINK 17
    Path pathSymlink = new Path("/file_symlink");
    fc.createSymlink(pathConcatTarget, pathSymlink, false);
    
    // OP_REASSIGN_LEASE 22
    String filePath = "/hard-lease-recovery-test";
    byte[] bytes = "foo-bar-baz".getBytes();
    DFSClientAdapter.stopLeaseRenewer(filesystem);
    FSDataOutputStream leaseRecoveryPath = filesystem.create(new Path(filePath));
    leaseRecoveryPath.write(bytes);
    leaseRecoveryPath.hflush();
    // Set the hard lease timeout to 1 second.
    cluster.setLeasePeriod(60 * 1000, 1000, nnIndex);
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      locatedBlocks = DFSClientAdapter.callGetBlockLocations(
          cluster.getNameNodeRpc(nnIndex), filePath, 0L, bytes.length);
    } while (locatedBlocks.isUnderConstruction());
    // OP_ADD_CACHE_POOL
    filesystem.addCachePool(new CachePoolInfo("pool1"));
    // OP_MODIFY_CACHE_POOL
    filesystem.modifyCachePool(new CachePoolInfo("pool1").setLimit(99l));
    // OP_ADD_PATH_BASED_CACHE_DIRECTIVE
    long id = filesystem.addCacheDirective(
        new CacheDirectiveInfo.Builder().
            setPath(new Path("/path")).
            setReplication((short)1).
            setPool("pool1").
            build(), EnumSet.of(CacheFlag.FORCE));
    // OP_MODIFY_PATH_BASED_CACHE_DIRECTIVE
    filesystem.modifyCacheDirective(
        new CacheDirectiveInfo.Builder().
            setId(id).
            setReplication((short)2).
            build(), EnumSet.of(CacheFlag.FORCE));
    // OP_REMOVE_PATH_BASED_CACHE_DIRECTIVE
    filesystem.removeCacheDirective(id);
    // OP_REMOVE_CACHE_POOL
    filesystem.removeCachePool("pool1");
    // OP_SET_ACL
    List<AclEntry> aclEntryList = Lists.newArrayList();
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.READ_WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setName("user")
            .setPermission(FsAction.READ_WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.WRITE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.GROUP)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.NONE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.OTHER)
            .build());
    filesystem.setAcl(pathConcatTarget, aclEntryList);
  }

  public static void abortStream(DFSOutputStream out) throws IOException {
    out.abort();
  }

  public static byte[] asArray(ByteBuffer buf) {
    byte arr[] = new byte[buf.remaining()];
    buf.duplicate().get(arr);
    return arr;
  }

  /**
   * Blocks until cache usage hits the expected new value.
   */
  public static long verifyExpectedCacheUsage(final long expectedCacheUsed,
      final long expectedBlocks, final FsDatasetSpi<?> fsd) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      private int tries = 0;
      
      @Override
      public Boolean get() {
        long curCacheUsed = fsd.getCacheUsed();
        long curBlocks = fsd.getNumBlocksCached();
        if ((curCacheUsed != expectedCacheUsed) ||
            (curBlocks != expectedBlocks)) {
          if (tries++ > 10) {
            LOG.info("verifyExpectedCacheUsage: have " +
                curCacheUsed + "/" + expectedCacheUsed + " bytes cached; " +
                curBlocks + "/" + expectedBlocks + " blocks cached. " +
                "memlock limit = " +
                NativeIO.POSIX.getCacheManipulator().getMemlockLimit() +
                ".  Waiting...");
          }
          return false;
        }
        LOG.info("verifyExpectedCacheUsage: got " +
            curCacheUsed + "/" + expectedCacheUsed + " bytes cached; " +
            curBlocks + "/" + expectedBlocks + " blocks cached. " +
            "memlock limit = " +
            NativeIO.POSIX.getCacheManipulator().getMemlockLimit());
        return true;
      }
    }, 100, 60000);
    return expectedCacheUsed;
  }

  /**
   * Round a long value up to a multiple of a factor.
   *
   * @param val    The value.
   * @param factor The factor to round up to.  Must be > 1.
   * @return       The rounded value.
   */
  public static long roundUpToMultiple(long val, int factor) {
    assert (factor > 1);
    long c = (val + factor - 1) / factor;
    return c * factor;
  }

  public static void checkComponentsEquals(byte[][] expected, byte[][] actual) {
    assertEquals("expected: " + DFSUtil.byteArray2PathString(expected)
        + ", actual: " + DFSUtil.byteArray2PathString(actual), expected.length,
        actual.length);
    int i = 0;
    for (byte[] e : expected) {
      byte[] actualComponent = actual[i++];
      assertTrue("expected: " + DFSUtil.bytes2String(e) + ", actual: "
          + DFSUtil.bytes2String(actualComponent),
          Arrays.equals(e, actualComponent));
    }
  }

  /**
   * A short-circuit test context which makes it easier to get a short-circuit
   * configuration and set everything up.
   */
  public static class ShortCircuitTestContext implements Closeable {
    private final String testName;
    private final TemporarySocketDirectory sockDir;
    private boolean closed = false;
    private final boolean formerTcpReadsDisabled;
    
    public ShortCircuitTestContext(String testName) {
      this.testName = testName;
      this.sockDir = new TemporarySocketDirectory();
      DomainSocket.disableBindPathValidation();
      formerTcpReadsDisabled = DFSInputStream.tcpReadsDisabledForTesting;
      Assume.assumeTrue(DomainSocket.getLoadingFailureReason() == null);
    }
    
    public Configuration newConfiguration() {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          new File(sockDir.getDir(),
            testName + "._PORT.sock").getAbsolutePath());
      return conf;
    }

    public String getTestName() {
      return testName;
    }

    public void close() throws IOException {
      if (closed) return;
      closed = true;
      DFSInputStream.tcpReadsDisabledForTesting = formerTcpReadsDisabled;
      sockDir.close();
    }
  }
}
