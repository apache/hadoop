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

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.TestTransferRbw;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/** Utilities for HDFS tests */
public class DFSTestUtil {
  
  private static Random gen = new Random();
  private static String[] dirNames = {
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
  };
  
  private int maxLevels;// = 3;
  private int maxSize;// = 8*1024;
  private int minSize = 1;
  private int nFiles;
  private MyFile[] files;
  
  /** Creates a new instance of DFSTestUtil
   *
   * @param testName Name of the test from where this utility is used
   * @param nFiles Number of files to be created
   * @param maxLevels Maximum number of directory levels
   * @param maxSize Maximum size for file
   */
  public DFSTestUtil(String testName, int nFiles, int maxLevels, int maxSize) {
    this.nFiles = nFiles;
    this.maxLevels = maxLevels;
    this.maxSize = maxSize;
  }
  
  /**
   * when formating a namenode - we must provide clusterid.
   * @param conf
   * @throws IOException
   */
  public static void formatNameNode(Configuration conf) throws IOException {
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.isEmpty())
      StartupOption.FORMAT.setClusterId("testClusterID");

    NameNode.format(conf);
  }
  
  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private class MyFile {
    
    private String name = "";
    private int size;
    private long seed;
    
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

  public static String readFile(FileSystem fs, Path fileName) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(fileName), os, 1024, true);
    return os.toString();
  }
  
  public static void createFile(FileSystem fs, Path fileName, long fileLen, 
      short replFactor, long seed) throws IOException {
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " + 
                            fileName.getParent().toString());
    }
    FSDataOutputStream out = null;
    try {
      out = fs.create(fileName, replFactor);
      byte[] toWrite = new byte[1024];
      Random rb = new Random(seed);
      long bytesToWrite = fileLen;
      while (bytesToWrite>0) {
        rb.nextBytes(toWrite);
        int bytesToWriteNext = (1024<bytesToWrite)?1024:(int)bytesToWrite;

        out.write(toWrite, 0, bytesToWriteNext);
        bytesToWrite -= bytesToWriteNext;
      }
      out.close();
      out = null;
    } finally {
      IOUtils.closeStream(out);
    }
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
                                              throws IOException {
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
      throws IOException, TimeoutException,InterruptedException {
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
      repls = ns.getBlockManager().numCorruptReplicas(b.getLocalBlock());
      count++;
      Thread.sleep(1000);
    }
    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for corrupt replicas."
          + " Waiting for "+corruptRepls+", but only found "+repls);
    }
  }

  /*
   * Wait up to 20s for the given DN (host:port) to be decommissioned.
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
        if (name.equals(info.getName())) {
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
  
  /** wait for the file's replication to be done */
  public static void waitReplication(FileSystem fs, Path fileName, 
      short replFactor)  throws IOException {
    boolean good;
    do {
      good = true;
      BlockLocation locs[] = fs.getFileBlockLocations(
        fs.getFileStatus(fileName), 0, Long.MAX_VALUE);
      for (int j = 0; j < locs.length; j++) {
        String[] hostnames = locs[j].getNames();
        if (hostnames.length != replFactor) {
          String hostNameList = "";
          for (String h : hostnames) hostNameList += h + " ";
          System.out.println("Block " + j + " of file " + fileName 
              + " has replication factor " + hostnames.length + "; locations "
              + hostNameList);
          good = false;
          try {
            System.out.println("Waiting for replication factor to drain");
            Thread.sleep(100);
          } catch (InterruptedException e) {} 
          break;
        }
      }
      if (good) {
        System.out.println("All blocks of file " + fileName
            + " verified to have replication factor " + replFactor);
      }
    } while(!good);
  }
  
  /** delete directory and everything underneath it.*/
  public void cleanup(FileSystem fs, String topdir) throws IOException {
    Path root = new Path(topdir);
    fs.delete(root, true);
    files = null;
  }
  
  public static ExtendedBlock getFirstBlock(FileSystem fs, Path path) throws IOException {
    DFSDataInputStream in = 
      (DFSDataInputStream) ((DistributedFileSystem)fs).open(path);
    in.readByte();
    return in.getCurrentBlock();
  }  

  public static List<LocatedBlock> getAllBlocks(FSDataInputStream in)
      throws IOException {
    return ((DFSClient.DFSDataInputStream) in).getAllBlocks();
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
  
  // Returns url content as string.
  public static String urlGet(URL url) throws IOException {
    URLConnection conn = url.openConnection();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    return out.toString();
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
    in.readFully(content);
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
}
