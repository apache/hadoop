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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.TestTransferRbw;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.XAttrStorage;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.VersionInfo;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Assume;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/** Utilities for HDFS tests */
public class DFSTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DFSTestUtil.class);
  
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
    // Use a copy of conf as it can be altered by namenode during format.
    NameNode.format(new Configuration(conf));
  }

  /**
   * Create a new HA-enabled configuration.
   */
  public static Configuration newHAConfiguration(final String logicalName) {
    Configuration conf = new Configuration();
    addHAConfiguration(conf, logicalName);
    return conf;
  }

  /**
   * Add a new HA configuration.
   */
  public static void addHAConfiguration(Configuration conf,
      final String logicalName) {
    String nsIds = conf.get(DFSConfigKeys.DFS_NAMESERVICES);
    if (nsIds == null) {
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, logicalName);
    } else { // append the nsid
      conf.set(DFSConfigKeys.DFS_NAMESERVICES, nsIds + "," + logicalName);
    }
    conf.set(DFSUtil.addKeySuffixes(HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            logicalName), "nn1,nn2");
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX +
            "." + logicalName,
            ConfiguredFailoverProxyProvider.class.getName());
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
  }

  public static void setFakeHttpAddresses(Configuration conf,
      final String logicalName) {
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        logicalName, "nn1"), "127.0.0.1:12345");
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        logicalName, "nn2"), "127.0.0.1:12346");
  }

  public static void setEditLogForTesting(FSNamesystem fsn, FSEditLog newLog) {
    // spies are shallow copies, must allow async log to restart its thread
    // so it has the new copy
    newLog.restart();
    Whitebox.setInternalState(fsn.getFSImage(), "editLog", newLog);
    Whitebox.setInternalState(fsn.getFSDirectory(), "editLog", newLog);
  }

  public static void enableAllECPolicies(DistributedFileSystem fs)
      throws IOException {
    // Enable all available EC policies
    for (ErasureCodingPolicy ecPolicy :
        SystemErasureCodingPolicies.getPolicies()) {
      fs.enableErasureCodingPolicy(ecPolicy.getName());
    }
  }

  public static ErasureCodingPolicyState getECPolicyState(
      final ErasureCodingPolicy policy) {
    final ErasureCodingPolicyInfo[] policyInfos =
        ErasureCodingPolicyManager.getInstance().getPolicies();
    for (ErasureCodingPolicyInfo pi : policyInfos) {
      if (pi.getPolicy().equals(policy)) {
        return pi.getState();
      }
    }
    throw new IllegalArgumentException("ErasureCodingPolicy <" + policy
        + "> doesn't exist in the policies:" + Arrays.toString(policyInfos));
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

  public static String readResoucePlainFile(
      String fileName) throws IOException {
    File file = new File(System.getProperty(
        "test.cache.data", "build/test/cache"), fileName);
    StringBuilder s = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.length() <= 0 || line.startsWith("#")) {
          continue;
        }
        s.append(line);
        s.append("\n");
      }
    }
    return s.toString();
  }

  public static byte[] readFileAsBytes(FileSystem fs, Path fileName) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      IOUtils.copyBytes(fs.open(fileName), os, 1024);
      return os.toByteArray();
    }
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
    try (ByteArrayOutputStream os = new ByteArrayOutputStream();
         FSDataInputStream in = fs.open(fileName)) {
      IOUtils.copyBytes(in, os, 1024, true);
      return os.toByteArray();
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
    createFile(fs, fileName, false, bufferLen, fileLen, blockSize, replFactor,
      seed, false);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush) throws IOException {
        createFile(fs, fileName, isLazyPersist, bufferLen, fileLen, blockSize,
          replFactor, seed, flush, null);
  }

  public static void createFile(FileSystem fs, Path fileName,
      boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
      short replFactor, long seed, boolean flush,
      InetSocketAddress[] favoredNodes) throws IOException {
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          fileName.getParent().toString());
    }
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);
    createFlags.add(OVERWRITE);
    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }
    try (FSDataOutputStream out = (favoredNodes == null) ?
        fs.create(fileName, FsPermission.getFileDefault(), createFlags,
            fs.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, 4096), replFactor,
            blockSize, null)
        :
        ((DistributedFileSystem) fs).create(fileName, FsPermission.getDefault(),
            true, bufferLen, replFactor, blockSize, null, favoredNodes)
    ) {
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(seed);
        long bytesToWrite = fileLen;
        while (bytesToWrite > 0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
              : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
        if (flush) {
          out.hsync();
        }
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
      try (FSDataInputStream in = fs.open(fPath)) {
        byte[] toRead = new byte[files[idx].getSize()];
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        in.readFully(0, toRead);
        for (int i = 0; i < toRead.length; i++) {
          if (toRead[i] != toCompare[i]) {
            return false;
          }
        }
      }
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
    try (DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), cluster.getConfiguration(0))) {
      LocatedBlocks blocks;
      blocks = client.getNamenode().getBlockLocations(
           file.toString(), 0, Long.MAX_VALUE);
      return blocks.get(blockNo).isCorrupt();
    }
  }

  public static void waitForReplication(MiniDFSCluster cluster, ExtendedBlock b,
      int racks, int replicas, int neededReplicas)
      throws TimeoutException, InterruptedException {
    waitForReplication(cluster, b, racks, replicas, neededReplicas, 0);
  }

  /*
   * Wait up to 20s for the given block to be replicated across
   * the requested number of racks, with the requested number of
   * replicas, and the requested number of replicas still needed.
   */
  public static void waitForReplication(MiniDFSCluster cluster, ExtendedBlock b,
      int racks, int replicas, int neededReplicas, int neededDomains)
      throws TimeoutException, InterruptedException {
    int curRacks = 0;
    int curReplicas = 0;
    int curNeededReplicas = 0;
    int curDomains = 0;
    int count = 0;
    final int ATTEMPTS = 20;

    do {
      Thread.sleep(1000);
      int[] r = BlockManagerTestUtil.getReplicaInfo(cluster.getNamesystem(),
          b.getLocalBlock());
      curRacks = r[0];
      curReplicas = r[1];
      curNeededReplicas = r[2];
      curDomains = r[3];
      count++;
    } while ((curRacks != racks ||
              curReplicas != replicas ||
        curNeededReplicas != neededReplicas ||
        (neededDomains != 0 && curDomains != neededDomains))
        && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for replication."
          + " Needed replicas = "+neededReplicas
          + " Cur needed replicas = "+curNeededReplicas
          + " Replicas = "+replicas+" Cur replicas = "+curReplicas
          + " Racks = "+racks+" Cur racks = "+curRacks
          + " Domains = "+neededDomains+" Cur domains = "+curDomains);
    }
  }

  public static void waitForReplication(final DistributedFileSystem dfs,
      final Path file, final short replication, int waitForMillis)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          FileStatus stat = dfs.getFileStatus(file);
          BlockLocation[] locs = dfs.getFileBlockLocations(stat, 0, stat
              .getLen());
          for (BlockLocation loc : locs) {
            if (replication != loc.getHosts().length) {
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          LOG.info("getFileStatus on path " + file + " failed!", e);
          return false;
        }
      }
    }, 100, waitForMillis);
  }

  /**
   * Keep accessing the given file until the namenode reports that the
   * given block in the file contains the given number of corrupt replicas.
   */
  public static void waitCorruptReplicas(FileSystem fs, FSNamesystem ns,
      Path file, ExtendedBlock b, int corruptRepls)
      throws TimeoutException, InterruptedException {
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
    try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path)) {
      in.readByte();
      return in.getCurrentBlock();
    }
  }  

  public static List<LocatedBlock> getAllBlocks(FSDataInputStream in)
      throws IOException {
    return ((HdfsDataInputStream) in).getAllBlocks();
  }

  public static List<LocatedBlock> getAllBlocks(FileSystem fs, Path path)
      throws IOException {
    try (HdfsDataInputStream in = (HdfsDataInputStream) fs.open(path)) {
      return in.getAllBlocks();
    }
  }

  public static Token<BlockTokenIdentifier> getBlockToken(
      FSDataOutputStream out) {
    return ((DFSOutputStream) out.getWrappedStream()).getBlockToken();
  }

  public static String readFile(File f) throws IOException {
    try (BufferedReader in = new BufferedReader(new FileReader(f))) {
      StringBuilder b = new StringBuilder();
      int c;
      while ((c = in.read()) != -1) {
        b.append((char) c);
      }
      return b.toString();
    }
  }

  public static byte[] readFileAsBytes(File f) throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      IOUtils.copyBytes(new FileInputStream(f), os, 1024);
      return os.toByteArray();
    }
  }

  /* Write the given bytes to the given file */
  public static void writeFile(FileSystem fs, Path p, byte[] bytes)
      throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (InputStream is = new ByteArrayInputStream(bytes);
      FSDataOutputStream os = fs.create(p)) {
      IOUtils.copyBytes(is, os, bytes.length);
    }
  }

  /* Write the given bytes to the given file using the specified blockSize */
  public static void writeFile(
      FileSystem fs, Path p, byte[] bytes, long blockSize)
      throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (InputStream is = new ByteArrayInputStream(bytes);
        FSDataOutputStream os = fs.create(
            p, false, 4096, fs.getDefaultReplication(p), blockSize)) {
      IOUtils.copyBytes(is, os, bytes.length);
    }
  }

  /* Write the given string to the given file */
  public static void writeFile(FileSystem fs, Path p, String s)
      throws IOException {
    writeFile(fs, p, s.getBytes());
  }

  /* Append the given string to the given file */
  public static void appendFile(FileSystem fs, Path p, String s) 
      throws IOException {
    assert fs.exists(p);
    try (InputStream is = new ByteArrayInputStream(s.getBytes());
      FSDataOutputStream os = fs.append(p)) {
      IOUtils.copyBytes(is, os, s.length());
    }
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
    try (FSDataOutputStream out = fs.append(p)) {
      out.write(toAppend);
    }
  }

  /**
   * Append specified length of bytes to a given file, starting with new block.
   * @param fs The file system
   * @param p Path of the file to append
   * @param length Length of bytes to append to the file
   * @throws IOException
   */
  public static void appendFileNewBlock(DistributedFileSystem fs,
      Path p, int length) throws IOException {
    assert length >= 0;
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    appendFileNewBlock(fs, p, toAppend);
  }

  /**
   * Append specified bytes to a given file, starting with new block.
   *
   * @param fs The file system
   * @param p Path of the file to append
   * @param bytes The data to append
   * @throws IOException
   */
  public static void appendFileNewBlock(DistributedFileSystem fs,
      Path p, byte[] bytes) throws IOException {
    assert fs.exists(p);
    try (FSDataOutputStream out = fs.append(p,
        EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null)) {
      out.write(bytes);
    }
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
      final Configuration conf) throws IOException {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(conf);
        }
      });
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
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
    try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
      byte[] content = new byte[(int) file.length()];
      in.readFully(content);
      return content;
    }
  }

  /** For {@link TestTransferRbw} */
  public static BlockOpResponseProto transferRbw(final ExtendedBlock b, 
      final DFSClient dfsClient, final DatanodeInfo... datanodes) throws IOException {
    assertEquals(2, datanodes.length);
    final long writeTimeout = dfsClient.getDatanodeWriteTimeout(datanodes.length);
    try (Socket s = DataStreamer.createSocketForPipeline(datanodes[0],
             datanodes.length, dfsClient);
         DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
             NetUtils.getOutputStream(s, writeTimeout),
             DFSUtilClient.getSmallBufferSize(dfsClient.getConfiguration())));
         DataInputStream in = new DataInputStream(NetUtils.getInputStream(s))) {
      // send the request
      new Sender(out).transferBlock(b, new Token<BlockTokenIdentifier>(),
          dfsClient.clientName, new DatanodeInfo[]{datanodes[1]},
          new StorageType[]{StorageType.DEFAULT},
          new String[0]);
      out.flush();

      return BlockOpResponseProto.parseDelimitedFrom(in);
    }
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

  public static void setFederatedHAConfiguration(MiniDFSCluster cluster,
      Configuration conf) {
    Map<String, List<String>> nameservices = Maps.newHashMap();
    for (NameNodeInfo info : cluster.getNameNodeInfos()) {
      Preconditions.checkState(info.nameserviceId != null);
      List<String> nns = nameservices.get(info.nameserviceId);
      if (nns == null) {
        nns = Lists.newArrayList();
        nameservices.put(info.nameserviceId, nns);
      }
      nns.add(info.nnId);

      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_RPC_ADDRESS_KEY,
          info.nameserviceId, info.nnId),
          DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
          info.nameNode.getNameNodeAddress()).toString());
      conf.set(DFSUtil.addKeySuffixes(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          info.nameserviceId, info.nnId),
          DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
          info.nameNode.getNameNodeAddress()).toString());
    }
    for (Map.Entry<String, List<String>> entry : nameservices.entrySet()) {
      conf.set(DFSUtil.addKeySuffixes(DFS_HA_NAMENODES_KEY_PREFIX,
          entry.getKey()), Joiner.on(",").join(entry.getValue()));
      conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "."
          + entry.getKey(), ConfiguredFailoverProxyProvider.class.getName());
    }
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, Joiner.on(",")
        .join(nameservices.keySet()));
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
    return new DatanodeInfoBuilder().setNodeID(getLocalDatanodeID())
        .build();
  }

  public static DatanodeInfo getDatanodeInfo(String ipAddr) {
    return new DatanodeInfoBuilder().setNodeID(getDatanodeID(ipAddr))
        .build();
  }
  
  public static DatanodeInfo getLocalDatanodeInfo(int port) {
    return new DatanodeInfoBuilder().setNodeID(getLocalDatanodeID(port))
        .build();
  }

  public static DatanodeInfo getDatanodeInfo(String ipAddr, 
      String host, int port) {
    return new DatanodeInfoBuilder().setNodeID(
        new DatanodeID(ipAddr, host, UUID.randomUUID().toString(), port,
            DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
            DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
            DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT)).build();
  }

  public static DatanodeInfo getLocalDatanodeInfo(String ipAddr,
      String hostname, AdminStates adminState) {
    return new DatanodeInfoBuilder().setIpAddr(ipAddr).setHostName(hostname)
        .setDatanodeUuid("")
        .setXferPort(DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT)
        .setInfoPort(DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT)
        .setInfoSecurePort(DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT)
        .setIpcPort(DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT).setCapacity(1L)
        .setDfsUsed(2L).setRemaining(3L).setBlockPoolUsed(4L)
        .setCacheCapacity(0L).setCacheUsed(0L).setLastUpdate(0L)
        .setLastUpdateMonotonic(5).setXceiverCount(6)
        .setNetworkLocation("local").setAdminState(adminState)
        .build();
  }

  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation) {
    return getDatanodeDescriptor(ipAddr, DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT,
        rackLocation);
  }
  
  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      String rackLocation, String hostname) {
    return getDatanodeDescriptor(ipAddr, 
        DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT, rackLocation, hostname);
  }

  public static DatanodeStorageInfo createDatanodeStorageInfo(
      String storageID, String ip) {
    return createDatanodeStorageInfo(storageID, ip, "defaultRack", "host");
  }
  
  public static DatanodeStorageInfo[] createDatanodeStorageInfos(String[] racks) {
    return createDatanodeStorageInfos(racks, null);
  }
  
  public static DatanodeStorageInfo[] createDatanodeStorageInfos(String[] racks, String[] hostnames) {
    return createDatanodeStorageInfos(racks.length, racks, hostnames);
  }
  
  public static DatanodeStorageInfo[] createDatanodeStorageInfos(int n) {
    return createDatanodeStorageInfos(n, null, null);
  }

  public static DatanodeStorageInfo[] createDatanodeStorageInfos(
      int n, String[] racks, String[] hostnames) {
    return createDatanodeStorageInfos(n, racks, hostnames, null);
  }

  public static DatanodeStorageInfo[] createDatanodeStorageInfos(
      int n, String[] racks, String[] hostnames, StorageType[] types) {
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[n];
    for(int i = storages.length; i > 0; ) {
      final String storageID = "s" + i;
      final String ip = i + "." + i + "." + i + "." + i;
      i--;
      final String rack = (racks!=null && i < racks.length)? racks[i]: "defaultRack";
      final String hostname = (hostnames!=null && i < hostnames.length)? hostnames[i]: "host";
      final StorageType type = (types != null && i < types.length) ? types[i]
          : StorageType.DEFAULT;
      storages[i] = createDatanodeStorageInfo(storageID, ip, rack, hostname,
          type, null);
    }
    return storages;
  }

  public static DatanodeStorageInfo createDatanodeStorageInfo(
      String storageID, String ip, String rack, String hostname) {
    return createDatanodeStorageInfo(storageID, ip, rack, hostname,
        StorageType.DEFAULT, null);
  }

  public static DatanodeStorageInfo createDatanodeStorageInfo(
      String storageID, String ip, String rack, String hostname,
      StorageType type, String upgradeDomain) {
    final DatanodeStorage storage = new DatanodeStorage(storageID,
        DatanodeStorage.State.NORMAL, type);
    final DatanodeDescriptor dn = BlockManagerTestUtil.getDatanodeDescriptor(
        ip, rack, storage, hostname);
    if (upgradeDomain != null) {
      dn.setUpgradeDomain(upgradeDomain);
    }
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
      int port, String rackLocation, String hostname) {
    DatanodeID dnId = new DatanodeID(ipAddr, hostname,
        UUID.randomUUID().toString(), port,
        DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
        DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    return new DatanodeDescriptor(dnId, rackLocation);
  }
  
  public static DatanodeDescriptor getDatanodeDescriptor(String ipAddr,
      int port, String rackLocation) {
    return getDatanodeDescriptor(ipAddr, port, rackLocation, "host");
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
    // OP_APPEND 47
    FSDataOutputStream s2 = filesystem.append(pathFileCreate, 4096, null);
    s2.close();

    // OP_UPDATE_BLOCKS 25
    final String updateBlockFile = "/update_blocks";
    FSDataOutputStream fout = filesystem.create(new Path(updateBlockFile), true, 4096, (short)1, 4096L);
    fout.write(1);
    fout.hflush();
    long fileId = ((DFSOutputStream)fout.getWrappedStream()).getFileId();
    DFSClient dfsclient = DFSClientAdapter.getDFSClient(filesystem);
    LocatedBlocks blocks = dfsclient.getNamenode().getBlockLocations(updateBlockFile, 0, Integer.MAX_VALUE);
    dfsclient.getNamenode().abandonBlock(blocks.get(0).getBlock(), fileId, updateBlockFile, dfsclient.clientName);
    fout.close();

    // OP_SET_STORAGE_POLICY 45
    filesystem.setStoragePolicy(pathFileCreate,
        HdfsConstants.HOT_STORAGE_POLICY_NAME);
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
    // OP_SET_QUOTA_BY_STORAGETYPE
    filesystem.setQuotaByStorageType(pathDirectoryMkdir, StorageType.SSD, 888L);
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

    // OP_TRUNCATE 46
    length = blockSize * 2;
    DFSTestUtil.createFile(filesystem, pathFileCreate, length, replication,
        seed);
    filesystem.truncate(pathFileCreate, blockSize);

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
    // OP_SET_XATTR
    filesystem.setXAttr(pathConcatTarget, "user.a1", 
        new byte[]{0x31, 0x32, 0x33});
    filesystem.setXAttr(pathConcatTarget, "user.a2", 
        new byte[]{0x37, 0x38, 0x39});
    // OP_REMOVE_XATTR
    filesystem.removeXAttr(pathConcatTarget, "user.a2");

    // OP_ADD_ERASURE_CODING_POLICY
    ErasureCodingPolicy newPolicy1 =
        new ErasureCodingPolicy(ErasureCodeConstants.RS_3_2_SCHEMA, 8 * 1024);
    ErasureCodingPolicy[] policyArray = new ErasureCodingPolicy[] {newPolicy1};
    AddErasureCodingPolicyResponse[] responses =
        filesystem.addErasureCodingPolicies(policyArray);
    newPolicy1 = responses[0].getPolicy();

    // OP_ADD_ERASURE_CODING_POLICY - policy with extra options
    Map<String, String> extraOptions = new HashMap<String, String>();
    extraOptions.put("dummyKey", "dummyValue");
    ECSchema schema =
        new ECSchema(ErasureCodeConstants.RS_CODEC_NAME, 6, 10, extraOptions);
    ErasureCodingPolicy newPolicy2 = new ErasureCodingPolicy(schema, 4 * 1024);
    policyArray = new ErasureCodingPolicy[] {newPolicy2};
    responses = filesystem.addErasureCodingPolicies(policyArray);
    newPolicy2 = responses[0].getPolicy();
    // OP_ENABLE_ERASURE_CODING_POLICY
    filesystem.enableErasureCodingPolicy(newPolicy1.getName());
    filesystem.enableErasureCodingPolicy(newPolicy2.getName());
    // OP_DISABLE_ERASURE_CODING_POLICY
    filesystem.disableErasureCodingPolicy(newPolicy1.getName());
    filesystem.disableErasureCodingPolicy(newPolicy2.getName());
    // OP_REMOVE_ERASURE_CODING_POLICY
    filesystem.removeErasureCodingPolicy(newPolicy1.getName());
    filesystem.removeErasureCodingPolicy(newPolicy2.getName());

    // OP_ADD on erasure coding directory
    Path ecDir = new Path("/ec");
    filesystem.mkdirs(ecDir);
    final ErasureCodingPolicy defaultEcPolicy =
        SystemErasureCodingPolicies.getByID(
            SystemErasureCodingPolicies.RS_6_3_POLICY_ID);
    final ErasureCodingPolicy ecPolicyRS32 =
        SystemErasureCodingPolicies.getByID(
            SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
    filesystem.enableErasureCodingPolicy(ecPolicyRS32.getName());
    filesystem.enableErasureCodingPolicy(defaultEcPolicy.getName());
    filesystem.setErasureCodingPolicy(ecDir, defaultEcPolicy.getName());

    try (FSDataOutputStream out = filesystem.createFile(
        new Path(ecDir, "replicated")).replicate().build()) {
      out.write("replicated".getBytes());
    }

    try (FSDataOutputStream out = filesystem
        .createFile(new Path(ecDir, "RS-3-2"))
        .ecPolicyName(ecPolicyRS32.getName()).blockSize(1024 * 1024).build()) {
      out.write("RS-3-2".getBytes());
    }
  }

  public static void abortStream(DFSOutputStream out) throws IOException {
    out.abort();
  }

  public static void setPipeline(DFSOutputStream out, LocatedBlock lastBlock)
      throws IOException {
    out.getStreamer().setPipelineInConstruction(lastBlock);
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
    }, 100, 120000);
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
      conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
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

  /**
   * Verify that two files have the same contents.
   *
   * @param fs The file system containing the two files.
   * @param p1 The path of the first file.
   * @param p2 The path of the second file.
   * @param len The length of the two files.
   * @throws IOException
   */
  public static void verifyFilesEqual(FileSystem fs, Path p1, Path p2, int len)
      throws IOException {
    try (FSDataInputStream in1 = fs.open(p1);
         FSDataInputStream in2 = fs.open(p2)) {
      for (int i = 0; i < len; i++) {
        assertEquals("Mismatch at byte " + i, in1.read(), in2.read());
      }
    }
  }

  /**
   * Verify that two files have different contents.
   *
   * @param fs The file system containing the two files.
   * @param p1 The path of the first file.
   * @param p2 The path of the second file.
   * @param len The length of the two files.
   * @throws IOException
   */
  public static void verifyFilesNotEqual(FileSystem fs, Path p1, Path p2,
      int len) throws IOException {
    try (FSDataInputStream in1 = fs.open(p1);
         FSDataInputStream in2 = fs.open(p2)) {
      for (int i = 0; i < len; i++) {
        if (in1.read() != in2.read()) {
          return;
        }
      }
      fail("files are equal, but should not be");
    }
  }

  /**
   * Helper function that verified blocks of a file are placed on the
   * expected storage type.
   *
   * @param fs The file system containing the the file.
   * @param client The DFS client used to access the file
   * @param path name to the file to verify
   * @param storageType expected storage type
   * @returns true if file exists and its blocks are located on the expected
   *            storage type.
   *          false otherwise.
   */
  public static boolean verifyFileReplicasOnStorageType(FileSystem fs,
    DFSClient client, Path path, StorageType storageType) throws IOException {
    if (!fs.exists(path)) {
      LOG.info("verifyFileReplicasOnStorageType: file " + path + "does not exist");
      return false;
    }
    long fileLength = client.getFileInfo(path.toString()).getLen();
    LocatedBlocks locatedBlocks =
      client.getLocatedBlocks(path.toString(), 0, fileLength);
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      if (locatedBlock.getStorageTypes()[0] != storageType) {
        LOG.info("verifyFileReplicasOnStorageType: for file " + path +
            ". Expect blk" + locatedBlock +
          " on Type: " + storageType + ". Actual Type: " +
          locatedBlock.getStorageTypes()[0]);
        return false;
      }
    }
    return true;
  }

  /**
   * Verify the aggregated {@link ClientProtocol#getStats()} block counts equal
   * the sum of {@link ClientProtocol#getReplicatedBlockStats()} and
   * {@link ClientProtocol#getECBlockGroupStats()}.
   * @throws Exception
   */
  public static  void verifyClientStats(Configuration conf,
      MiniDFSCluster cluster) throws Exception {
    ClientProtocol client = NameNodeProxies.createProxy(conf,
        cluster.getFileSystem(0).getUri(),
        ClientProtocol.class).getProxy();
    long[] aggregatedStats = cluster.getNameNode().getRpcServer().getStats();
    ReplicatedBlockStats replicatedBlockStats =
        client.getReplicatedBlockStats();
    ECBlockGroupStats ecBlockGroupStats = client.getECBlockGroupStats();

    assertEquals("Under replicated stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_LOW_REDUNDANCY_IDX],
        aggregatedStats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX]);
    assertEquals("Low redundancy stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_LOW_REDUNDANCY_IDX],
        replicatedBlockStats.getLowRedundancyBlocks() +
            ecBlockGroupStats.getLowRedundancyBlockGroups());
    assertEquals("Corrupt blocks stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX],
        replicatedBlockStats.getCorruptBlocks() +
            ecBlockGroupStats.getCorruptBlockGroups());
    assertEquals("Missing blocks stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX],
        replicatedBlockStats.getMissingReplicaBlocks() +
            ecBlockGroupStats.getMissingBlockGroups());
    assertEquals("Missing blocks with replication factor one not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX],
        replicatedBlockStats.getMissingReplicationOneBlocks());
    assertEquals("Bytes in future blocks stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_BYTES_IN_FUTURE_BLOCKS_IDX],
        replicatedBlockStats.getBytesInFutureBlocks() +
            ecBlockGroupStats.getBytesInFutureBlockGroups());
    assertEquals("Pending deletion blocks stats not matching!",
        aggregatedStats[ClientProtocol.GET_STATS_PENDING_DELETION_BLOCKS_IDX],
        replicatedBlockStats.getPendingDeletionBlocks() +
            ecBlockGroupStats.getPendingDeletionBlocks());
  }

  /**
   * Helper function to create a key in the Key Provider. Defaults
   * to the first indexed NameNode's Key Provider.
   *
   * @param keyName The name of the key to create
   * @param cluster The cluster to create it in
   * @param conf Configuration to use
   */
  public static void createKey(String keyName, MiniDFSCluster cluster,
                                Configuration conf)
          throws NoSuchAlgorithmException, IOException {
    createKey(keyName, cluster, 0, conf);
  }

  /**
   * Helper function to create a key in the Key Provider.
   *
   * @param keyName The name of the key to create
   * @param cluster The cluster to create it in
   * @param idx The NameNode index
   * @param conf Configuration to use
   */
  public static void createKey(String keyName, MiniDFSCluster cluster,
                               int idx, Configuration conf)
      throws NoSuchAlgorithmException, IOException {
    NameNode nn = cluster.getNameNode(idx);
    KeyProvider provider = nn.getNamesystem().getProvider();
    final KeyProvider.Options options = KeyProvider.options(conf);
    options.setDescription(keyName);
    options.setBitLength(128);
    provider.createKey(keyName, options);
    provider.flush();
  }

  /**
   * @return the node which is expected to run the recovery of the
   * given block, which is known to be under construction inside the
   * given NameNOde.
   */
  public static DatanodeDescriptor getExpectedPrimaryNode(NameNode nn,
      ExtendedBlock blk) {
    BlockManager bm0 = nn.getNamesystem().getBlockManager();
    BlockInfo storedBlock = bm0.getStoredBlock(blk.getLocalBlock());
    assertTrue("Block " + blk + " should be under construction, " +
        "got: " + storedBlock, !storedBlock.isComplete());
    // We expect that the replica with the most recent heart beat will be
    // the one to be in charge of the synchronization / recovery protocol.
    final DatanodeStorageInfo[] storages = storedBlock
        .getUnderConstructionFeature().getExpectedStorageLocations();
    DatanodeStorageInfo expectedPrimary = storages[0];
    long mostRecentLastUpdate = expectedPrimary.getDatanodeDescriptor()
        .getLastUpdateMonotonic();
    for (int i = 1; i < storages.length; i++) {
      final long lastUpdate = storages[i].getDatanodeDescriptor()
          .getLastUpdateMonotonic();
      if (lastUpdate > mostRecentLastUpdate) {
        expectedPrimary = storages[i];
        mostRecentLastUpdate = lastUpdate;
      }
    }
    return expectedPrimary.getDatanodeDescriptor();
  }

  public static void toolRun(Tool tool, String cmd, int retcode, String contain)
      throws Exception {
    String [] cmds = StringUtils.split(cmd, ' ');
    System.out.flush();
    System.err.flush();
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    String output = null;
    int ret = 0;
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream(1024);
      try (PrintStream out = new PrintStream(bs)) {
        System.setOut(out);
        System.setErr(out);
        ret = tool.run(cmds);
        System.out.flush();
        System.err.flush();
      }
      output = bs.toString();
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    System.out.println("Output for command: " + cmd + " retcode: " + ret);
    if (output != null) {
      System.out.println(output);
    }
    assertEquals(retcode, ret);
    if (contain != null) {
      assertTrue("The real output is: " + output + ".\n It should contain: "
          + contain, output.contains(contain));
    }
  }

  public static void FsShellRun(String cmd, int retcode, String contain,
      Configuration conf) throws Exception {
    FsShell shell = new FsShell(new Configuration(conf));
    toolRun(shell, cmd, retcode, contain);
  }  

  public static void DFSAdminRun(String cmd, int retcode, String contain,
      Configuration conf) throws Exception {
    DFSAdmin admin = new DFSAdmin(new Configuration(conf));
    toolRun(admin, cmd, retcode, contain);
  }

  public static void FsShellRun(String cmd, Configuration conf)
      throws Exception {
    FsShellRun(cmd, 0, null, conf);
  }

  public static void addDataNodeLayoutVersion(final int lv, final String description)
      throws NoSuchFieldException, IllegalAccessException {
    Preconditions.checkState(lv < DataNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

    // Override {@link DataNodeLayoutVersion#CURRENT_LAYOUT_VERSION} via reflection.
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    Field field = DataNodeLayoutVersion.class.getField("CURRENT_LAYOUT_VERSION");
    field.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.setInt(null, lv);

    field = HdfsServerConstants.class.getField("DATANODE_LAYOUT_VERSION");
    field.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.setInt(null, lv);

    // Inject the feature into the FEATURES map.
    final LayoutVersion.FeatureInfo featureInfo =
        new LayoutVersion.FeatureInfo(lv, lv + 1, description, false);
    final LayoutVersion.LayoutFeature feature =
        new LayoutVersion.LayoutFeature() {
      @Override
      public LayoutVersion.FeatureInfo getInfo() {
        return featureInfo;
      }
    };

    // Update the FEATURES map with the new layout version.
    LayoutVersion.updateMap(DataNodeLayoutVersion.FEATURES,
                            new LayoutVersion.LayoutFeature[] { feature });
  }

  /**
   * Wait for datanode to reach alive or dead state for waitTime given in
   * milliseconds.
   */
  public static void waitForDatanodeState(
      final MiniDFSCluster cluster, final String nodeID,
      final boolean alive, int waitTime)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        FSNamesystem namesystem = cluster.getNamesystem();
        final DatanodeDescriptor dd = BlockManagerTestUtil.getDatanode(
            namesystem, nodeID);
        return (dd.isAlive() == alive);
      }
    }, 100, waitTime);
  }

  /**
   * Change the length of a block at datanode dnIndex.
   */
  public static boolean changeReplicaLength(MiniDFSCluster cluster,
      ExtendedBlock blk, int dnIndex, int lenDelta) throws IOException {
    File blockFile = cluster.getBlockFile(dnIndex, blk);
    if (blockFile != null && blockFile.exists()) {
      try (RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw")) {
        raFile.setLength(raFile.length() + lenDelta);
      }
      return true;
    }
    LOG.info("failed to change length of block " + blk);
    return false;
  }

  public static void setNameNodeLogLevel(Level level) {
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, level);
    GenericTestUtils.setLogLevel(BlockManager.LOG, level);
    GenericTestUtils.setLogLevel(LeaseManager.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, level);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, level);
  }

  public static void setNameNodeLogLevel(org.slf4j.event.Level level) {
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, level);
    GenericTestUtils.setLogLevel(BlockManager.LOG, level);
    GenericTestUtils.setLogLevel(LeaseManager.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.LOG, level);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, level);
    GenericTestUtils.setLogLevel(NameNode.blockStateChangeLog, level);
  }

  /**
   * Get the NamenodeProtocol RPC proxy for the NN associated with this
   * DFSClient object
   *
   * @param nameNodeUri the URI of the NN to get a proxy for.
   *
   * @return the Namenode RPC proxy associated with this DFSClient object
   */
  @VisibleForTesting
  public static NamenodeProtocol getNamenodeProtocolProxy(Configuration conf,
      URI nameNodeUri, UserGroupInformation ugi)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(conf,
        DFSUtilClient.getNNAddress(nameNodeUri), NamenodeProtocol.class, ugi,
        false).getProxy();
  }

  /**
   * Get the RefreshUserMappingsProtocol RPC proxy for the NN associated with
   * this DFSClient object
   *
   * @param nnAddr the address of the NN to get a proxy for.
   *
   * @return the RefreshUserMappingsProtocol RPC proxy associated with this
   * DFSClient object
   */
  @VisibleForTesting
  public static RefreshUserMappingsProtocol getRefreshUserMappingsProtocolProxy(
      Configuration conf, InetSocketAddress nnAddr) throws IOException {
    return NameNodeProxies.createNonHAProxy(
        conf, nnAddr, RefreshUserMappingsProtocol.class,
        UserGroupInformation.getCurrentUser(), false).getProxy();
  }

  /**
   * Set the datanode dead
   */
  public static void setDatanodeDead(DatanodeInfo dn) {
    dn.setLastUpdate(0);
    // Set this to a large negative value.
    // On short-lived VMs, the monotonic time can be less than the heartbeat
    // expiry time. Setting this to 0 will fail to immediately mark the DN as
    // dead.
    dn.setLastUpdateMonotonic(Long.MIN_VALUE/2);
  }

  /**
   * Update lastUpdate and lastUpdateMonotonic with some offset.
   */
  public static void resetLastUpdatesWithOffset(DatanodeInfo dn, long offset) {
    dn.setLastUpdate(Time.now() + offset);
    dn.setLastUpdateMonotonic(Time.monotonicNow() + offset);
  }
  
  /**
   * This method takes a set of block locations and fills the provided buffer
   * with expected bytes based on simulated content from
   * {@link SimulatedFSDataset}.
   *
   * @param lbs The block locations of a file
   * @param expected The buffer to be filled with expected bytes on the above
   *                 locations.
   */
  public static void fillExpectedBuf(LocatedBlocks lbs, byte[] expected) {
    Block[] blks = new Block[lbs.getLocatedBlocks().size()];
    for (int i = 0; i < lbs.getLocatedBlocks().size(); i++) {
      blks[i] = lbs.getLocatedBlocks().get(i).getBlock().getLocalBlock();
    }
    int bufPos = 0;
    for (Block b : blks) {
      for (long blkPos = 0; blkPos < b.getNumBytes(); blkPos++) {
        assert bufPos < expected.length;
        expected[bufPos++] = SimulatedFSDataset.simulatedByte(b, blkPos);
      }
    }
  }

  public static StorageReceivedDeletedBlocks[] makeReportForReceivedBlock(
      Block block, BlockStatus blockStatus, DatanodeStorage storage) {
    ReceivedDeletedBlockInfo[] receivedBlocks = new ReceivedDeletedBlockInfo[1];
    receivedBlocks[0] = new ReceivedDeletedBlockInfo(block, blockStatus, null);
    StorageReceivedDeletedBlocks[] reports = new StorageReceivedDeletedBlocks[1];
    reports[0] = new StorageReceivedDeletedBlocks(storage, receivedBlocks);
    return reports;
  }

  /**
   * Creates the metadata of a file in striped layout. This method only
   * manipulates the NameNode state without injecting data to DataNode.
   * You should disable periodical heartbeat before use this.
   * @param file Path of the file to create
   * @param dir Parent path of the file
   * @param numBlocks Number of striped block groups to add to the file
   * @param numStripesPerBlk Number of striped cells in each block
   * @param toMkdir
   */
  public static void createStripedFile(MiniDFSCluster cluster, Path file,
      Path dir, int numBlocks, int numStripesPerBlk, boolean toMkdir)
      throws Exception {
    createStripedFile(cluster, file, dir, numBlocks, numStripesPerBlk,
        toMkdir, StripedFileTestUtil.getDefaultECPolicy());
  }

  /**
   * Creates the metadata of a file in striped layout. This method only
   * manipulates the NameNode state without injecting data to DataNode.
   * You should disable periodical heartbeat before use this.
   * @param file Path of the file to create
   * @param dir Parent path of the file
   * @param numBlocks Number of striped block groups to add to the file
   * @param numStripesPerBlk Number of striped cells in each block
   * @param toMkdir
   * @param ecPolicy erasure coding policy apply to created file. A null value
   *                 means using default erasure coding policy.
   */
  public static void createStripedFile(MiniDFSCluster cluster, Path file,
      Path dir, int numBlocks, int numStripesPerBlk, boolean toMkdir,
      ErasureCodingPolicy ecPolicy) throws Exception {
    DistributedFileSystem dfs = cluster.getFileSystem();
    // If outer test already set EC policy, dir should be left as null
    if (toMkdir) {
      assert dir != null;
      dfs.mkdirs(dir);
      try {
        dfs.getClient()
            .setErasureCodingPolicy(dir.toString(), ecPolicy.getName());
      } catch (IOException e) {
        if (!e.getMessage().contains("non-empty directory")) {
          throw e;
        }
      }
    }

    cluster.getNameNodeRpc()
        .create(file.toString(), new FsPermission((short)0755),
        dfs.getClient().getClientName(),
        new EnumSetWritable<>(EnumSet.of(CreateFlag.CREATE)),
            false, (short) 1, 128 * 1024 * 1024L, null, null, null);

    FSNamesystem ns = cluster.getNamesystem();
    FSDirectory fsdir = ns.getFSDirectory();
    INodeFile fileNode = fsdir.getINode4Write(file.toString()).asFile();

    ExtendedBlock previous = null;
    for (int i = 0; i < numBlocks; i++) {
      Block newBlock = addBlockToFile(true, cluster.getDataNodes(), dfs, ns,
          file.toString(), fileNode, dfs.getClient().getClientName(),
          previous, numStripesPerBlk, 0);
      previous = new ExtendedBlock(ns.getBlockPoolId(), newBlock);
    }

    dfs.getClient().namenode.complete(file.toString(),
        dfs.getClient().getClientName(), previous, fileNode.getId());
  }

  /**
   * Adds a block or a striped block group to a file.
   * This method only manipulates NameNode
   * states of the file and the block without injecting data to DataNode.
   * It does mimic block reports.
   * You should disable periodical heartbeat before use this.
   * @param isStripedBlock a boolean tell if the block added a striped block
   * @param dataNodes List DataNodes to host the striped block group
   * @param previous Previous block in the file
   * @param numStripes Number of stripes in each block group
   * @param len block size for a non striped block added
   * @return The added block or block group
   */
  public static Block addBlockToFile(boolean isStripedBlock,
      List<DataNode> dataNodes, DistributedFileSystem fs, FSNamesystem ns,
      String file, INodeFile fileNode,
      String clientName, ExtendedBlock previous, int numStripes, int len)
      throws Exception {
    fs.getClient().namenode.addBlock(file, clientName, previous, null,
        fileNode.getId(), null, null);

    final BlockInfo lastBlock = fileNode.getLastBlock();
    final int groupSize = fileNode.getPreferredBlockReplication();
    assert dataNodes.size() >= groupSize;
    // 1. RECEIVING_BLOCK IBR
    for (int i = 0; i < groupSize; i++) {
      DataNode dn = dataNodes.get(i);
      final Block block = new Block(lastBlock.getBlockId() + i, 0,
          lastBlock.getGenerationStamp());
      DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
      StorageReceivedDeletedBlocks[] reports = DFSTestUtil
          .makeReportForReceivedBlock(block,
              ReceivedDeletedBlockInfo.BlockStatus.RECEIVING_BLOCK, storage);
      for (StorageReceivedDeletedBlocks report : reports) {
        ns.processIncrementalBlockReport(dn.getDatanodeId(), report);
      }
    }

    final ErasureCodingPolicy ecPolicy =
        fs.getErasureCodingPolicy(new Path(file));
    // 2. RECEIVED_BLOCK IBR
    long blockSize = isStripedBlock ?
        numStripes * ecPolicy.getCellSize() : len;
    for (int i = 0; i < groupSize; i++) {
      DataNode dn = dataNodes.get(i);
      final Block block = new Block(lastBlock.getBlockId() + i,
          blockSize, lastBlock.getGenerationStamp());
      DatanodeStorage storage = new DatanodeStorage(UUID.randomUUID().toString());
      StorageReceivedDeletedBlocks[] reports = DFSTestUtil
          .makeReportForReceivedBlock(block,
              ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, storage);
      for (StorageReceivedDeletedBlocks report : reports) {
        ns.processIncrementalBlockReport(dn.getDatanodeId(), report);
      }
    }
    long bytes = isStripedBlock ?
        numStripes * ecPolicy.getCellSize() * ecPolicy.getNumDataUnits() : len;
    lastBlock.setNumBytes(bytes);
    return lastBlock;
  }

  /*
   * Copy a block from sourceProxy to destination. If the block becomes
   * over-replicated, preferably remove it from source.
   * Return true if a block is successfully copied; otherwise false.
   */
  public static boolean replaceBlock(ExtendedBlock block, DatanodeInfo source,
      DatanodeInfo sourceProxy, DatanodeInfo destination) throws IOException {
    return replaceBlock(block, source, sourceProxy, destination,
        StorageType.DEFAULT, Status.SUCCESS);
  }

  /*
   * Replace block
   */
  public static boolean replaceBlock(ExtendedBlock block, DatanodeInfo source,
      DatanodeInfo sourceProxy, DatanodeInfo destination,
      StorageType targetStorageType, Status opStatus) throws IOException,
      SocketException {
    Socket sock = new Socket();
    try {
      sock.connect(NetUtils.createSocketAddr(destination.getXferAddr()),
          HdfsConstants.READ_TIMEOUT);
      sock.setKeepAlive(true);
      // sendRequest
      DataOutputStream out = new DataOutputStream(sock.getOutputStream());
      new Sender(out).replaceBlock(block, targetStorageType,
          BlockTokenSecretManager.DUMMY_TOKEN, source.getDatanodeUuid(),
          sourceProxy, null);
      out.flush();
      // receiveResponse
      DataInputStream reply = new DataInputStream(sock.getInputStream());

      BlockOpResponseProto proto = BlockOpResponseProto.parseDelimitedFrom(
          reply);
      while (proto.getStatus() == Status.IN_PROGRESS) {
        proto = BlockOpResponseProto.parseDelimitedFrom(reply);
      }
      return proto.getStatus() == opStatus;
    } finally {
      sock.close();
    }
  }

  /**
   * Because currently DFSStripedOutputStream does not support hflush/hsync,
   * tests can use this method to flush all the buffered data to DataNodes.
   */
  public static ExtendedBlock flushInternal(DFSStripedOutputStream out)
      throws IOException {
    out.flushAllInternals();
    return out.getBlock();
  }

  public static ExtendedBlock flushBuffer(DFSStripedOutputStream out)
      throws IOException {
    out.flush();
    return out.getBlock();
  }

  public static void waitForMetric(final JMXGet jmx, final String metricName, final int expectedValue)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          final int currentValue = Integer.parseInt(jmx.getValue(metricName));
          return currentValue == expectedValue;
        } catch (Exception e) {
          throw new RuntimeException(
              "Test failed due to unexpected exception", e);
        }
      }
    }, 50, 60000);
  }

  /**
   * Close current file system and create a new instance as given
   * {@link UserGroupInformation}.
   */
  public static FileSystem login(final FileSystem fs,
      final Configuration conf, final UserGroupInformation ugi)
          throws IOException, InterruptedException {
    if (fs != null) {
      fs.close();
    }
    return DFSTestUtil.getFileSystemAs(ugi, conf);
  }

  /**
   * Test if the given {@link FileStatus} user, group owner and its permission
   * are expected, throw {@link AssertionError} if any value is not expected.
   */
  public static void verifyFilePermission(FileStatus stat, String owner,
      String group, FsAction u, FsAction g, FsAction o) {
    if(stat != null) {
      if(!Strings.isNullOrEmpty(owner)) {
        assertEquals(owner, stat.getOwner());
      }
      if(!Strings.isNullOrEmpty(group)) {
        assertEquals(group, stat.getGroup());
      }
      FsPermission permission = stat.getPermission();
      if(u != null) {
        assertEquals(u, permission.getUserAction());
      }
      if (g != null) {
        assertEquals(g, permission.getGroupAction());
      }
      if (o != null) {
        assertEquals(o, permission.getOtherAction());
      }
    }
  }

  public static void verifyDelete(FsShell shell, FileSystem fs, Path path,
      boolean shouldExistInTrash) throws Exception {
    Path trashPath = Path.mergePaths(shell.getCurrentTrashDir(path), path);

    verifyDelete(shell, fs, path, trashPath, shouldExistInTrash);
  }

  public static void verifyDelete(FsShell shell, FileSystem fs, Path path,
      Path trashPath, boolean shouldExistInTrash) throws Exception {
    assertTrue(path + " file does not exist", fs.exists(path));

    // Verify that trashPath has a path component named ".Trash"
    Path checkTrash = trashPath;
    while (!checkTrash.isRoot() && !checkTrash.getName().equals(".Trash")) {
      checkTrash = checkTrash.getParent();
    }
    assertEquals("No .Trash component found in trash path " + trashPath,
        ".Trash", checkTrash.getName());

    String[] argv = new String[]{"-rm", "-r", path.toString()};
    int res = ToolRunner.run(shell, argv);
    assertEquals("rm failed", 0, res);
    if (shouldExistInTrash) {
      assertTrue("File not in trash : " + trashPath, fs.exists(trashPath));
    } else {
      assertFalse("File in trash : " + trashPath, fs.exists(trashPath));
    }
  }

  /**
   * Create open files under root path.
   * @param fs the filesystem.
   * @param filePrefix the prefix of the files.
   * @param numFilesToCreate the number of files to create.
   */
  public static Map<Path, FSDataOutputStream> createOpenFiles(FileSystem fs,
      String filePrefix, int numFilesToCreate) throws IOException {
    return createOpenFiles(fs, new Path("/"), filePrefix, numFilesToCreate);
  }

  /**
   * Create open files.
   * @param fs the filesystem.
   * @param baseDir the base path of the files.
   * @param filePrefix the prefix of the files.
   * @param numFilesToCreate the number of files to create.
   */
  public static Map<Path, FSDataOutputStream> createOpenFiles(FileSystem fs,
      Path baseDir, String filePrefix, int numFilesToCreate)
      throws IOException {
    final Map<Path, FSDataOutputStream> filesCreated = new HashMap<>();
    final byte[] buffer = new byte[(int) (1024 * 1.75)];
    final Random rand = new Random(0xFEED0BACL);
    for (int i = 0; i < numFilesToCreate; i++) {
      Path file = new Path(baseDir, filePrefix + "-" + i);
      FSDataOutputStream stm = fs.create(file, true, 1024, (short) 1, 1024);
      rand.nextBytes(buffer);
      stm.write(buffer);
      filesCreated.put(file, stm);
    }
    return filesCreated;
  }

  public static HashSet<Path> closeOpenFiles(
      HashMap<Path, FSDataOutputStream> openFilesMap,
      int numFilesToClose) throws IOException {
    HashSet<Path> closedFiles = new HashSet<>();
    for (Iterator<Entry<Path, FSDataOutputStream>> it =
         openFilesMap.entrySet().iterator(); it.hasNext();) {
      Entry<Path, FSDataOutputStream> entry = it.next();
      LOG.info("Closing file: " + entry.getKey());
      entry.getValue().close();
      closedFiles.add(entry.getKey());
      it.remove();
      numFilesToClose--;
      if (numFilesToClose == 0) {
        break;
      }
    }
    return closedFiles;
  }

  /**
   * Setup cluster with desired number of DN, racks, and specified number of
   * rack that only has 1 DN. Other racks will be evenly setup with the number
   * of DNs.
   *
   * @param conf the conf object to start the cluster.
   * @param numDatanodes number of total Datanodes.
   * @param numRacks number of total racks
   * @param numSingleDnRacks number of racks that only has 1 DN
   * @throws Exception
   */
  public static MiniDFSCluster setupCluster(final Configuration conf,
                                            final int numDatanodes,
                                            final int numRacks,
                                            final int numSingleDnRacks)
      throws Exception {
    assert numDatanodes > numRacks;
    assert numRacks > numSingleDnRacks;
    assert numSingleDnRacks >= 0;
    final String[] racks = new String[numDatanodes];
    for (int i = 0; i < numSingleDnRacks; i++) {
      racks[i] = "/rack" + i;
    }
    for (int i = numSingleDnRacks; i < numDatanodes; i++) {
      racks[i] =
          "/rack" + (numSingleDnRacks + (i % (numRacks - numSingleDnRacks)));
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .racks(racks)
        .build();
    cluster.waitActive();
    return cluster;
  }

  /**
   * Check the correctness of the snapshotDiff report.
   * Make sure all items in the passed entries are in the snapshotDiff
   * report.
   */
  public static void verifySnapshotDiffReport(DistributedFileSystem fs,
      Path dir, String from, String to,
      DiffReportEntry... entries) throws IOException {
    SnapshotDiffReport report = fs.getSnapshotDiffReport(dir, from, to);
    // reverse the order of from and to
    SnapshotDiffReport inverseReport = fs
        .getSnapshotDiffReport(dir, to, from);
    LOG.info(report.toString());
    LOG.info(inverseReport.toString() + "\n");

    assertEquals(entries.length, report.getDiffList().size());
    assertEquals(entries.length, inverseReport.getDiffList().size());

    for (DiffReportEntry entry : entries) {
      if (entry.getType() == DiffType.MODIFY) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(entry));
      } else if (entry.getType() == DiffType.DELETE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.CREATE, entry.getSourcePath())));
      } else if (entry.getType() == DiffType.CREATE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.DELETE, entry.getSourcePath())));
      }
    }
  }

  /**
   * Check whether the Block movement has been successfully
   * completed to satisfy the storage policy for the given file.
   * @param fileName file name.
   * @param expectedStorageType storage type.
   * @param expectedStorageCount expected storage type.
   * @param timeout timeout.
   * @param fs distributedFileSystem.
   * @throws Exception
   */
  public static void waitExpectedStorageType(String fileName,
      final StorageType expectedStorageType, int expectedStorageCount,
      int timeout, DistributedFileSystem fs) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        final LocatedBlock lb;
        try {
          lb = fs.getClient().getLocatedBlocks(fileName, 0).get(0);
        } catch (IOException e) {
          LOG.error("Exception while getting located blocks", e);
          return false;
        }
        int actualStorageCount = 0;
        for(StorageType type : lb.getStorageTypes()) {
          if (expectedStorageType == type) {
            actualStorageCount++;
          }
        }
        LOG.info(
            expectedStorageType + " replica count, expected="
                + expectedStorageCount + " and actual=" + actualStorageCount);
        return expectedStorageCount == actualStorageCount;
      }
    }, 500, timeout);
  }

  /**
   * Waits for removal of a specified Xattr on a specified file.
   *
   * @param srcPath
   *          file name.
   * @param xattr
   *          name of the extended attribute.
   * @param ns
   *          Namesystem
   * @param timeout
   *          max wait time
   * @throws Exception
   */
  public static void waitForXattrRemoved(String srcPath, String xattr,
      Namesystem ns, int timeout) throws TimeoutException, InterruptedException,
          UnresolvedLinkException, AccessControlException,
          ParentNotDirectoryException {
    final INode inode = ns.getFSDirectory().getINode(srcPath);
    final XAttr satisfyXAttr = XAttrHelper.buildXAttr(xattr);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<XAttr> existingXAttrs = XAttrStorage.readINodeXAttrs(inode);
        return !existingXAttrs.contains(satisfyXAttr);
      }
    }, 100, timeout);
  }

  /**
   * Get namenode connector using the given configuration and file path.
   *
   * @param conf
   *          hdfs configuration
   * @param filePath
   *          file path
   * @param namenodeCount
   *          number of namenodes
   * @param createMoverPath
   *          create move path flag to skip the path creation
   * @return Namenode connector.
   * @throws IOException
   */
  public static NameNodeConnector getNameNodeConnector(Configuration conf,
      Path filePath, int namenodeCount, boolean createMoverPath)
          throws IOException {
    final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    Assert.assertEquals(namenodeCount, namenodes.size());
    NameNodeConnector.checkOtherInstanceRunning(createMoverPath);
    while (true) {
      try {
        final List<NameNodeConnector> nncs = NameNodeConnector
            .newNameNodeConnectors(namenodes,
                StoragePolicySatisfier.class.getSimpleName(),
                filePath, conf,
                NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);
        return nncs.get(0);
      } catch (IOException e) {
        LOG.warn("Failed to connect with namenode", e);
        // Ignore
      }
    }
  }

  /**
   * Run the fsck command using the specified params.
   *
   * @param conf HDFS configuration to use
   * @param expectedErrCode The error code expected to be returned by
   *                         the fsck command
   * @param checkErrorCode Should the error code be checked
   * @param path actual arguments to the fsck command
   **/
  public static String runFsck(Configuration conf, int expectedErrCode,
                        boolean checkErrorCode, String... path)
          throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    if (checkErrorCode) {
      assertEquals(expectedErrCode, errCode);
    }
    return bStream.toString();
  }
}
