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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.MountInfo;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.InMemoryLevelDBAliasMapClient;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.LevelDBFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.NamenodeInMemoryAliasMapClient;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import static org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PROVIDED_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PROVIDED_VOLUME_READ_THROUGH;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_UGI_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_MOUNT_INODES_MAX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.BlockResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FSTreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FixedBlockResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FsUGIResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.SimpleReadCacheManager;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.SingleUGIResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreePath;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.TreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.UGIResolver;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.RemoteMountUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LEVELDB_PATH;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_OVERREPLICATION_FACTOR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;
import static org.apache.hadoop.hdfs.MiniDFSCluster.configureInMemoryAliasMapAddresses;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CACHE_READTHROUGH;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;
import static org.apache.hadoop.net.NodeBase.PATH_SEPARATOR_STR;
import static org.junit.Assert.*;

/**
 * Integration tests for the Provided implementation.
 */
public class ITestProvidedImplementation {

  @Rule public TestName name = new TestName();
  public static final Logger LOG =
      LoggerFactory.getLogger(ITestProvidedImplementation.class);

  private final Random r = new Random();
  private final File fBASE = GenericTestUtils.getRandomizedTestDir();
  private final Path pBASE = new Path(fBASE.toURI().toString());
  private final Path providedPath = new Path(pBASE, "providedDir");
  private final Path nnDirPath = new Path(pBASE, "nnDir");
  private final String singleUser = "usr1";
  private final String singleGroup = "grp1";
  private final int numFiles = 10;
  private final String filePrefix = "file";
  private final String fileSuffix = ".dat";
  private final int baseFileLen = 1024;
  private long providedDataSize = 0;
  private final String bpid = "BP-1234-10.1.1.1-1224";
  private static final String clusterID = "CID-PROVIDED";

  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void setSeed() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
    conf = new HdfsConfiguration();
    conf.set(HDFS_MINIDFS_BASEDIR, fBASE.getAbsolutePath());
    conf.set(SingleUGIResolver.USER, singleUser);
    conf.set(SingleUGIResolver.GROUP, singleGroup);

    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, true);

    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR,
        nnDirPath.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
        new Path(nnDirPath, fileNameFromBlockPoolID(bpid)).toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER, "\t");

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(providedPath.toUri()).toString());
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_PROVIDED_VOLUME_LAZY_LOAD, true);

    File imageDir = new File(providedPath.toUri());
    if (!imageDir.exists()) {
      LOG.info("Creating directory: " + imageDir);
      imageDir.mkdirs();
    }

    File nnDir = new File(nnDirPath.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }

    providedDataSize = createFiles(providedPath, numFiles, baseFileLen,
        filePrefix, fileSuffix);
  }

  /**
   * Create the specified number of files in the specified path. Files are
   * created with the name that is a concatenation of {@code filePrefix}, the
   * file index (0 to {@code numFiles} -1) and the {@code fileSuffix}, in that
   * order.
   *
   * @param dir the path to create the files in (assumes that path exists).
   * @param numFiles number of files to create.
   * @param baseFileLen base length of each file.
   * @param filePrefix file name prefix.
   * @param fileSuffix file name suffix.
   */
  static long createFiles(Path dir, int numFiles, long baseFileLen,
      String filePrefix, String fileSuffix) throws IOException {
    long totalDataSize = 0;
    for (int i = 0; i < numFiles; i++) {
      File newFile = new File(
          new Path(dir, filePrefix + i + fileSuffix).toUri());
      long currFileSize = baseFileLen * i;
      totalDataSize += currFileSize;
      if (!newFile.exists()) {
        createLocalFile(newFile, currFileSize);
      }
    }
    return totalDataSize;
  }

  private static void createLocalFile(File file, long lengthInBytes)
      throws IOException {
    LOG.info("Creating {}", file);
    file.createNewFile();
    DataOutputStream writer =
        new DataOutputStream(new FileOutputStream(file.getAbsolutePath()));
    for (int j = 0; j < lengthInBytes; j++) {
      writer.writeByte(j);
    }
    writer.flush();
    writer.close();
  }

  @After
  public void shutdown() throws Exception {
    try {
      if (cluster != null) {
        cluster.shutdown(true, true);
      }
    } finally {
      cluster = null;
      try {
        FileUtils.deleteDirectory(fBASE);
      } catch (IOException e) {
        LOG.warn("Cleanup failed for {}; Exception: {}", fBASE.getPath(), e);
      }
    }
  }

  void createImage(TreeWalk t, Path out,
      Class<? extends BlockResolver> blockIdsClass) throws Exception {
    createImage(t, out, blockIdsClass, "", TextFileRegionAliasMap.class,
        bpid, conf);
  }

  static void createImage(TreeWalk t, Path out,
      Class<? extends BlockResolver> blockIdsClass, String clusterID,
      Class<? extends BlockAliasMap> aliasMapClass, String blockPoolID,
      Configuration config) throws Exception {
    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(config);
    opts.output(out.toString())
        .blocks(aliasMapClass)
        .blockIds(blockIdsClass)
        .clusterID(clusterID)
        .blockPoolID(blockPoolID);
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        w.accept(e);
      }
    }
  }

  static MiniDFSCluster startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, Configuration conf) throws IOException {
    return startCluster(nspath, numDatanodes, storageTypes,
        storageTypesPerDatanode, doFormat, conf, null);
  }

  static MiniDFSCluster startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, Configuration conf,
      String[] racks) throws IOException {
    return startCluster(nspath, numDatanodes,
        storageTypes, storageTypesPerDatanode,
        doFormat, conf, racks, null,
        new MiniDFSCluster.Builder(conf));
  }

  static MiniDFSCluster startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, Configuration conf, String[] racks,
      MiniDFSNNTopology topo,
      MiniDFSCluster.Builder builder) throws IOException {
    return startCluster(nspath, numDatanodes,
        storageTypes, storageTypesPerDatanode,
        doFormat, conf, racks, topo,
        builder, providedNameservice);
  }

  static MiniDFSCluster startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, Configuration conf, String[] racks,
      MiniDFSNNTopology topo,
      MiniDFSCluster.Builder builder, String providedNameservice)
      throws IOException {
    return startCluster(nspath, numDatanodes, storageTypes,
        storageTypesPerDatanode, null, doFormat, conf, racks, topo, builder,
        providedNameservice);
  }

  static MiniDFSCluster startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      long[] storageCapacities,
      boolean doFormat, Configuration conf, String[] racks,
      MiniDFSNNTopology topo,
      MiniDFSCluster.Builder builder, String providedNameservice)
      throws IOException {
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());

    builder.format(doFormat).manageNameDfsDirs(doFormat)
        .numDataNodes(numDatanodes).racks(racks);
    if (storageTypesPerDatanode != null) {
      builder.storageTypes(storageTypesPerDatanode);
    } else if (storageTypes != null) {
      builder.storagesPerDatanode(storageTypes.length)
          .storageTypes(storageTypes);
    }
    if (storageCapacities != null) {
      builder.storageCapacities(storageCapacities);
    }
    if (topo != null) {
      builder.nnTopology(topo);
      // If HA or Federation is enabled and formatting is set to false,
      // copy the FSImage to all Namenode directories.
      if ((topo.isHA() || topo.isFederated()) && !doFormat) {
        builder.manageNameDfsDirs(true);
        builder.enableManagedDfsDirsRedundancy(false);
        builder.manageNameDfsSharedDirs(true);
        List<File> nnDirs = getProvidedNamenodeDirs(
            conf.get(HDFS_MINIDFS_BASEDIR), topo, providedNameservice);
        for (File nnDir : nnDirs) {
          MiniDFSCluster.copyNameDirs(Collections.singletonList(nspath.toUri()),
              Collections.singletonList(fileAsURI(nnDir)), conf);
        }
      }
    }
    MiniDFSCluster cluster = builder.build();
    cluster.waitActive();
    return cluster;
  }

  private static List<File> getProvidedNamenodeDirs(String baseDir,
      MiniDFSNNTopology topo, String providedNameservice) {

    if (providedNameservice == null || providedNameservice.length() == 0) {
      throw new IllegalArgumentException("Provided namespace is invalid");
    }

    List<File> nnDirs = new ArrayList<>();
    int nsCounter = 0;
    for (MiniDFSNNTopology.NSConf nsConf : topo.getNameservices()) {
      int nnCounter = nsCounter;
      for (MiniDFSNNTopology.NNConf nnConf : nsConf.getNNs()) {
        if (providedNameservice.equals(nsConf.getId())) {
          // only add the first one
          File[] nnFiles = MiniDFSCluster.getNameNodeDirectory(baseDir,
              nsCounter, nnCounter);
          if (nnFiles == null || nnFiles.length == 0) {
            throw new RuntimeException("Failed to get a location for the"
                + "Namenode directory for namespace: " + nsConf.getId()
                + " and namenodeId: " + nnConf.getNnId());
          }
          nnDirs.add(nnFiles[0]);
        }
        nnCounter++;
      }
      nsCounter = nnCounter;
    }
    return nnDirs;
  }

  @Test(timeout=20000)
  public void testLoadImage() throws Exception {
    final long seed = r.nextLong();
    LOG.info("providedPath: " + providedPath);
    createImage(new RandomTreeWalk(seed), nnDirPath, FixedBlockResolver.class);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, true);
    cluster = startCluster(nnDirPath, 0,
        new StorageType[] {StorageType.DISK}, null,
        false, conf);

    FileSystem fs = cluster.getFileSystem();
    for (TreePath e : new RandomTreeWalk(seed)) {
      FileStatus rs = e.getFileStatus();
      Path hp = new Path(rs.getPath().toUri().getPath());
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);
      assertEquals(rs.getPath().toUri().getPath(),
                   hs.getPath().toUri().getPath());
      assertEquals(rs.getPermission(), hs.getPermission());
      assertEquals(rs.getLen(), hs.getLen());
      assertEquals(singleUser, hs.getOwner());
      assertEquals(singleGroup, hs.getGroup());
      assertEquals(rs.getAccessTime(), hs.getAccessTime());
      assertEquals(rs.getModificationTime(), hs.getModificationTime());
    }
  }

  @Test(timeout=30000)
  public void testProvidedReporting() throws Exception {
    // set this to false to keep track of capacity.
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_PROVIDED_VOLUME_LAZY_LOAD,
        false);
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS,
        SingleUGIResolver.class, UGIResolver.class);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    int numDatanodes = 10;
    cluster = startCluster(nnDirPath, numDatanodes,
        new StorageType[] {StorageType.DISK}, null,
        false, conf);

    long diskCapacity = 1000;
    // set the DISK capacity for testing
    for (DataNode dn: cluster.getDataNodes()) {
      for (FsVolumeSpi ref : dn.getFSDataset().getFsVolumeReferences()) {
        if (ref.getStorageType() == StorageType.DISK) {
          ((FsVolumeImpl) ref).setCapacityForTesting(diskCapacity);
        }
      }
    }
    // trigger heartbeats to update the capacities
    cluster.triggerHeartbeats();
    Thread.sleep(10000);
    // verify namenode stats
    FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
    DatanodeStatistics dnStats = namesystem.getBlockManager()
        .getDatanodeManager().getDatanodeStatistics();

    // total capacity reported includes only the local volumes and
    // not the provided capacity
    assertEquals(diskCapacity * numDatanodes, namesystem.getTotal());

    // total storage used should be equal to the totalProvidedStorage
    // no capacity should be remaining!
    assertEquals(providedDataSize, dnStats.getProvidedCapacity());
    assertEquals(providedDataSize, namesystem.getProvidedCapacityTotal());
    assertEquals(providedDataSize, dnStats.getStorageTypeStats()
        .get(StorageType.PROVIDED).getCapacityTotal());
    assertEquals(providedDataSize, dnStats.getStorageTypeStats()
        .get(StorageType.PROVIDED).getCapacityUsed());
    assertEquals(0, dnStats.getStorageTypeStats()
        .get(StorageType.PROVIDED).getCapacityRemaining());

    // verify datanode stats
    for (DataNode dn: cluster.getDataNodes()) {
      for (StorageReport report : dn.getFSDataset()
          .getStorageReports(namesystem.getBlockPoolId())) {
        if (report.getStorage().getStorageType() == StorageType.PROVIDED) {
          assertEquals(providedDataSize, report.getCapacity());
          assertEquals(providedDataSize, report.getDfsUsed());
          assertEquals(providedDataSize, report.getBlockPoolUsed());
          assertEquals(0, report.getNonDfsUsed());
          assertEquals(0, report.getRemaining());
        }
      }
    }

    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
            cluster.getNameNodePort()), cluster.getConfiguration(0));
    BlockManager bm = namesystem.getBlockManager();
    for (int fileId = 0; fileId < numFiles; fileId++) {
      String filename = "/" + filePrefix + fileId + fileSuffix;
      LocatedBlocks locatedBlocks = client.getLocatedBlocks(
          filename, 0, baseFileLen);
      for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
        BlockInfo blockInfo =
            bm.getStoredBlock(locatedBlock.getBlock().getLocalBlock());
        Iterator<DatanodeStorageInfo> storagesItr = blockInfo.getStorageInfos();

        DatanodeStorageInfo info = storagesItr.next();
        assertEquals(StorageType.PROVIDED, info.getStorageType());
        DatanodeDescriptor dnDesc = info.getDatanodeDescriptor();
        // check the locations that are returned by FSCK have the right name
        assertEquals(ProvidedStorageMap.ProvidedDescriptor.NETWORK_LOCATION
            + PATH_SEPARATOR_STR + ProvidedStorageMap.ProvidedDescriptor.NAME,
            NodeBase.getPath(dnDesc));
        // no DatanodeStorageInfos should remain
        assertFalse(storagesItr.hasNext());
      }
    }
  }

  @Test(timeout=500000)
  public void testDefaultReplication() throws Exception {
    int targetReplication = 2;
    conf.setInt(DFS_REPLICATION_KEY, targetReplication);
    conf.setInt(FixedBlockMultiReplicaResolver.REPLICATION, targetReplication);
    conf.setInt(DFS_PROVIDED_OVERREPLICATION_FACTOR_KEY, 0);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockMultiReplicaResolver.class);
    // make the last Datanode with only DISK
    cluster = startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.DISK},
            {StorageType.DISK},
            {StorageType.DISK}},
        false, conf);
    // wait for the replication to finish
    Thread.sleep(50000);

    FileSystem fs = cluster.getFileSystem();
    int count = 0;
    for (TreePath e : new FSTreeWalk(providedPath, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(providedPath, rs.getPath());
      LOG.info("path: " + hp.toUri().getPath());
      e.accept(count++);
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);

      if (rs.isFile()) {
        BlockLocation[] bl = fs.getFileBlockLocations(
            hs.getPath(), 0, hs.getLen());
        int i = 0;
        for(; i < bl.length; i++) {
          int currentRep = bl[i].getHosts().length;
          assertEquals(targetReplication, currentRep);
        }
      }
    }
  }


  static Path removePrefix(Path base, Path walk) {
    return removePrefix(base, walk, new Path("/"));
  }

  static Path removePrefix(Path base, Path walk, Path mount) {
    Path wpath = new Path(walk.toUri().getPath());
    Path bpath = new Path(base.toUri().getPath());
    Path ret = new Path("/");
    while (!(bpath.equals(wpath) || "".equals(wpath.getName()))) {
      ret = "".equals(ret.getName())
        ? new Path("/", wpath.getName())
        : new Path(new Path("/", wpath.getName()),
                   new Path(ret.toString().substring(1)));
      wpath = wpath.getParent();
    }
    if (!bpath.equals(wpath)) {
      throw new IllegalArgumentException(base + " not a prefix of " + walk);
    }
    if (!ret.equals(new Path("/"))) {
      ret = new Path(mount, new Path(ret.toString().substring(1)));
    } else {
      ret = mount;
    }
    return ret;
  }

  private void verifyFileSystemContents(int nnIndex) throws Exception {
    verifyPaths(cluster, conf, new Path("/"), providedPath, nnIndex);
  }

  static void verifyPaths(MiniDFSCluster cluster, Configuration conf,
      Path mountPath, Path remotePath) throws Exception {
    verifyPaths(cluster, conf, mountPath, remotePath, 0);
  }

  static void verifyPaths(MiniDFSCluster cluster, Configuration conf,
      Path mountPath, Path remotePath, int nnIndex) throws Exception {
    verifyPaths(cluster, conf, mountPath, remotePath, nnIndex, true);
  }

  static void verifyPaths(MiniDFSCluster cluster, Configuration conf,
      Path mountPath, Path remotePath, int nnIndex, boolean verifyFileContents)
      throws Exception {
    FileSystem fs = cluster.getFileSystem(nnIndex);
    assertTrue(fs.exists(mountPath));

    int count = 0;
    // read NN metadata, verify contents match
    for (TreePath e : new FSTreeWalk(remotePath, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(remotePath, rs.getPath(), mountPath);
      LOG.info("path: " + hp.toUri().getPath());
      e.accept(count++);
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);
      assertEquals(hp.toUri().getPath(), hs.getPath().toUri().getPath());
      assertEquals(rs.getPermission(), hs.getPermission());
      assertEquals(rs.getOwner(), hs.getOwner());
      assertEquals(rs.getGroup(), hs.getGroup());

      if (rs.isFile() && verifyFileContents) {
        verifyFileContents(fs, rs, hs);
      }
    }
  }

  static void verifyFileContents(FileSystem fs, FileStatus local,
      FileStatus remote) throws Exception {
    assertEquals(local.getLen(), remote.getLen());
    try (ReadableByteChannel i = Channels
        .newChannel(new FileInputStream(new File(local.getPath().toUri())))) {
      try (ReadableByteChannel j =
          Channels.newChannel(fs.open(remote.getPath()))) {
        ByteBuffer ib = ByteBuffer.allocate(4096);
        ByteBuffer jb = ByteBuffer.allocate(4096);
        while (true) {
          int il = i.read(ib);
          int jl = j.read(jb);
          if (il < 0 || jl < 0) {
            assertEquals(il, jl);
            break;
          }
          ib.flip();
          jb.flip();
          int cmp = Math.min(ib.remaining(), jb.remaining());
          for (int k = 0; k < cmp; ++k) {
            assertEquals(ib.get(), jb.get());
          }
          ib.compact();
          jb.compact();
        }
      }
    }
  }

  private BlockLocation[] createFile(Path path, short replication,
      long fileLen, long blockLen) throws IOException {
    FileSystem fs = cluster.getFileSystem();
    // create a file that is not provided
    DFSTestUtil.createFile(fs, path, false, (int) blockLen,
        fileLen, blockLen, replication, 0, true);
    return fs.getFileBlockLocations(path, 0, fileLen);
  }

  @Test(timeout=30000)
  public void testClusterWithEmptyImage() throws IOException {
    // start a cluster with 2 datanodes without any provided storage
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    cluster = startCluster(nnDirPath, 2, null,
        new StorageType[][] {
            {StorageType.DISK},
            {StorageType.DISK}},
        true, conf);
    assertTrue(cluster.isClusterUp());
    assertTrue(cluster.isDataNodeUp());

    BlockLocation[] locations = createFile(new Path("/testFile1.dat"),
        (short) 2, 1024*1024, 1024*1024);
    assertEquals(1, locations.length);
    assertEquals(2, locations[0].getHosts().length);
  }

  private DatanodeInfo[] getAndCheckBlockLocations(DFSClient client,
      String filename, long fileLen, long expectedBlocks, int expectedLocations)
      throws IOException {
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(filename, 0, fileLen);
    // given the start and length in the above call,
    // only one LocatedBlock in LocatedBlocks
    assertEquals(expectedBlocks, locatedBlocks.getLocatedBlocks().size());
    DatanodeInfo[] locations =
        locatedBlocks.getLocatedBlocks().get(0).getLocations();
    assertEquals(expectedLocations, locations.length);
    return locations;
  }

  /**
   * Tests setting replication of provided files.
   * @throws Exception
   */
  @Test(timeout=50000)
  public void testSetReplicationForProvidedFiles() throws Exception {
    final int replicationLimit = 2;
    conf.setInt(DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, replicationLimit);
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // 10 Datanodes with both DISK and PROVIDED storage
    cluster = startCluster(nnDirPath, 10,
        new StorageType[]{StorageType.DISK},
        null,
        false, conf);
    setAndUnsetReplication("/" + filePrefix + (numFiles - 1) + fileSuffix);
  }

  private void setAndUnsetReplication(String filename) throws Exception {
    Path file = new Path(filename);
    FileSystem fs = cluster.getFileSystem();
    // thread to make sure that the replication limit is always satisfied
    // for the datanodes.
    final int replicationLimit =
        conf.getInt(DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    Thread thread = new Thread(() -> {
      Set<DatanodeDescriptor> dns =
          cluster.getNamesystem(0).getBlockManager()
          .getDatanodeManager().getDatanodes();
      while (true) {
        for (DatanodeDescriptor dn : dns) {
          assertTrue(
              dn.getNumberOfBlocksToBeReplicated() <= replicationLimit);
        }
      }
    });
    thread.start();
    // set the replication to 4, and test that the file has
    // the required replication.
    short newReplication = 4;
    LOG.info("Setting replication of file {} to {}", filename, newReplication);
    fs.setReplication(file, newReplication);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fs,
        file, newReplication, 10000);
    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), cluster.getConfiguration(0));
    getAndCheckBlockLocations(client, filename, baseFileLen, 1, newReplication);

    // set the replication back to 1
    newReplication = 1;
    LOG.info("Setting replication of file {} back to {}",
        filename, newReplication);
    fs.setReplication(file, newReplication);
    // defaultReplication number of replicas should be returned
    int defaultReplication = conf.getInt(DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fs,
        file, (short) defaultReplication, 10000);
    getAndCheckBlockLocations(client, filename, baseFileLen, 1,
        defaultReplication);
    thread.interrupt();
  }

  @Test(timeout=30000)
  public void testProvidedDatanodeFailures() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    cluster = startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false, conf);

    DataNode providedDatanode1 = cluster.getDataNodes().get(0);
    DataNode providedDatanode2 = cluster.getDataNodes().get(1);

    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), cluster.getConfiguration(0));

    DatanodeStorageInfo providedDNInfo = getProvidedDatanodeStorageInfo();

    if (numFiles >= 1) {
      String filename = "/" + filePrefix + (numFiles - 1) + fileSuffix;
      // 2 locations returned as there are 2 PROVIDED datanodes
      DatanodeInfo[] dnInfos =
          getAndCheckBlockLocations(client, filename, baseFileLen, 1, 2);
      // the location should be one of the provided DNs available
      assertTrue(
          dnInfos[0].getDatanodeUuid().equals(
              providedDatanode1.getDatanodeUuid())
          || dnInfos[0].getDatanodeUuid().equals(
              providedDatanode2.getDatanodeUuid()));

      // stop the 1st provided datanode
      MiniDFSCluster.DataNodeProperties providedDNProperties1 =
          cluster.stopDataNode(0);

      // make NameNode detect that datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(
          cluster.getNameNode(),
          providedDatanode1.getDatanodeId().getXferAddr());

      // should find the block on the 2nd provided datanode
      dnInfos = getAndCheckBlockLocations(client, filename, baseFileLen, 1, 1);
      assertEquals(providedDatanode2.getDatanodeUuid(),
          dnInfos[0].getDatanodeUuid());

      // stop the 2nd provided datanode
      MiniDFSCluster.DataNodeProperties providedDNProperties2 =
          cluster.stopDataNode(0);
      // make NameNode detect that datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(
          cluster.getNameNode(),
          providedDatanode2.getDatanodeId().getXferAddr());
      getAndCheckBlockLocations(client, filename, baseFileLen, 1, 0);

      // BR count for the provided ProvidedDatanodeStorageInfo should reset to
      // 0, when all DNs with PROVIDED storage fail.
      assertEquals(0, providedDNInfo.getBlockReportCount());
      // restart the provided datanode
      cluster.restartDataNode(providedDNProperties1, true);
      cluster.waitActive();

      assertEquals(1, providedDNInfo.getBlockReportCount());

      // should find the block on the 1st provided datanode now
      dnInfos = getAndCheckBlockLocations(client, filename, baseFileLen, 1, 1);
      // not comparing UUIDs as the datanode can now have a different one.
      assertEquals(providedDatanode1.getDatanodeId().getXferAddr(),
          dnInfos[0].getXferAddr());
    }
  }

  @Test(timeout=300000)
  public void testTransientDeadDatanodes() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
            FixedBlockResolver.class);
    // 3 Datanodes, 2 PROVIDED and other DISK
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    cluster = startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false, conf);

    DataNode providedDatanode = cluster.getDataNodes().get(0);
    DatanodeStorageInfo providedDNInfo = getProvidedDatanodeStorageInfo();
    int initialBRCount = providedDNInfo.getBlockReportCount();
    for (int i= 0; i < numFiles; i++) {
      // expect to have 2 locations as we have 2 provided Datanodes.
      verifyFileLocation(i, 2);
      // NameNode thinks the datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(
          cluster.getNameNode(),
          providedDatanode.getDatanodeId().getXferAddr());
      cluster.waitActive();
      cluster.triggerHeartbeats();
      Thread.sleep(1000);
      // the report count should just continue to increase.
      assertEquals(initialBRCount + i + 1,
          providedDNInfo.getBlockReportCount());
      verifyFileLocation(i, 2);
    }
  }

  private DatanodeStorageInfo getProvidedDatanodeStorageInfo() {
    ProvidedStorageMap providedStorageMap =
        cluster.getNamesystem().getBlockManager().getProvidedStorageMap();
    return providedStorageMap.getProvidedStorageInfo();
  }

  @Test(timeout=30000)
  public void testNamenodeRestart() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    // 3 Datanodes, 2 PROVIDED and other DISK
    cluster = startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false, conf);

    verifyFileLocation(numFiles - 1, 2);
    cluster.restartNameNodes();
    cluster.waitActive();
    verifyFileLocation(numFiles - 1, 2);
  }

  /**
   * verify that the specified file has a valid provided location.
   * @param fileIndex the index of the file to verify.
   * @throws Exception
   */
  private void verifyFileLocation(int fileIndex, int replication)
      throws Exception {
    DFSClient client = new DFSClient(
        new InetSocketAddress("localhost", cluster.getNameNodePort()),
        cluster.getConfiguration(0));
    if (fileIndex < numFiles && fileIndex >= 0) {
      String filename = filePrefix + fileIndex + fileSuffix;
      File file = new File(new Path(providedPath, filename).toUri());
      long fileLen = file.length();
      long blockSize = conf.getLong(FixedBlockResolver.BLOCKSIZE,
          FixedBlockResolver.BLOCKSIZE_DEFAULT);
      long numLocatedBlocks =
          fileLen == 0 ? 1 : (long) Math.ceil(fileLen * 1.0 / blockSize);
      getAndCheckBlockLocations(client, "/" + filename, fileLen,
          numLocatedBlocks, replication);
    }
  }

  @Test(timeout=30000)
  public void testSetClusterID() throws Exception {
    String clusterID = "PROVIDED-CLUSTER";
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class, clusterID, TextFileRegionAliasMap.class,
        bpid, conf);
    // 2 Datanodes, 1 PROVIDED and other DISK
    cluster = startCluster(nnDirPath, 2, null,
        new StorageType[][] {
            {StorageType.DISK},
            {StorageType.DISK}},
        false, conf);
    NameNode nn = cluster.getNameNode();
    assertEquals(clusterID, nn.getNamesystem().getClusterId());
  }

  @Test(timeout=30000)
  public void testNumberOfProvidedLocations() throws Exception {
    // set default replication to 4
    conf.setInt(DFS_REPLICATION_KEY, 4);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // start with 4 PROVIDED location
    cluster = startCluster(nnDirPath, 4,
        new StorageType[]{StorageType.DISK},
        null,
        false, conf);
    int expectedLocations = 4;
    for (int i = 0; i < numFiles; i++) {
      verifyFileLocation(i, expectedLocations);
    }
    // stop 2 datanodes, one after the other and verify number of locations.
    for (int i = 1; i <= 2; i++) {
      DataNode dn = cluster.getDataNodes().get(0);
      cluster.stopDataNode(0);
      // make NameNode detect that datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(cluster.getNameNode(),
          dn.getDatanodeId().getXferAddr());

      expectedLocations = 4 - i;
      for (int j = 0; j < numFiles; j++) {
        verifyFileLocation(j, expectedLocations);
      }
    }
  }

  @Test(timeout=30000)
  public void testNumberOfProvidedLocationsManyBlocks() throws Exception {
    // increase number of blocks per file to at least 10 blocks per file
    conf.setLong(FixedBlockResolver.BLOCKSIZE, baseFileLen/10);
    // set default replication to 4
    conf.setInt(DFS_REPLICATION_KEY, 4);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // start with 4 PROVIDED location
    cluster = startCluster(nnDirPath, 4,
        new StorageType[]{StorageType.DISK},
        null,
        false, conf);
    int expectedLocations = 4;
    for (int i = 0; i < numFiles; i++) {
      verifyFileLocation(i, expectedLocations);
    }
  }

  private File createInMemoryAliasMapImage() throws Exception {
    return createInMemoryAliasMapImage(new FSTreeWalk(providedPath, conf));
  }

  private File createInMemoryAliasMapImage(FSTreeWalk treewalk)
      throws Exception {
    return createInMemoryAliasMapImage(
        conf, nnDirPath, bpid, clusterID, treewalk);
  }

  public static File createInMemoryAliasMapImage(Configuration conf,
      Path nnDirPath, String bpid, String clusterID,
      FSTreeWalk treewalk) throws Exception {
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
        UGIResolver.class);
    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        InMemoryLevelDBAliasMapClient.class, BlockAliasMap.class);
    conf.set(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS, "localhost:32445");
    File tempDirectory =
        new File(new Path(nnDirPath, "in-memory-alias-map").toUri());
    File levelDBDir = new File(tempDirectory, bpid);
    levelDBDir.mkdirs();
    conf.set(DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
        tempDirectory.getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    conf.set(DFS_PROVIDED_ALIASMAP_LEVELDB_PATH,
        tempDirectory.getAbsolutePath());

    createImage(treewalk,
        nnDirPath,
        FixedBlockResolver.class, clusterID,
        LevelDBFileRegionAliasMap.class,  bpid, conf);
    return tempDirectory;
  }

  @Test
  public void testInMemoryAliasMap() throws Exception {
    File aliasMapImage = createInMemoryAliasMapImage();
    // start cluster with two datanodes,
    // each with 1 PROVIDED volume and other DISK volume
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    cluster = startCluster(nnDirPath, 2,
        new StorageType[] {StorageType.DISK},
        null, false, conf);
    verifyFileSystemContents(0);
    FileUtils.deleteDirectory(aliasMapImage);
  }

  @Test
  public void testHigherDefaultReplication() throws Exception {
    conf.setInt(DFS_REPLICATION_KEY, 10);
    testInMemoryAliasMap();
  }

  /**
   * Find a free port that hasn't been assigned yet.
   *
   * @param usedPorts set of ports that have already been assigned.
   * @param maxTrials maximum number of random ports to try before failure.
   * @return an unassigned port.
   */
  private int getUnAssignedPort(Set<Integer> usedPorts, int maxTrials) {
    int count = 0;
    while (count < maxTrials) {
      int port = NetUtils.getFreeSocketPort();
      if (usedPorts.contains(port)) {
        count++;
      } else {
        return port;
      }
    }
    return -1;
  }

  private static String providedNameservice;

  /**
   * Extends the {@link MiniDFSCluster.Builder} to create instances of
   * {@link MiniDFSClusterBuilderAliasMap}.
   */
  public static class MiniDFSClusterBuilderAliasMap
          extends MiniDFSCluster.Builder {

    MiniDFSClusterBuilderAliasMap(Configuration conf,
        String providedNS) {
      super(conf);
      providedNameservice = providedNS;
    }

    @Override
    public MiniDFSCluster build() throws IOException {
      return new MiniDFSClusterAliasMap(this);
    }
  }

  /**
   * Extends {@link MiniDFSCluster} to correctly configure the InMemoryAliasMap.
   */
  public static class MiniDFSClusterAliasMap extends MiniDFSCluster {

    private Map<String, Collection<URI>> formattedDirsByNamespaceId;
    private Set<Integer> completedNNs;

    MiniDFSClusterAliasMap(MiniDFSCluster.Builder builder) throws IOException {
      super(builder);
    }

    @Override
    protected void initNameNodeConf(Configuration conf, String nameserviceId,
        int nsIndex, String nnId, boolean manageNameDfsDirs,
        boolean enableManagedDfsDirsRedundancy, int nnIndex)
        throws IOException {

      if (formattedDirsByNamespaceId == null) {
        formattedDirsByNamespaceId = new HashMap<>();
        completedNNs = new HashSet<>();
      }

      super.initNameNodeConf(conf, nameserviceId, nsIndex, nnId,
          manageNameDfsDirs, enableManagedDfsDirsRedundancy, nnIndex);

      if (providedNameservice == null
          || providedNameservice.equals(nameserviceId)) {
        // configure the InMemoryAliasMp.
        conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
        String directory = conf.get(DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
        if (directory == null || !new File(directory).exists()) {
          throw new IllegalArgumentException("In-memory alias map configured"
              + "with the proper location; Set "
              + DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR);
        }
        // get the name of the directory (final component in path) used for map.
        // Assume that the aliasmap configured with the same final component
        // name in all Namenodes but is located in the path specified by
        // DFS_NAMENODE_NAME_DIR_KEY
        String dirName = new Path(directory).getName();
        String nnDir =
            conf.getTrimmedStringCollection(DFS_NAMENODE_NAME_DIR_KEY)
                .iterator().next();
        conf.set(DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
            new File(new Path(nnDir, dirName).toUri()).getAbsolutePath());
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);

        // format the shared edits dir with the proper VERSION file.
        NameNode.initializeSharedEdits(conf);
      } else {
        if (!completedNNs.contains(nnIndex)) {
          // format the NN directories for non-provided namespaces
          // if the directory for a namespace has been formatted, copy it over.
          Collection<URI> namespaceDirs = FSNamesystem.getNamespaceDirs(conf);
          if (formattedDirsByNamespaceId.containsKey(nameserviceId)) {
            copyNameDirs(formattedDirsByNamespaceId.get(nameserviceId),
                namespaceDirs, conf);
          } else {
            for (URI nameDirUri : namespaceDirs) {
              File nameDir = new File(nameDirUri);
              if (nameDir.exists() && !FileUtil.fullyDelete(nameDir)) {
                throw new IOException("Could not fully delete " + nameDir);
              }
            }
            HdfsServerConstants.StartupOption.FORMAT.setClusterId(clusterID);
            DFSTestUtil.formatNameNode(conf);
            formattedDirsByNamespaceId.put(nameserviceId, namespaceDirs);
          }
          conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, false);
          completedNNs.add(nnIndex);
        }
      }
    }

    @Override
    protected void createNameNode(Configuration hdfsConf, boolean format,
        HdfsServerConstants.StartupOption operation, String clusterId,
        String nameserviceId, String nnId) throws IOException {
      hdfsConf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
          NamenodeInMemoryAliasMapClient.class, BlockAliasMap.class);
      // create the NN with the new config
      super.createNameNode(hdfsConf, format, operation, clusterId,
          nameserviceId, nnId);
    }
  }

  /**
   * Configures the addresseses of the InMemoryAliasMap.
   *
   * @param topology the MiniDFS topology to use.
   * @param providedNameservice the nameservice id that supports provided.
   */
  private void configureAliasMapAddresses(MiniDFSNNTopology topology,
      String providedNameservice) {
    conf.unset(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS);
    Set<Integer> assignedPorts = new HashSet<>();
    for (MiniDFSNNTopology.NSConf nsConf : topology.getNameservices()) {
      for (MiniDFSNNTopology.NNConf nnConf : nsConf.getNNs()) {
        if (providedNameservice.equals(nsConf.getId())) {
          String key =
              DFSUtil.addKeySuffixes(DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
                  nsConf.getId(), nnConf.getNnId());
          int port = getUnAssignedPort(assignedPorts, 10);
          if (port == -1) {
            throw new RuntimeException("No free ports available");
          }
          assignedPorts.add(port);
          conf.set(key, "127.0.0.1:" + port);

          String binHostKey =
              DFSUtil.addKeySuffixes(
                  DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST,
                  nsConf.getId(), nnConf.getNnId());
          conf.set(binHostKey, "0.0.0.0");
        }
      }
    }
  }

  /**
   * Verify the mounted contents of the Filesystem.
   *
   * @param topology the topology of the cluster.
   * @param providedNameservice the namespace id of the provided namenodes.
   * @throws Exception
   */
  private void verifyPathsWithHAFailoverIfNecessary(MiniDFSNNTopology topology,
      String providedNameservice) throws Exception {
    List<Integer> nnIndexes = cluster.getNNIndexes(providedNameservice);
    if (topology.isHA()) {
      int nn1 = nnIndexes.get(0);
      int nn2 = nnIndexes.get(1);
      try {
        verifyFileSystemContents(nn1);
        fail("Read operation should fail as no Namenode is active");
      } catch (RemoteException e) {
        LOG.info("verifyPaths failed!. Expected exception: {}" + e);
      }
      cluster.transitionToActive(nn1);
      LOG.info("Verifying data from NN with index = {}", nn1);
      verifyFileSystemContents(nn1);
      // transition to the second namenode.
      cluster.transitionToStandby(nn1);
      cluster.transitionToActive(nn2);
      LOG.info("Verifying data from NN with index = {}", nn2);
      verifyFileSystemContents(nn2);

      cluster.shutdownNameNodes();
      try {
        verifyFileSystemContents(nn2);
        fail("Read operation should fail as no Namenode is active");
      } catch (NullPointerException e) {
        LOG.info("verifyPaths failed!. Expected exception: {}" + e);
      }
    } else {
      verifyFileSystemContents(nnIndexes.get(0));
    }
  }

  @Test
  public void testInMemoryAliasMapMultiTopologies() throws Exception {
    MiniDFSNNTopology[] topologies =
        new MiniDFSNNTopology[] {MiniDFSNNTopology.simpleHATopology(),
            MiniDFSNNTopology.simpleFederatedTopology(3),
            MiniDFSNNTopology.simpleHAFederatedTopology(3)};

    for (MiniDFSNNTopology topology : topologies) {
      LOG.info("Starting test with topology with HA = {}, federation = {}",
          topology.isHA(), topology.isFederated());
      setSeed();
      createInMemoryAliasMapImage();
      conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
      conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
      providedNameservice = topology.getNameservices().get(0).getId();
      // configure the AliasMap addresses
      configureInMemoryAliasMapAddresses(topology, conf, providedNameservice);
      cluster =
          startCluster(nnDirPath, 2,
              new StorageType[] {StorageType.DISK}, null, false, conf,
              null, topology,
              new MiniDFSClusterBuilderAliasMap(conf, providedNameservice));

      verifyPathsWithHAFailoverIfNecessary(topology, providedNameservice);
      shutdown();
    }
  }

  private DatanodeDescriptor getDatanodeDescriptor(DatanodeManager dnm,
      int dnIndex) throws Exception {
    return dnm.getDatanode(cluster.getDataNodes().get(dnIndex).getDatanodeId());
  }

  private void startDecommission(FSNamesystem namesystem, DatanodeManager dnm,
      int dnIndex) throws Exception {
    namesystem.writeLock();
    DatanodeDescriptor dnDesc = getDatanodeDescriptor(dnm, dnIndex);
    dnm.getDatanodeAdminManager().startDecommission(dnDesc);
    namesystem.writeUnlock();
  }

  private void startMaintenance(FSNamesystem namesystem, DatanodeManager dnm,
      int dnIndex) throws Exception {
    namesystem.writeLock();
    DatanodeDescriptor dnDesc = getDatanodeDescriptor(dnm, dnIndex);
    dnm.getDatanodeAdminManager().startMaintenance(dnDesc, Long.MAX_VALUE);
    namesystem.writeUnlock();
  }

  private void stopMaintenance(FSNamesystem namesystem, DatanodeManager dnm,
      int dnIndex) throws Exception {
    namesystem.writeLock();
    DatanodeDescriptor dnDesc = getDatanodeDescriptor(dnm, dnIndex);
    dnm.getDatanodeAdminManager().stopMaintenance(dnDesc);
    namesystem.writeUnlock();
  }

  @Test
  public void testDatanodeLifeCycle() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    cluster = startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false, conf);

    int fileIndex = numFiles - 1;

    final BlockManager blockManager = cluster.getNamesystem().getBlockManager();
    final DatanodeManager dnm = blockManager.getDatanodeManager();

    // to start, all 3 DNs are live in ProvidedDatanodeDescriptor.
    verifyFileLocation(fileIndex, 3);

    // de-commision first DN; still get 3 replicas.
    startDecommission(cluster.getNamesystem(), dnm, 0);
    verifyFileLocation(fileIndex, 3);

    // remains the same even after heartbeats.
    cluster.triggerHeartbeats();
    verifyFileLocation(fileIndex, 3);

    // start maintenance for 2nd DN; still get 3 replicas.
    startMaintenance(cluster.getNamesystem(), dnm, 1);
    verifyFileLocation(fileIndex, 3);

    DataNode dn1 = cluster.getDataNodes().get(0);
    DataNode dn2 = cluster.getDataNodes().get(1);

    // stop the 1st DN while being decommissioned.
    MiniDFSCluster.DataNodeProperties dn1Properties = cluster.stopDataNode(0);
    BlockManagerTestUtil.noticeDeadDatanode(cluster.getNameNode(),
        dn1.getDatanodeId().getXferAddr());

    // get 2 locations
    verifyFileLocation(fileIndex, 2);

    // stop dn2 while in maintenance.
    MiniDFSCluster.DataNodeProperties dn2Properties = cluster.stopDataNode(1);
    BlockManagerTestUtil.noticeDeadDatanode(cluster.getNameNode(),
        dn2.getDatanodeId().getXferAddr());

    // 2 valid locations will be found as blocks on nodes that die during
    // maintenance are not marked for removal.
    verifyFileLocation(fileIndex, 2);

    // stop the maintenance; get only 1 replicas
    stopMaintenance(cluster.getNamesystem(), dnm, 0);
    verifyFileLocation(fileIndex, 1);

    // restart the stopped DN.
    cluster.restartDataNode(dn1Properties, true);
    cluster.waitActive();

    // reports all 3 replicas
    verifyFileLocation(fileIndex, 2);

    cluster.restartDataNode(dn2Properties, true);
    cluster.waitActive();

    // reports all 3 replicas
    verifyFileLocation(fileIndex, 3);
  }

  @Test
  public void testProvidedWithHierarchicalTopology() throws Exception {
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
        UGIResolver.class);
    String packageName = "org.apache.hadoop.hdfs.server.blockmanagement";
    String[] policies = new String[] {
        "BlockPlacementPolicyDefault",
        "BlockPlacementPolicyRackFaultTolerant",
        "BlockPlacementPolicyWithNodeGroup",
        "BlockPlacementPolicyWithUpgradeDomain"};
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    String[] racks =
        {"/pod0/rack0", "/pod0/rack0", "/pod0/rack1", "/pod0/rack1",
            "/pod1/rack0", "/pod1/rack0", "/pod1/rack1", "/pod1/rack1" };
    for (String policy: policies) {
      LOG.info("Using policy: " + packageName + "." + policy);
      conf.set(DFS_BLOCK_REPLICATOR_CLASSNAME_KEY, packageName + "." + policy);
      cluster = startCluster(nnDirPath, racks.length,
          new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
          null, false, conf, racks);
      verifyFileSystemContents(0);
      setAndUnsetReplication("/" + filePrefix + (numFiles - 1) + fileSuffix);
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockReadWithClientCache() throws Exception {
    // set read through to be true!
    conf.setBoolean(DFS_CLIENT_CACHE_READTHROUGH, true);
    testBlockReadWithCaching();
  }

  @Test
  public void testBlockReadWithoutClientCache() throws Exception {
    // set client read through to be false!
    conf.setBoolean(DFS_CLIENT_CACHE_READTHROUGH, false);
    // we should still be caching it.
    testBlockReadWithCaching();
  }

  private void testBlockReadWithCaching() throws Exception {
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
        UGIResolver.class);
    // test default replication to 1, to test caching.
    conf.setInt(DFS_REPLICATION_KEY, 1);
    int defaultReplication = 1;
    conf.setInt(FixedBlockMultiReplicaResolver.REPLICATION, defaultReplication);
    short overRep = 1;
    conf.setInt(DFS_PROVIDED_OVERREPLICATION_FACTOR_KEY, overRep);
    conf.setBoolean(DFS_DATANODE_PROVIDED_VOLUME_READ_THROUGH, true);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf);
    FileSystem fs = cluster.getFileSystem();
    Thread.sleep(2000);
    int count = 0;

    for (TreePath e : new FSTreeWalk(providedPath, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(providedPath, rs.getPath());
      // skip HDFS specific files, which may have been created later on
      if (hp.toString().contains("in_use.lock")
          || hp.toString().contains("current")) {
        continue;
      }
      e.accept(count++);
      FileStatus hs = fs.getFileStatus(hp);

      // read the file causing it to get cached!
      int bufferLength = 1024;
      byte[] buf = new byte[bufferLength];
      if (rs.isFile() && rs.getLen() > 0) {
        IOUtils.readFully(fs.open(hs.getPath()), buf, 0, bufferLength);
        LOG.info("Finished reading file: " + rs.getPath());
      }
    }
    // wait for read throughs to finish
    Thread.sleep(10000);
    DFSClient client = new DFSClient(
        new InetSocketAddress("localhost", cluster.getNameNodePort()),
        cluster.getConfiguration(0));

    for (TreePath e : new FSTreeWalk(providedPath, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(providedPath, rs.getPath());
      // skip HDFS specific files, which may have been created later on.
      if (hp.toString().contains("in_use.lock")
          || hp.toString().contains("current")) {
        continue;
      }
      e.accept(count++);
      FileStatus hs = fs.getFileStatus(hp);
      if (rs.isFile() && rs.getLen() > 0) {
        getAndCheckBlockLocations(client, hp.toString(), baseFileLen, 1,
            (short) (hs.getReplication() + overRep));
      }
    }
  }

  @Test
  public void testDynamicImageMountNamespace() throws Exception {
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, true);
    cluster = startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.DISK},
        null, false, conf);

    File newDir = new File(new File(providedPath.toUri()), "newDir");
    String remotePath = newDir.toURI().toString();
    String mountpoint = "/mounts/mount1";
    // call mount
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    // mount when the remote path doesn't exist.
    LambdaTestUtils.intercept(RemoteException.class,
        "File " + remotePath + " does not exist",
        () -> client.addMount(remotePath, mountpoint, null));

    // create the new files to mount
    newDir.mkdirs();
    createFiles(new Path(newDir.toURI()), 10, baseFileLen, "newFile",
        fileSuffix);
    assertTrue(client.addMount(remotePath, mountpoint,
        RemoteMountUtils.decodeConfig("a=b,c=d")));
    // verify new image!!
    verifyPaths(cluster, conf, new Path(mountpoint), new Path(newDir.toURI()),
        0, true);
    verifyMountXAttrs(cluster, 0, new Path(mountpoint), "a=b,c=d");
    verifyUpdatedAliasMap(cluster.getNameNode(), cluster.getFileSystem(),
        new Path(mountpoint));

    LambdaTestUtils.intercept(RemoteException.class,
        "Mount path " + mountpoint + " already exists",
        () -> client.addMount(remotePath, mountpoint, null));

    String newMountPoint = mountpoint + "/new-mount";
    LambdaTestUtils.intercept(RemoteException.class,
        "Mount point " + newMountPoint +
            " cannot belong to the existing mount " + mountpoint,
        () -> client.addMount(remotePath, newMountPoint, null));

    // create directories, files, and delete should all fail on a mount.
    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint, () -> client
            .mkdirs(mountpoint + "/newDir", FsPermission.getDefault(),
                false));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.create(mountpoint + "/newFile.dat", true));

    assertTrue(client.exists(mountpoint + "/newFile0" + fileSuffix));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.delete(mountpoint, true));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.delete("/mounts", true));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.rename("/mounts", "/target", Options.Rename.NONE));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.rename("/target", "/mounts", Options.Rename.NONE));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.rename("/target", mountpoint, Options.Rename.NONE));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.rename(mountpoint, "/target", Options.Rename.NONE));

    LambdaTestUtils.intercept(AccessControlException.class,
        "No modifications allowed within mount: " + mountpoint,
        () -> client.rename(mountpoint, "/target"));

  }

  private void verifyMountXAttrs(MiniDFSCluster cluster, int nnIndex,
      Path mountPath, String attrString) throws IOException {
    Map<String, byte[]> xAttrs =
        cluster.getFileSystem(nnIndex).getXAttrs(mountPath);
    assertEquals(3, xAttrs.size());
    assertEquals("true", new String(xAttrs.get("user.isMount")));
    Map<String, String> attrs = RemoteMountUtils.decodeConfig(attrString);
    for (Map.Entry<String, String> attr : attrs.entrySet()) {
      String key = "trusted.mount.config." + attr.getKey();
      assertEquals(attr.getValue(), new String(xAttrs.get(key)));
    }
  }

  private void verifyUpdatedAliasMap(NameNode nn, FileSystem fs, Path rootPath)
      throws Exception {
    FSNamesystem fsNamesystem = nn.getNamesystem();
    // verify that alias map is updated, and we can find the new block ids.
    BlockAliasMap aliasMap =
        fsNamesystem.getBlockManager().getProvidedStorageMap().getAliasMap();
    BlockAliasMap.Reader reader = aliasMap.getReader(null, bpid);
    assertNotNull(reader);
    for (FileStatus status : fs.listStatus(rootPath)) {
      String path =
          getPathWithoutSchemeAndAuthority(status.getPath()).toString();
      LocatedBlocks blocks =
          fsNamesystem.getBlockLocations("test", path, 0, status.getLen());
      for (LocatedBlock block : blocks.getLocatedBlocks()) {
        LOG.info("Checking " + path + " block " + block.getBlock());
        assertNotNull(reader.resolve(block.getBlock().getBlockId()));
      }
    }
  }

  @Test
  public void testDynamicImageMountNamespaceHA() throws Exception {
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);
    HAUtil.setAllowStandbyReads(conf, true);

    MiniDFSNNTopology topology = MiniDFSNNTopology.simpleHATopology();
    createInMemoryAliasMapImage();
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureInMemoryAliasMapAddresses(topology, conf, providedNameservice);

    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf, null, topology,
        new MiniDFSClusterBuilderAliasMap(conf, providedNameservice));

    List<Integer> nnIndexes = cluster.getNNIndexes(providedNameservice);
    int nn1 = nnIndexes.get(0);
    int nn2 = nnIndexes.get(1);
    cluster.transitionToActive(nn1);

    verifyPaths(cluster, conf, new Path("/"), providedPath, nn1, true);
    LOG.info("Verified initial mount. Created new files");

    // create the new files to mount
    File newDir = new File(new File(providedPath.toUri()), "newDir");
    newDir.mkdirs();
    createFiles(new Path(newDir.toURI()), 10, baseFileLen, "newFile",
        fileSuffix);

    String newMountPoint = "/mount1";
    LOG.info("Calling createMount with mountpoint " + newMountPoint);
    // call mount
    DFSClient client =
        new DFSClient(cluster.getNameNode(nn1).getNameNodeAddress(), conf);
    client.addMount(newDir.toURI().toString(), newMountPoint,
        RemoteMountUtils.decodeConfig("a=b,c=d"));

    LOG.info("Verifying data on new mount with 1st NN");
    verifyPaths(cluster, conf, new Path(newMountPoint),
        new Path(newDir.toURI()), nn1, true);
    verifyMountXAttrs(cluster, nn1, new Path(newMountPoint), "a=b,c=d");
    verifyUpdatedAliasMap(cluster.getNameNode(nn1), cluster.getFileSystem(nn1),
        new Path(newMountPoint));

    LOG.info("Transitioning to next NN");
    HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(nn1),
        cluster.getNameNode(nn2));
    cluster.transitionToStandby(nn1);
    cluster.transitionToActive(nn2);
    // trigger heartbeats to update the datanodes
    cluster.triggerHeartbeats();
    Thread.sleep(1000);
    // verify on the 2nd namenode
    LOG.info("Verifying data on new mount with 2nd NN");
    verifyPaths(cluster, conf, new Path(newMountPoint),
        new Path(newDir.toURI()), nn2, true);
    verifyUpdatedAliasMap(cluster.getNameNode(nn2), cluster.getFileSystem(nn2),
        new Path(newMountPoint));

    client = new DFSClient(cluster.getNameNode(nn2).getNameNodeAddress(), conf);

    // mount on the same path should fail
    try {
      client.addMount(newDir.toURI().toString(), newMountPoint, null);
      fail("addMount should fail when mounting on the same path");
    } catch (RemoteException e) {
      assertTrue(e.getMessage()
          .contains("Mount path " + newMountPoint + " already exists"));
      LOG.info("Expected exception: {}", e);
    }
    // now call removeMount and transition to the other NN.
    assertTrue(client.removeMount(newMountPoint));
    HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(nn2),
        cluster.getNameNode(nn1));
    cluster.transitionToStandby(nn2);
    cluster.transitionToActive(nn1);
    client = new DFSClient(cluster.getNameNode(nn1).getNameNodeAddress(), conf);
    // nothing should exist under the mount point.
    assertNull(client.getFileInfo(newMountPoint));
    // mount should succeed now.
    assertTrue(client.addMount(newDir.toURI().toString(), newMountPoint,
        RemoteMountUtils.decodeConfig("a=b,c=d")));
    verifyPaths(cluster, conf, new Path(newMountPoint),
        new Path(newDir.toURI()), nn1, true);
    verifyMountXAttrs(cluster, nn1, new Path(newMountPoint), "a=b,c=d");
  }

  @Test
  public void testAbsentFiles() throws Exception {
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
        UGIResolver.class);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf);
    FileSystem fs = cluster.getFileSystem();
    String fileToDelete = filePrefix + (numFiles - 1) + fileSuffix;
    new File(new File(providedPath.toUri()), fileToDelete).delete();
    FSDataInputStream ins = fs.open(new Path("/" + fileToDelete));
    try {
      ins.read();
      fail("Expected to fail on reading file " + fileToDelete);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Could not obtain block"));
    }
  }

  @Test
  public void testMountLimits() throws Exception {
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    // set limit on inodes to 5
    int maxInodes = 10;
    int maxMounts = 1;
    conf.setInt(DFS_PROVIDED_READ_MOUNT_INODES_MAX, maxInodes);
    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf);

    // create the new files to mount
    final File dir1 = new File(new File(providedPath.toUri()), "dir1");
    dir1.mkdirs();
    createFiles(new Path(dir1.toURI()), maxInodes * 2, baseFileLen, "file",
        fileSuffix);
    // call mount -- this should fail as we are mounting too many files.
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    LambdaTestUtils.intercept(IOException.class,
        "Number of inodes in remote path (24) exceed the maximum allowed of 10",
        () -> client.addMount(dir1.toURI().toString(), "/mount1", null));

    File dir2 = new File(new File(providedPath.toUri()), "dir2");
    dir2.mkdirs();
    createFiles(new Path(dir2.toURI()), maxInodes - 5, baseFileLen, "file",
        fileSuffix);
    // this mount should succeed as we are mounting fewer inodes than max value.
    assertTrue(client.addMount(dir2.toURI().toString(), "/mount1", null));
  }

  @Test
  public void testBootstrapAliasMap() throws Exception {
    int numNamenodes = 3;
    MiniDFSNNTopology topology =
        MiniDFSNNTopology.simpleHATopology(numNamenodes);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureInMemoryAliasMapAddresses(topology, conf, providedNameservice);
    cluster = startCluster(nnDirPath, 2,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
        false, conf, null, topology,
        new MiniDFSClusterBuilderAliasMap(conf, providedNameservice));

    // make NN with index 0 the active, shutdown and delete the directories
    // of others. This will delete the aliasmap on these namenodes as well.
    cluster.transitionToActive(0);
    verifyFileSystemContents(0);
    for (int nnIndex = 1; nnIndex < numNamenodes; nnIndex++) {
      cluster.shutdownNameNode(nnIndex);
      // delete the namenode directories including alias map.
      for (URI u : cluster.getNameDirs(nnIndex)) {
        File dir = new File(u.getPath());
        assertTrue(FileUtil.fullyDelete(dir));
      }
    }

    // start the other namenodes and bootstrap them
    for (int index = 1; index < numNamenodes; index++) {
      // add some content to aliasmap dir
      File aliasMapDir = new File(fBASE, "aliasmap-" + index);
      // create a directory inside aliasMapDir
      if (!new File(aliasMapDir, "tempDir").mkdirs()) {
        throw new IOException("Unable to create directory " + aliasMapDir);
      }
      Configuration currNNConf = cluster.getConfiguration(index);
      currNNConf.set(DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
          aliasMapDir.getAbsolutePath());
      // without force this should fail as aliasmap is not empty.
      int rc =
          BootstrapStandby.run(new String[] {"-nonInteractive"}, currNNConf);
      assertNotEquals(0, rc);
      // force deletes the contents of the aliasmap.
      rc = BootstrapStandby.run(new String[] {"-nonInteractive", "-force"},
          currNNConf);
      assertEquals(0, rc);
    }

    // check if aliasmap files are the same on all NNs
    checkInMemoryAliasMapContents(0, numNamenodes);
    // restart the killed namenodes.
    for (int i = 1; i < numNamenodes; i++) {
      cluster.restartNameNode(i, false);
    }

    cluster.waitClusterUp();
    cluster.waitActive();

    // transition to namenode 1 as the active
    int nextNN = 1;
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(nextNN);
    // all files must be accessible from nextNN.
    verifyFileSystemContents(nextNN);
  }

  /**
   * Check if the alias map contents of the namenodes are the same as the base.
   *
   * @param baseNN index of the namenode to compare against.
   * @param numNamenodes total number of namenodes in the cluster.
   */
  private void checkInMemoryAliasMapContents(int baseNN, int numNamenodes)
      throws Exception {
    InMemoryLevelDBAliasMapServer baseAliasMap =
        cluster.getNameNode(baseNN).getAliasMapServer();
    for (int i = 0; i < numNamenodes; i++) {
      if (baseNN == i) {
        continue;
      }
      InMemoryLevelDBAliasMapServer otherAliasMap =
          cluster.getNameNode(baseNN).getAliasMapServer();
      verifyAliasMapEquals(baseAliasMap, otherAliasMap);
    }
  }

  /**
   * Verify that the contents of the aliasmaps are the same.
   * 
   * @param aliasMap1
   * @param aliasMap2
   */
  private void verifyAliasMapEquals(InMemoryLevelDBAliasMapServer aliasMap1,
      InMemoryLevelDBAliasMapServer aliasMap2) throws Exception {
    Set<FileRegion> fileRegions1 = getFileRegions(aliasMap1);
    Set<FileRegion> fileRegions2 = getFileRegions(aliasMap2);
    assertTrue(fileRegions1.equals(fileRegions2));
  }

  /**
   * Get all the aliases the aliasmap contains.
   * 
   * @param aliasMap aliasmap to explore.
   * @return set of all aliases.
   * @throws IOException
   */
  private Set<FileRegion> getFileRegions(InMemoryLevelDBAliasMapServer aliasMap)
      throws IOException {
    Set<FileRegion> fileRegions = new HashSet<>();
    Block marker = null;
    while (true) {
      InMemoryAliasMapProtocol.IterationResult result =
          aliasMap.list(Optional.ofNullable(marker));
      fileRegions.addAll(result.getFileRegions());
      marker = result.getNextBlock().orElse(null);
      if (marker == null) {
        break;
      }
    }
    return fileRegions;
  }

  private MiniDFSCluster createRemoteHDFSClusterWithAcls(Path testFile)
      throws Exception {
    // create a second HDFS cluster to act as remote.
    Configuration remoteConf = new HdfsConfiguration();
    remoteConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

    MiniDFSCluster remoteCluster =
        new MiniDFSCluster.Builder(remoteConf).numDataNodes(3).build();
    try {
      FileSystem remoteFS = remoteCluster.getFileSystem();
      // create directory on remote
      Path remoteDir = testFile.getParent();
      remoteFS.mkdirs(remoteDir);
      List<AclEntry> acls =
          Lists.newArrayList(AclTestHelpers.aclEntry(AclEntryScope.DEFAULT,
              AclEntryType.USER, "hdfs", FsAction.ALL));
      remoteFS.setAcl(remoteDir, acls);
      // create file in the remote path
      OutputStream out = remoteFS.create(testFile);
      for (int i = 0; i < 10; ++i) {
        out.write(i);
      }
      out.close();
      return remoteCluster;
    } catch (IOException e) {
      remoteCluster.shutdown();
      throw e;
    }
  }

  private void testAclsEqual(Path file, FileSystem remoteFS, FileSystem localFS)
      throws Exception {
    // acls on the file and it's parent should be the same.
    assertEquals(remoteFS.getAclStatus(file), localFS.getAclStatus(file));
    Path parent = file.getParent();
    if (parent != null) {
      assertEquals(remoteFS.getAclStatus(parent), localFS.getAclStatus(parent));
    }
  }

  @Test
  public void testDynamicMountACLs() throws Exception {
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_MOUNT_ACLS_ENABLED, true);
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
    createInMemoryAliasMapImage();
    cluster = startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
        false, conf);

    Path remoteDir = new Path("/data/");
    String testFileName = "test1.dat";
    Path testFile = new Path(remoteDir, testFileName);
    MiniDFSCluster remoteCluster = createRemoteHDFSClusterWithAcls(testFile);
    try {
      FileSystem remoteFS = remoteCluster.getFileSystem();
      // mount the remote path on cluster.
      String mountpoint = "/data";
      DFSClient client =
          new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
      String pathToMount = remoteFS.getUri().toString() + remoteDir.toString();
      assertTrue(client.addMount(pathToMount, mountpoint, null));
      // verify that the acls mirrored during mount.
      testAclsEqual(testFile, remoteFS, cluster.getFileSystem());
    } finally {
      remoteCluster.shutdown();
    }
  }

  @Test
  public void testImageWriterAcls() throws Exception {
    Path remoteDir = new Path("/data/");
    String testFileName = "test1.dat";
    Path testFile = new Path(remoteDir, testFileName);
    MiniDFSCluster remoteCluster = createRemoteHDFSClusterWithAcls(testFile);
    try {
      conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_MOUNT_ACLS_ENABLED, true);
      conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
      conf.setBoolean(DFS_DATANODE_PROVIDED_ENABLED, false);
      conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
          UGIResolver.class);
      // create the FSImage from remoteCluster
      FileSystem remoteFS = remoteCluster.getFileSystem();
      // this should fail -- TODO fix Acls for ImageWriter in 2.9.2
      createInMemoryAliasMapImage(
          new FSTreeWalk(new Path(remoteFS.getUri() + "/"), conf));
      cluster = startCluster(nnDirPath, 3,
          new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
          false, conf);
      // verify that the acls mirrored during mount.
      testAclsEqual(testFile, remoteFS, cluster.getFileSystem());
    } catch (UnsupportedOperationException e) {
      LOG.info("Expected exception: ", e);
    } finally {
      assertNull(cluster);
      remoteCluster.shutdown();
    }
  }

  @Test
  public void testNamenodeRestartDynamicMounts() throws Exception {
    MiniDFSNNTopology topology = MiniDFSNNTopology.simpleSingleNN(0, 0);

    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureInMemoryAliasMapAddresses(topology, conf, providedNameservice);
    cluster = startCluster(nnDirPath, 2, new StorageType[] {StorageType.DISK},
        null, false, conf, null, topology,
        new MiniDFSClusterBuilderAliasMap(conf, providedNameservice));
    // create the new files to mount
    final File dir1 = new File(new File(providedPath.toUri()), "dir1");
    dir1.mkdirs();
    createFiles(new Path(dir1.toURI()), 10, baseFileLen, "file", fileSuffix);
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    // this mount should succeed even though there is no capacity in the
    // cluster.
    String mountPath1 = "/mount1";
    String mountPath2 = "/mount2";
    assertTrue(client.addMount(dir1.toURI().toString(), mountPath1, null));
    assertTrue(client.addMount(dir1.toURI().toString(), mountPath2, null));

    // verify files in mounts.
    verifyPaths(cluster, conf, new Path(mountPath1), new Path(dir1.toURI()), 0,
        true);
    verifyPaths(cluster, conf, new Path(mountPath2), new Path(dir1.toURI()), 0,
        true);
    LOG.info("Restarting Namenode");
    cluster.restartNameNodes();
    // verify again!
    verifyPaths(cluster, conf, new Path(mountPath1), new Path(dir1.toURI()), 0,
        true);
    verifyPaths(cluster, conf, new Path(mountPath2), new Path(dir1.toURI()), 0,
        true);
  }

  @Test
  public void testMountWithoutLocalSpace() throws Exception {
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);

    // start cluster with two Datanode.
    cluster = startCluster(nnDirPath, 2, new StorageType[] {StorageType.DISK},
        null, false, conf);

    // set the remaining capacity of the Datanode to 0.
    final DatanodeRegistration nodeReg = InternalDataNodeTestUtils
        .getDNRegistrationForBP(cluster.getDataNodes().get(0), bpid);
    final DatanodeDescriptor dd =
        NameNodeAdapter.getDatanode(cluster.getNamesystem(), nodeReg);
    for (DatanodeStorageInfo storage : dd.getStorageInfos()) {
      if (storage.getStorageType() != StorageType.PROVIDED) {
        storage.setUtilizationForTesting(0, 0, 0, 0);
      }
    }
    // create the new files to mount
    final File dir1 = new File(new File(providedPath.toUri()), "dir1");
    dir1.mkdirs();
    createFiles(new Path(dir1.toURI()), 10, baseFileLen, "file", fileSuffix);
    // create another directory inside the first one.
    final File dir2 = new File(dir1, "dir2");
    dir2.mkdirs();
    createFiles(new Path(dir2.toURI()), 10, baseFileLen, "file", fileSuffix);
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    String mount = "/remotes/path/to/mount";
    // this mount should succeed even though there is no capacity in the
    // cluster.
    assertTrue(client.addMount(dir1.toURI().toString(), mount, null));
    verifyPaths(cluster, conf, new Path(mount), new Path(dir1.toURI()), 0,
        true);

    // check that storage policy is set correctly.
    final BlockStoragePolicySuite suite =
        BlockStoragePolicySuite.createDefaultSuite();

    assertEquals(suite.getDefaultPolicy(), client.getStoragePolicy("/"));
    assertEquals(suite.getDefaultPolicy(), client.getStoragePolicy("/remotes"));
    // policy of the mount and directories below it should be PROVIDED.
    assertEquals(suite.getPolicy(HdfsConstants.PROVIDED_STORAGE_POLICY_NAME),
        client.getStoragePolicy(mount));
    assertEquals(suite.getPolicy(HdfsConstants.PROVIDED_STORAGE_POLICY_NAME),
        client.getStoragePolicy(mount + "/dir2"));

    // set the capacity of Datanode back.
    for (DatanodeStorageInfo storage : dd.getStorageInfos()) {
      if (storage.getStorageType() != StorageType.PROVIDED) {
        storage.setUtilizationForTesting(10 * baseFileLen, 0L, 10 * baseFileLen,
            0L);
      }
    }
    // test that creating other files still succeeds.
    BlockLocation[] locations = createFile(new Path("/local/testFile1.dat"),
        (short) 2, 2 * baseFileLen, 2 * baseFileLen);
    assertEquals(1, locations.length);
    assertEquals(2, locations[0].getHosts().length);
  }

  @Test
  public void testLimitedCacheSpace() throws Exception {
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    // enable read-through
    conf.setBoolean(DFS_DATANODE_PROVIDED_VOLUME_READ_THROUGH, true);
    // set block len.
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen / 10);
    // fraction of space allowed as cache
    double cacheFraction = 0.1;
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION, cacheFraction);
    // eviction threshold is at 100% usage.
    conf.setDouble(DFS_PROVIDED_READ_CACHE_CAPACITY_THRESHOLD, 1);

    // set over-replication
    short overRep = 1;
    conf.setInt(DFS_PROVIDED_OVERREPLICATION_FACTOR_KEY, overRep);
    long diskCapacity = baseFileLen * 100;
    // start cluster with one DN.
    cluster = startCluster(nnDirPath, 1, new StorageType[] {StorageType.DISK},
        null, false, conf, null, null, new MiniDFSCluster.Builder(conf), null);

    try (FsDatasetSpi.FsVolumeReferences volumes =
        cluster.getDataNodes().get(0).getFSDataset().getFsVolumeReferences()) {
      for (FsVolumeSpi volume : volumes) {
        if (volume.getStorageType() == StorageType.DISK) {
          ((FsVolumeImpl) volume).setCapacityForTesting(diskCapacity);
        }
      }

    }
    // create the new files to mount
    final File dir1 = new File(new File(providedPath.toUri()), "dir1");
    dir1.mkdirs();
    int numFiles = 10;
    createFiles(new Path(dir1.toURI()), numFiles, baseFileLen, filePrefix,
        fileSuffix);
    // call mount -- this should fail as we are mounting too many files.
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    // mount the files.
    String mount = "/mounts/dir1/";
    assertTrue(client.addMount(dir1.toURI().toString(), mount, null));
    cluster.triggerHeartbeats();
    verifyPaths(cluster, conf, new Path(mount), new Path(dir1.toURI()), 0,
        true);
    // allow for cache manager to kick in
    Thread.sleep(1000);
    cluster.triggerHeartbeats();
    FSNamesystem namesystem = cluster.getNamesystem();
    SimpleReadCacheManager cacheManager = (SimpleReadCacheManager) namesystem
        .getMountManager().getReadCacheManager();
    // blocks have to be evicted due to the space constraints
    assertTrue(cacheManager.getNumBlocksEvicted() > 0);
    LOG.info("Blocks evicted " + cacheManager.getNumBlocksEvicted());

    DatanodeStatistics dnStats = namesystem.getBlockManager()
        .getDatanodeManager().getDatanodeStatistics();
    assertTrue(
        cacheManager.getCacheUsedForProvided() <= dnStats.getCapacityTotal()
            * cacheFraction);
    // check the DN capacities
    final DatanodeRegistration nodeReg = InternalDataNodeTestUtils
        .getDNRegistrationForBP(cluster.getDataNodes().get(0), bpid);

    final DatanodeDescriptor dd =
        NameNodeAdapter.getDatanode(cluster.getNamesystem(), nodeReg);
    for (DatanodeStorageInfo storage : dd.getStorageInfos()) {
      if (storage.getStorageType() == StorageType.DISK) {
        // the remaining capacity needs to be higher than false
        assertTrue(storage.getDatanodeDescriptor()
            .getRemaining() >= diskCapacity * cacheFraction);
      }
    }

    // space check on the DN as well
    for (DataNode dn : cluster.getDataNodes()) {
      for (StorageReport report : dn.getFSDataset()
          .getStorageReports(namesystem.getBlockPoolId())) {
        if (report.getStorage().getStorageType() == StorageType.DISK) {
          assertTrue(report.getRemaining() >= diskCapacity * cacheFraction);
        }
      }
    }

    // restart the NN and make sure the cache used is tracked correctly.
    long cacheUsed = cacheManager.getCacheUsedForProvided();
    assertTrue(cacheUsed > 0);
    cluster.restartNameNode();
    namesystem = cluster.getNamesystem();
    cacheManager = (SimpleReadCacheManager) namesystem.getMountManager()
        .getReadCacheManager();
    cluster.triggerHeartbeats();
    Thread.sleep(1000);
    assertEquals(cacheUsed, cacheManager.getCacheUsedForProvided());

    client = new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), conf);
    // mount new directory with 0 cache capacity.
    Map<String, String> config = new HashMap<>();
    config.put(DFS_PROVIDED_READ_CACHE_CAPACITY_FRACTION, "0");
    assertTrue(
        client.addMount(dir1.toURI().toString(), "/mounts/dir2", config));
    Thread.sleep(1000);
    assertEquals(0, cacheManager.getCacheUsedForProvided());

    namesystem.removeMount(mount);
    cluster.triggerHeartbeats();
    Thread.sleep(1000);
    assertEquals(0, cacheManager.getCacheUsedForProvided());
  }

  @Test
  public void testDynamicMountsCheckpoint() throws Exception {
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        "0.0.0.0:0");
    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf);

    File newDir = new File(new File(providedPath.toUri()), "newDir");
    String remotePath = newDir.toURI().toString();
    String mountpoint = "/mount1";
    // call mount
    DFSClient client =
        new DFSClient(cluster.getNameNode().getNameNodeAddress(), conf);

    // create the new files to mount
    newDir.mkdirs();
    createFiles(new Path(newDir.toURI()), 10, baseFileLen, "newFile",
        fileSuffix);
    assertTrue(client.addMount(remotePath, mountpoint, null));
    // verify new image!!
    verifyPaths(cluster, conf, new Path(mountpoint), new Path(newDir.toURI()),
        0, true);
    SecondaryNameNode.CommandLineOpts opts =
        new SecondaryNameNode.CommandLineOpts();
    opts.parse("-format");
    SecondaryNameNode secondary = new SecondaryNameNode(conf, opts);
    secondary.doCheckpoint();
    secondary.shutdown();

    // re-start the namenode.
    cluster.restartNameNodes();
    client = new DFSClient(cluster.getNameNode().getNameNodeAddress(), conf);

    List<MountInfo> mountInfos =
        client.listMounts(false).getMountInfos();
    assertEquals(1, mountInfos.size());
    assertEquals(MountInfo.MountStatus.CREATED,
        mountInfos.get(0).getMountStatus());
  }

  @Test
  public void testDynamicMountsCheckpointHA() throws Exception {
    conf.setInt(FixedBlockResolver.BLOCKSIZE, baseFileLen);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY, 100);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1);

    MiniDFSNNTopology topology =
        MiniDFSNNTopology.simpleHATopology(2, NetUtils.getFreeSocketPort());
    createInMemoryAliasMapImage();
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureInMemoryAliasMapAddresses(topology, conf, providedNameservice);

    cluster = startCluster(nnDirPath, 3, new StorageType[] {StorageType.DISK},
        null, false, conf, null, topology,
        new MiniDFSClusterBuilderAliasMap(conf, providedNameservice));

    cluster.waitActive();

    List<Integer> nnIndexes = cluster.getNNIndexes(providedNameservice);
    int nn0 = nnIndexes.get(0);
    cluster.transitionToActive(nn0);

    // create the new files to mount
    File newDir = new File(new File(providedPath.toUri()), "newDir");
    newDir.mkdirs();
    createFiles(new Path(newDir.toURI()), 10, baseFileLen, "newFile",
        fileSuffix);

    String mountPath = "/mount1";
    LOG.info("Calling createMount with mountpoint " + mountPath);
    // call mount
    DFSClient client =
        new DFSClient(cluster.getNameNode(nn0).getNameNodeAddress(), conf);
    client.addMount(newDir.toURI().toString(), mountPath, null);

    LOG.info("Verifying data on new mount with 1st NN");
    verifyPaths(cluster, conf, new Path(mountPath), new Path(newDir.toURI()),
        nn0, true);

    List<MountInfo> mountInfos =
        client.listMounts(false).getMountInfos();
    assertEquals(1, mountInfos.size());
    assertEquals(MountInfo.MountStatus.CREATED,
        mountInfos.get(0).getMountStatus());
    HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
        cluster.getNameNode(1));

    HATestUtil.waitForCheckpoint(cluster, 1, ImmutableList.of(170));
    HATestUtil.waitForCheckpoint(cluster, 0, ImmutableList.of(170));

    cluster.restartNameNodes();
    cluster.transitionToActive(nn0);
    client = new DFSClient(cluster.getNameNode(nn0).getNameNodeAddress(), conf);
    mountInfos = client.listMounts(false).getMountInfos();
    assertEquals(1, mountInfos.size());
    assertEquals(MountInfo.MountStatus.CREATED,
        mountInfos.get(0).getMountStatus());
    verifyPaths(cluster, conf, new Path(mountPath), new Path(newDir.toURI()),
        nn0, true);
  }
}
