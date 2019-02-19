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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
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
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LEVELDB_PATH;
import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;
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
  private final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
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
    conf.set(SingleUGIResolver.USER, singleUser);
    conf.set(SingleUGIResolver.GROUP, singleGroup);

    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);

    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR,
        nnDirPath.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
        new Path(nnDirPath, fileNameFromBlockPoolID(bpid)).toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER, "\t");

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(providedPath.toUri()).toString());
    File imageDir = new File(providedPath.toUri());
    if (!imageDir.exists()) {
      LOG.info("Creating directory: " + imageDir);
      imageDir.mkdirs();
    }

    File nnDir = new File(nnDirPath.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }

    // create 10 random files under pBASE
    for (int i=0; i < numFiles; i++) {
      File newFile = new File(
          new Path(providedPath, filePrefix + i + fileSuffix).toUri());
      if(!newFile.exists()) {
        try {
          LOG.info("Creating " + newFile.toString());
          newFile.createNewFile();
          Writer writer = new OutputStreamWriter(
              new FileOutputStream(newFile.getAbsolutePath()), "utf-8");
          for(int j=0; j < baseFileLen*i; j++) {
            writer.write("0");
          }
          writer.flush();
          writer.close();
          providedDataSize += newFile.length();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @After
  public void shutdown() throws Exception {
    try {
      if (cluster != null) {
        cluster.shutdown(true, true);
      }
    } finally {
      cluster = null;
    }
  }

  void createImage(TreeWalk t, Path out,
      Class<? extends BlockResolver> blockIdsClass) throws Exception {
    createImage(t, out, blockIdsClass, "", TextFileRegionAliasMap.class);
  }

  void createImage(TreeWalk t, Path out,
      Class<? extends BlockResolver> blockIdsClass, String clusterID,
      Class<? extends BlockAliasMap> aliasMapClass) throws Exception {
    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(conf);
    opts.output(out.toString())
        .blocks(aliasMapClass)
        .blockIds(blockIdsClass)
        .clusterID(clusterID)
        .blockPoolID(bpid);
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        w.accept(e);
      }
    }
  }
  void startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat) throws IOException {
    startCluster(nspath, numDatanodes, storageTypes, storageTypesPerDatanode,
        doFormat, null);
  }

  void startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, String[] racks) throws IOException {
    startCluster(nspath, numDatanodes,
        storageTypes, storageTypesPerDatanode,
        doFormat, racks, null,
        new MiniDFSCluster.Builder(conf));
  }

  void startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat, String[] racks,
      MiniDFSNNTopology topo,
      MiniDFSCluster.Builder builder) throws IOException {
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());

    builder.format(doFormat)
        .manageNameDfsDirs(doFormat)
        .numDataNodes(numDatanodes)
        .racks(racks);
    if (storageTypesPerDatanode != null) {
      builder.storageTypes(storageTypesPerDatanode);
    } else if (storageTypes != null) {
      builder.storagesPerDatanode(storageTypes.length)
          .storageTypes(storageTypes);
    }
    if (topo != null) {
      builder.nnTopology(topo);
      // If HA or Federation is enabled and formatting is set to false,
      // copy the FSImage to all Namenode directories.
      if ((topo.isHA() || topo.isFederated()) && !doFormat) {
        builder.manageNameDfsDirs(true);
        builder.enableManagedDfsDirsRedundancy(false);
        builder.manageNameDfsSharedDirs(true);
        List<File> nnDirs =
            getProvidedNamenodeDirs(conf.get(HDFS_MINIDFS_BASEDIR), topo);
        for (File nnDir : nnDirs) {
          MiniDFSCluster.copyNameDirs(
              Collections.singletonList(nspath.toUri()),
              Collections.singletonList(fileAsURI(nnDir)),
              conf);
        }
      }
    }
    cluster = builder.build();
    cluster.waitActive();
  }

  private static List<File> getProvidedNamenodeDirs(String baseDir,
      MiniDFSNNTopology topo) {
    List<File> nnDirs = new ArrayList<>();
    int nsCounter = 0;
    for (MiniDFSNNTopology.NSConf nsConf : topo.getNameservices()) {
      int nnCounter = nsCounter;
      for (MiniDFSNNTopology.NNConf nnConf : nsConf.getNNs()) {
        if (providedNameservice.equals(nsConf.getId())) {
          // only add the first one
          File[] nnFiles =
              MiniDFSCluster.getNameNodeDirectory(
                  baseDir, nsCounter, nnCounter);
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
    startCluster(nnDirPath, 0,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
        false);

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
    conf.setClass(ImageWriter.Options.UGI_CLASS,
        SingleUGIResolver.class, UGIResolver.class);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    int numDatanodes = 10;
    startCluster(nnDirPath, numDatanodes,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
        false);
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
    conf.setInt(FixedBlockMultiReplicaResolver.REPLICATION, targetReplication);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockMultiReplicaResolver.class);
    // make the last Datanode with only DISK
    startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false);
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
    return ret;
  }

  private void verifyFileSystemContents(int nnIndex) throws Exception {
    FileSystem fs = cluster.getFileSystem(nnIndex);
    int count = 0;
    // read NN metadata, verify contents match
    for (TreePath e : new FSTreeWalk(providedPath, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(providedPath, rs.getPath());
      LOG.info("path: " + hp.toUri().getPath());
      e.accept(count++);
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);
      assertEquals(hp.toUri().getPath(), hs.getPath().toUri().getPath());
      assertEquals(rs.getPermission(), hs.getPermission());
      assertEquals(rs.getOwner(), hs.getOwner());
      assertEquals(rs.getGroup(), hs.getGroup());

      if (rs.isFile()) {
        assertEquals(rs.getLen(), hs.getLen());
        try (ReadableByteChannel i = Channels.newChannel(
              new FileInputStream(new File(rs.getPath().toUri())))) {
          try (ReadableByteChannel j = Channels.newChannel(
                fs.open(hs.getPath()))) {
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
    startCluster(nnDirPath, 2, null,
        new StorageType[][] {
            {StorageType.DISK},
            {StorageType.DISK}},
        true);
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
    checkUniqueness(locations);
    return locations;
  }

  /**
   * verify that the given locations are all unique.
   * @param locations
   */
  private void checkUniqueness(DatanodeInfo[] locations) {
    Set<String> set = new HashSet<>();
    for (DatanodeInfo info: locations) {
      assertFalse("All locations should be unique",
          set.contains(info.getDatanodeUuid()));
      set.add(info.getDatanodeUuid());
    }
  }

  /**
   * Tests setting replication of provided files.
   * @throws Exception
   */
  @Test(timeout=50000)
  public void testSetReplicationForProvidedFiles() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // 10 Datanodes with both DISK and PROVIDED storage
    startCluster(nnDirPath, 10,
        new StorageType[]{
            StorageType.PROVIDED, StorageType.DISK},
        null,
        false);
    setAndUnsetReplication("/" + filePrefix + (numFiles - 1) + fileSuffix);
  }

  private void setAndUnsetReplication(String filename) throws Exception {
    Path file = new Path(filename);
    FileSystem fs = cluster.getFileSystem();
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
    int defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fs,
        file, (short) defaultReplication, 10000);
    getAndCheckBlockLocations(client, filename, baseFileLen, 1,
        defaultReplication);
  }

  @Test(timeout=30000)
  public void testProvidedDatanodeFailures() throws Exception {
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
            FixedBlockResolver.class);
    startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false);

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
    startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false);

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
    // 3 Datanodes, 2 PROVIDED and other DISK
    startCluster(nnDirPath, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false);

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
        FixedBlockResolver.class, clusterID, TextFileRegionAliasMap.class);
    // 2 Datanodes, 1 PROVIDED and other DISK
    startCluster(nnDirPath, 2, null,
        new StorageType[][] {
            {StorageType.PROVIDED, StorageType.DISK},
            {StorageType.DISK}},
        false);
    NameNode nn = cluster.getNameNode();
    assertEquals(clusterID, nn.getNamesystem().getClusterId());
  }

  @Test(timeout=30000)
  public void testNumberOfProvidedLocations() throws Exception {
    // set default replication to 4
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 4);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // start with 4 PROVIDED location
    startCluster(nnDirPath, 4,
        new StorageType[]{
            StorageType.PROVIDED, StorageType.DISK},
        null,
        false);
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
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 4);
    createImage(new FSTreeWalk(providedPath, conf), nnDirPath,
        FixedBlockResolver.class);
    // start with 4 PROVIDED location
    startCluster(nnDirPath, 4,
        new StorageType[]{
            StorageType.PROVIDED, StorageType.DISK},
        null,
        false);
    int expectedLocations = 4;
    for (int i = 0; i < numFiles; i++) {
      verifyFileLocation(i, expectedLocations);
    }
  }

  private File createInMemoryAliasMapImage() throws Exception {
    conf.setClass(ImageWriter.Options.UGI_CLASS, FsUGIResolver.class,
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

    createImage(new FSTreeWalk(providedPath, conf),
        nnDirPath,
        FixedBlockResolver.class, clusterID,
        LevelDBFileRegionAliasMap.class);

    return tempDirectory;
  }

  @Test
  public void testInMemoryAliasMap() throws Exception {
    File aliasMapImage = createInMemoryAliasMapImage();
    // start cluster with two datanodes,
    // each with 1 PROVIDED volume and other DISK volume
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    startCluster(nnDirPath, 2,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false);
    verifyFileSystemContents(0);
    FileUtils.deleteDirectory(aliasMapImage);
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
  private static class MiniDFSClusterBuilderAliasMap
      extends MiniDFSCluster.Builder {

    MiniDFSClusterBuilderAliasMap(Configuration conf) {
      super(conf);
    }

    @Override
    public MiniDFSCluster build() throws IOException {
      return new MiniDFSClusterAliasMap(this);
    }
  }

  /**
   * Extends {@link MiniDFSCluster} to correctly configure the InMemoryAliasMap.
   */
  private static class MiniDFSClusterAliasMap extends MiniDFSCluster {

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

      if (providedNameservice.equals(nameserviceId)) {
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
        new MiniDFSNNTopology[] {
            MiniDFSNNTopology.simpleHATopology(),
            MiniDFSNNTopology.simpleFederatedTopology(3),
            MiniDFSNNTopology.simpleHAFederatedTopology(3)
        };

    for (MiniDFSNNTopology topology : topologies) {
      LOG.info("Starting test with topology with HA = {}, federation = {}",
          topology.isHA(), topology.isFederated());
      setSeed();
      createInMemoryAliasMapImage();
      conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
      conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
      providedNameservice = topology.getNameservices().get(0).getId();
      // configure the AliasMap addresses
      configureAliasMapAddresses(topology, providedNameservice);
      startCluster(nnDirPath, 2,
          new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
          null, false, null, topology,
          new MiniDFSClusterBuilderAliasMap(conf));

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
    startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false);

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
    conf.setClass(ImageWriter.Options.UGI_CLASS, FsUGIResolver.class,
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
      startCluster(nnDirPath, racks.length,
          new StorageType[]{StorageType.PROVIDED, StorageType.DISK},
          null, false, racks);
      verifyFileSystemContents(0);
      setAndUnsetReplication("/" + filePrefix + (numFiles - 1) + fileSuffix);
      cluster.shutdown();
    }
  }

  @Test
  public void testBootstrapAliasMap() throws Exception {
    int numNamenodes = 3;
    MiniDFSNNTopology topology =
        MiniDFSNNTopology.simpleHATopology(numNamenodes);

    createInMemoryAliasMapImage();
    conf.setBoolean(DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    conf.setInt(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_LOAD_RETRIES, 10);
    providedNameservice = topology.getNameservices().get(0).getId();
    // configure the AliasMap addresses
    configureAliasMapAddresses(topology, providedNameservice);
    startCluster(nnDirPath, 2,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK}, null,
        false, null, topology, new MiniDFSClusterBuilderAliasMap(conf));

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
    Optional<Block> marker = Optional.empty();
    while (true) {
      InMemoryAliasMapProtocol.IterationResult result = aliasMap.list(marker);
      fileRegions.addAll(result.getFileRegions());
      marker = result.getNextBlock();
      if (!marker.isPresent()) {
        break;
      }
    }
    return fileRegions;
  }
}
