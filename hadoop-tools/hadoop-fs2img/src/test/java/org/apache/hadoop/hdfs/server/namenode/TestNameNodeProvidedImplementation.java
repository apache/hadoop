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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;
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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestNameNodeProvidedImplementation {

  @Rule public TestName name = new TestName();
  public static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodeProvidedImplementation.class);

  final Random r = new Random();
  final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
  final Path BASE = new Path(fBASE.toURI().toString());
  final Path NAMEPATH = new Path(BASE, "providedDir");
  final Path NNDIRPATH = new Path(BASE, "nnDir");
  final Path BLOCKFILE = new Path(NNDIRPATH, "blocks.csv");
  final String SINGLEUSER = "usr1";
  final String SINGLEGROUP = "grp1";
  private final int numFiles = 10;
  private final String filePrefix = "file";
  private final String fileSuffix = ".dat";
  private final int baseFileLen = 1024;

  Configuration conf;
  MiniDFSCluster cluster;

  @Before
  public void setSeed() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
    conf = new HdfsConfiguration();
    conf.set(SingleUGIResolver.USER, SINGLEUSER);
    conf.set(SingleUGIResolver.GROUP, SINGLEGROUP);

    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
        DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);

    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_PATH,
        BLOCKFILE.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_PATH,
        BLOCKFILE.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER, ",");

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
        new File(NAMEPATH.toUri()).toString());
    File imageDir = new File(NAMEPATH.toUri());
    if (!imageDir.exists()) {
      LOG.info("Creating directory: " + imageDir);
      imageDir.mkdirs();
    }

    File nnDir = new File(NNDIRPATH.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }

    // create 10 random files under BASE
    for (int i=0; i < numFiles; i++) {
      File newFile = new File(
          new Path(NAMEPATH, filePrefix + i + fileSuffix).toUri());
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
    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(conf);
    opts.output(out.toString())
        .blocks(TextFileRegionAliasMap.class)
        .blockIds(blockIdsClass);
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        w.accept(e);
      }
    }
  }

  void startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode,
      boolean doFormat)
      throws IOException {
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());

    if (storageTypesPerDatanode != null) {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(doFormat)
          .manageNameDfsDirs(doFormat)
          .numDataNodes(numDatanodes)
          .storageTypes(storageTypesPerDatanode)
          .build();
    } else if (storageTypes != null) {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(doFormat)
          .manageNameDfsDirs(doFormat)
          .numDataNodes(numDatanodes)
          .storagesPerDatanode(storageTypes.length)
          .storageTypes(storageTypes)
          .build();
    } else {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(doFormat)
          .manageNameDfsDirs(doFormat)
          .numDataNodes(numDatanodes)
          .build();
    }
    cluster.waitActive();
  }

  @Test(timeout = 20000)
  public void testLoadImage() throws Exception {
    final long seed = r.nextLong();
    LOG.info("NAMEPATH: " + NAMEPATH);
    createImage(new RandomTreeWalk(seed), NNDIRPATH, FixedBlockResolver.class);
    startCluster(NNDIRPATH, 0, new StorageType[] {StorageType.PROVIDED},
        null, false);

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
      assertEquals(SINGLEUSER, hs.getOwner());
      assertEquals(SINGLEGROUP, hs.getGroup());
      assertEquals(rs.getAccessTime(), hs.getAccessTime());
      assertEquals(rs.getModificationTime(), hs.getModificationTime());
    }
  }

  @Test(timeout=20000)
  public void testBlockLoad() throws Exception {
    conf.setClass(ImageWriter.Options.UGI_CLASS,
        SingleUGIResolver.class, UGIResolver.class);
    createImage(new FSTreeWalk(NAMEPATH, conf), NNDIRPATH,
        FixedBlockResolver.class);
    startCluster(NNDIRPATH, 1, new StorageType[] {StorageType.PROVIDED},
        null, false);
  }

  @Test(timeout=500000)
  public void testDefaultReplication() throws Exception {
    int targetReplication = 2;
    conf.setInt(FixedBlockMultiReplicaResolver.REPLICATION, targetReplication);
    createImage(new FSTreeWalk(NAMEPATH, conf), NNDIRPATH,
        FixedBlockMultiReplicaResolver.class);
    // make the last Datanode with only DISK
    startCluster(NNDIRPATH, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED},
            {StorageType.PROVIDED},
            {StorageType.DISK}},
        false);
    // wait for the replication to finish
    Thread.sleep(50000);

    FileSystem fs = cluster.getFileSystem();
    int count = 0;
    for (TreePath e : new FSTreeWalk(NAMEPATH, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(NAMEPATH, rs.getPath());
      LOG.info("hp " + hp.toUri().getPath());
      //skip HDFS specific files, which may have been created later on.
      if (hp.toString().contains("in_use.lock")
          || hp.toString().contains("current")) {
        continue;
      }
      e.accept(count++);
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);

      if (rs.isFile()) {
        BlockLocation[] bl = fs.getFileBlockLocations(
            hs.getPath(), 0, hs.getLen());
        int i = 0;
        for(; i < bl.length; i++) {
          int currentRep = bl[i].getHosts().length;
          assertEquals(targetReplication , currentRep);
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

  @Test(timeout=30000)
  public void testBlockRead() throws Exception {
    conf.setClass(ImageWriter.Options.UGI_CLASS,
        FsUGIResolver.class, UGIResolver.class);
    createImage(new FSTreeWalk(NAMEPATH, conf), NNDIRPATH,
        FixedBlockResolver.class);
    startCluster(NNDIRPATH, 3, new StorageType[] {StorageType.PROVIDED},
        null, false);
    FileSystem fs = cluster.getFileSystem();
    Thread.sleep(2000);
    int count = 0;
    // read NN metadata, verify contents match
    for (TreePath e : new FSTreeWalk(NAMEPATH, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(NAMEPATH, rs.getPath());
      LOG.info("hp " + hp.toUri().getPath());
      //skip HDFS specific files, which may have been created later on.
      if(hp.toString().contains("in_use.lock")
          || hp.toString().contains("current")) {
        continue;
      }
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
    //create a sample file that is not provided
    DFSTestUtil.createFile(fs, path, false, (int) blockLen,
        fileLen, blockLen, replication, 0, true);
    return fs.getFileBlockLocations(path, 0, fileLen);
  }

  @Test
  public void testClusterWithEmptyImage() throws IOException {
    // start a cluster with 2 datanodes without any provided storage
    startCluster(NNDIRPATH, 2, null,
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
      String filename, int expectedLocations) throws IOException {
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(
        filename, 0, baseFileLen);
    //given the start and length in the above call,
    //only one LocatedBlock in LocatedBlocks
    assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    LocatedBlock locatedBlock = locatedBlocks.getLocatedBlocks().get(0);
    assertEquals(expectedLocations, locatedBlock.getLocations().length);
    return locatedBlock.getLocations();
  }

  /**
   * Tests setting replication of provided files.
   * @throws Exception
   */
  @Test
  public void testSetReplicationForProvidedFiles() throws Exception {
    createImage(new FSTreeWalk(NAMEPATH, conf), NNDIRPATH,
        FixedBlockResolver.class);
    startCluster(NNDIRPATH, 2, null,
        new StorageType[][]{
            {StorageType.PROVIDED},
            {StorageType.DISK}},
        false);

    String filename = "/" + filePrefix + (numFiles - 1) + fileSuffix;
    Path file = new Path(filename);
    FileSystem fs = cluster.getFileSystem();

    //set the replication to 2, and test that the file has
    //the required replication.
    fs.setReplication(file, (short) 2);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fs,
        file, (short) 2, 10000);
    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), cluster.getConfiguration(0));
    getAndCheckBlockLocations(client, filename, 2);

    //set the replication back to 1
    fs.setReplication(file, (short) 1);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fs,
        file, (short) 1, 10000);
    //the only replica left should be the PROVIDED datanode
    DatanodeInfo[] infos = getAndCheckBlockLocations(client, filename, 1);
    assertEquals(cluster.getDataNodes().get(0).getDatanodeUuid(),
        infos[0].getDatanodeUuid());
  }

  @Test
  public void testProvidedDatanodeFailures() throws Exception {
    createImage(new FSTreeWalk(NAMEPATH, conf), NNDIRPATH,
            FixedBlockResolver.class);
    startCluster(NNDIRPATH, 3, null,
        new StorageType[][] {
            {StorageType.PROVIDED},
            {StorageType.PROVIDED},
            {StorageType.DISK}},
        false);

    DataNode providedDatanode1 = cluster.getDataNodes().get(0);
    DataNode providedDatanode2 = cluster.getDataNodes().get(1);

    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
            cluster.getNameNodePort()), cluster.getConfiguration(0));

    if (numFiles >= 1) {
      String filename = "/" + filePrefix + (numFiles - 1) + fileSuffix;

      DatanodeInfo[] dnInfos = getAndCheckBlockLocations(client, filename, 1);
      //the location should be one of the provided DNs available
      assertTrue(
          dnInfos[0].getDatanodeUuid().equals(
              providedDatanode1.getDatanodeUuid())
          || dnInfos[0].getDatanodeUuid().equals(
              providedDatanode2.getDatanodeUuid()));

      //stop the 1st provided datanode
      MiniDFSCluster.DataNodeProperties providedDNProperties1 =
          cluster.stopDataNode(0);

      //make NameNode detect that datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(
          cluster.getNameNode(),
          providedDatanode1.getDatanodeId().getXferAddr());

      //should find the block on the 2nd provided datanode
      dnInfos = getAndCheckBlockLocations(client, filename, 1);
      assertEquals(providedDatanode2.getDatanodeUuid(),
          dnInfos[0].getDatanodeUuid());

      // stop the 2nd provided datanode
      MiniDFSCluster.DataNodeProperties providedDNProperties2 =
          cluster.stopDataNode(0);
      // make NameNode detect that datanode is down
      BlockManagerTestUtil.noticeDeadDatanode(
          cluster.getNameNode(),
          providedDatanode2.getDatanodeId().getXferAddr());
      getAndCheckBlockLocations(client, filename, 0);

      //restart the provided datanode
      cluster.restartDataNode(providedDNProperties1, true);
      cluster.waitActive();

      //should find the block on the 1st provided datanode now
      dnInfos = getAndCheckBlockLocations(client, filename, 1);
      //not comparing UUIDs as the datanode can now have a different one.
      assertEquals(providedDatanode1.getDatanodeId().getXferAddr(),
          dnInfos[0].getXferAddr());
    }
  }
}
