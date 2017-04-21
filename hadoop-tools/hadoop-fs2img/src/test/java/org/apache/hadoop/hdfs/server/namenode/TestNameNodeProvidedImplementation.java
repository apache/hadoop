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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockFormatProvider;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockProvider;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.FileRegionProvider;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.hdfs.server.common.TextFileRegionProvider;
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
  final Path NAMEPATH = new Path(BASE, "providedDir");;
  final Path NNDIRPATH = new Path(BASE, "nnDir");
  final Path BLOCKFILE = new Path(NNDIRPATH, "blocks.csv");
  final String SINGLEUSER = "usr1";
  final String SINGLEGROUP = "grp1";

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

    conf.setClass(DFSConfigKeys.DFS_NAMENODE_BLOCK_PROVIDER_CLASS,
        BlockFormatProvider.class, BlockProvider.class);
    conf.setClass(DFSConfigKeys.DFS_PROVIDER_CLASS,
        TextFileRegionProvider.class, FileRegionProvider.class);
    conf.setClass(DFSConfigKeys.DFS_PROVIDER_BLK_FORMAT_CLASS,
        TextFileRegionFormat.class, BlockFormat.class);

    conf.set(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_WRITE_PATH,
        BLOCKFILE.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_READ_PATH,
        BLOCKFILE.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER, ",");

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
    for (int i=0; i < 10; i++) {
      File newFile = new File(new Path(NAMEPATH, "file" + i).toUri());
      if(!newFile.exists()) {
        try {
          LOG.info("Creating " + newFile.toString());
          newFile.createNewFile();
          Writer writer = new OutputStreamWriter(
              new FileOutputStream(newFile.getAbsolutePath()), "utf-8");
          for(int j=0; j < 10*i; j++) {
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
        .blocks(TextFileRegionFormat.class)
        .blockIds(blockIdsClass);
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        w.accept(e);
      }
    }
  }

  void startCluster(Path nspath, int numDatanodes,
      StorageType[] storageTypes,
      StorageType[][] storageTypesPerDatanode)
      throws IOException {
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());

    if (storageTypesPerDatanode != null) {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(false)
          .manageNameDfsDirs(false)
          .numDataNodes(numDatanodes)
          .storageTypes(storageTypesPerDatanode)
          .build();
    } else if (storageTypes != null) {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(false)
          .manageNameDfsDirs(false)
          .numDataNodes(numDatanodes)
          .storagesPerDatanode(storageTypes.length)
          .storageTypes(storageTypes)
          .build();
    } else {
      cluster = new MiniDFSCluster.Builder(conf)
          .format(false)
          .manageNameDfsDirs(false)
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
    startCluster(NNDIRPATH, 0, new StorageType[] {StorageType.PROVIDED}, null);

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
    startCluster(NNDIRPATH, 1, new StorageType[] {StorageType.PROVIDED}, null);
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
          {StorageType.DISK}}
        );
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
    startCluster(NNDIRPATH, 3, new StorageType[] {StorageType.PROVIDED}, null);
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
}
