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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;


public class TestBlockFixer extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestBlockFixer");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR,
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  String hftp = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;
  Random rand = new Random();

  /**
   * Test the filtering of trash files from the list of corrupt files.
   */
  public void testTrashFilter() {
    List<Path> files = new LinkedList<Path>();
    // Paths that do not match the trash pattern.
    Path p1 = new Path("/user/raid/raidtest/f1");
    Path p2 = new Path("/user/.Trash/");
    // Paths that match the trash pattern.
    Path p3 = new Path("/user/raid/.Trash/raidtest/f1");
    Path p4 = new Path("/user/raid/.Trash/");
    files.add(p1);
    files.add(p3);
    files.add(p4);
    files.add(p2);

    Configuration conf = new Configuration();
    RaidUtils.filterTrash(conf, files);

    assertEquals(2, files.size());
    for (Path p: files) {
      assertTrue(p == p1 || p == p2);
    }
  }

  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  public void testBlockFix() throws Exception {
    LOG.info("Test testBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1);
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();

      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 0);
      assertEquals(0, cnode.blockFixer.filesFixed());

      // Corrupt blocks in two different stripes. We can fix them.
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);

      corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], file1.toUri().getPath());
      assertEquals(3,
        RaidDFSUtil.corruptBlocksInFile(dfs, file1.toUri().getPath(), 0,
          srcStat.getLen()).size());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals(1, cnode.blockFixer.filesFixed());

      dfs = getDFS(conf, dfs);
      assertTrue(TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

    } catch (Exception e) {
      LOG.info("Test testBlockFix Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testBlockFix completed.");
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  public void testGeneratedBlock() throws Exception {
    LOG.info("Test testGeneratedBlock started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1);
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFile(fileSys, file1, 1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testGeneratedBlock created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.setLong("raid.blockfix.filespertask", 2L);
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();

      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 0);
      assertEquals(0, cnode.blockFixer.filesFixed());

      corruptBlock(locs.get(0).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);

      corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], file1.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testGeneratedBlock waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals(1, cnode.blockFixer.filesFixed());

      // Stop RaidNode
      cnode.stop(); cnode.join(); cnode = null;

      // The block has successfully been reconstructed.
      dfs = getDFS(conf, dfs);
      assertTrue(TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

      // Now corrupt the generated block.
      locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());
      corruptBlock(locs.get(0).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);

      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      try {
        TestRaidDfs.validateFile(dfs, file1, file1Len, crc1);
        fail("Expected exception not thrown");
      } catch (org.apache.hadoop.fs.ChecksumException ce) {
      } catch (org.apache.hadoop.hdfs.BlockMissingException bme) {
      }
    } catch (Exception e) {
      LOG.info("Test testGeneratedBlock Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testGeneratedBlock completed.");
  }

  /**
   * Corrupt a parity file and wait for it to get fixed.
   */
  public void testParityBlockFix() throws Exception {
    LOG.info("Test testParityBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1);
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    Path parityFile = new Path("/destraid/user/dhruba/raidtest/file1");
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testParityBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();

      long parityCRC = getCRC(fileSys, parityFile);

      FileStatus parityStat = fileSys.getFileStatus(parityFile);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, parityFile.toUri().getPath(), 0, parityStat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 0);
      assertEquals(0, cnode.blockFixer.filesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 1, 2};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, parityFile, corruptBlockIdxs, blockSize);

      corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], parityFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testParityBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals(1, cnode.blockFixer.filesFixed());

      long checkCRC = getCRC(fileSys, parityFile);

      assertEquals(parityCRC, checkCRC);

    } catch (Exception e) {
      LOG.info("Test testParityBlockFix Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testParityBlockFix completed.");
  }

  public void testParityHarBlockFix() throws Exception {
    LOG.info("Test testParityHarBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, 0); // Time before har = 0 days.
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    // Parity file will have 7 blocks.
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testParityHarBlockFix created test files");

    // create an instance of the RaidNode
    // HAR block size = 2 * src block size = 2 * parity block size.
    Configuration localConf = new Configuration(conf);
    localConf.setLong("har.block.size", blockSize * 2);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      Path harDirectory =
        new Path("/destraid/user/dhruba/raidtest/raidtest" + RaidNode.HAR_SUFFIX);
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 1000 * 120) {
        if (fileSys.exists(harDirectory)) {
          break;
        }
        LOG.info("Test testParityHarBlockFix waiting for har");
        Thread.sleep(1000);
      }

      Path partFile = new Path(harDirectory, "part-0");
      long partCRC = getCRC(fileSys, partFile);
      FileStatus partStat = fileSys.getFileStatus(partFile);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, partFile.toUri().getPath(), 0, partStat.getLen());
      // 7 parity blocks => 4 har blocks.
      assertEquals(4, locs.getLocatedBlocks().size());
      cnode.stop(); cnode.join();

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 0);
      assertEquals(0, cnode.blockFixer.filesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 3};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, partFile, corruptBlockIdxs,
        partStat.getBlockSize());

      corruptFiles = RaidDFSUtil.getCorruptFiles(conf);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], partFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testParityHarBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals(1, cnode.blockFixer.filesFixed());

      long checkCRC = getCRC(fileSys, partFile);

      assertEquals(partCRC, checkCRC);
    } catch (Exception e) {
      LOG.info("Test testParityHarBlockFix Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testParityHarBlockFix completed.");
  }

  private static DistributedFileSystem getDFS(
        Configuration conf, FileSystem dfs) throws IOException {
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    FileSystem.closeAll();
    return (DistributedFileSystem) FileSystem.get(dfsUri, clientConf);
  }

  private void mySetup(int stripeLength, int timeBeforeHar) throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // make all deletions not go through Trash
    conf.set("fs.shell.delete.classname", "org.apache.hadoop.hdfs.DFSClient");

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:0");
    conf.setInt("hdfs.raid.stripeLength", stripeLength);
    conf.set("hdfs.raid.locs", "/destraid");

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);

    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"/user/dhruba/raidtest\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<erasureCode>xor</erasureCode> " +
                        "<destPath> /destraid</destPath> " +
                        "<property> " +
                          "<name>targetReplication</name> " +
                          "<value>1</value> " +
                          "<description>after RAIDing, decrease the replication factor of a file to this value." +
                          "</description> " +
                        "</property> " +
                        "<property> " +
                          "<name>metaReplication</name> " +
                          "<value>1</value> " +
                          "<description> replication factor of parity file" +
                          "</description> " +
                        "</property> " +
                        "<property> " +
                          "<name>modTimePeriod</name> " +
                          "<value>2000</value> " +
                          "<description> time (milliseconds) after a file is modified to make it " +
                                         "a candidate for RAIDing " +
                          "</description> " +
                        "</property> ";
    if (timeBeforeHar >= 0) {
      str +=
                        "<property> " +
                          "<name>time_before_har</name> " +
                          "<value>" + timeBeforeHar + "</value> " +
                          "<description> amount of time waited before har'ing parity files" +
                          "</description> " +
                        "</property> ";
    }

    str +=
                     "</policy>" +
                   "</srcPath>" +
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  private long getCRC(FileSystem fs, Path p) throws IOException {
    CRC32 crc = new CRC32();
    FSDataInputStream stm = fs.open(p);
    for (int b = 0; b > 0; b = stm.read()) {
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }

  void corruptBlock(String blockName) throws IOException {
    assertTrue("Could not corrupt block",
        dfs.corruptBlockOnDataNodes(blockName) > 0);
  }

  static void reportCorruptBlocks(FileSystem fs, Path file, int[] idxs,
    long blockSize) throws IOException {

    FSDataInputStream in = fs.open(file);
    for (int idx: idxs) {
      long offset = idx * blockSize;
      LOG.info("Reporting corrupt block " + file + ":" + offset);
      in.seek(offset);
      try {
        in.readFully(new byte[(int)blockSize]);
        fail("Expected exception not thrown for " + file + ":" + offset);
      } catch (org.apache.hadoop.fs.ChecksumException e) {
      } catch (org.apache.hadoop.hdfs.BlockMissingException bme) {
      }
    }
  }
}

