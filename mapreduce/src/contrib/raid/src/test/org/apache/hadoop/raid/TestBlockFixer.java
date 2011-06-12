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
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;


public class TestBlockFixer {
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
   * Tests isXorParityFile and isRsParityFile
   */
  @Test
  public void testIsParityFile() throws IOException {
    Configuration testConf = new Configuration();
    testConf.set("hdfs.raid.locations", "/raid");
    testConf.set("hdfs.raidrs.locations", "/raidrs");

    BlockFixer.BlockFixerHelper helper =
      new BlockFixer.BlockFixerHelper(testConf);

    assertFalse("incorrectly identified rs parity file as xor parity file",
                helper.isXorParityFile(new Path("/raidrs/test/test")));
    assertTrue("could not identify rs parity file",
               helper.isRsParityFile(new Path("/raidrs/test/test")));
    assertTrue("could not identify xor parity file",
               helper.isXorParityFile(new Path("/raid/test/test")));
    assertFalse("incorrectly identified xor parity file as rs parity file",
                helper.isRsParityFile(new Path("/raid/test/test")));
  }


  /**
   * Test the filtering of trash files from the list of corrupt files.
   */
  @Test
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

    assertEquals("expected 2 non-trash files but got " + files.size(),
                 2, files.size());
    for (Path p: files) {
      assertTrue("wrong file returned by filterTrash",
                 p == p1 || p == p2);
    }
  }

  @Test
  public void testBlockFixLocal() throws Exception {
    implBlockFix(true);
  }

  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  protected void implBlockFix(boolean local) throws Exception {
    LOG.info("Test testBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
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
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockFixer");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockFixer");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();
      
      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());
      
      // Corrupt blocks in two different stripes. We can fix them.
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      
      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      assertEquals("wrong number of corrupt blocks", 3,
        RaidDFSUtil.corruptBlocksInFile(dfs, file1.toUri().getPath(), 0,
          srcStat.getLen()).size());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed", 1, cnode.blockFixer.filesFixed());
      
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

    } catch (Exception e) {
      LOG.info("Test testBlockFix Exception " + e +
               StringUtils.stringifyException(e));
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
  protected void generatedBlockTestCommon(String testName, int blockToCorrupt,
                                        boolean local) throws Exception {
    LOG.info("Test " + testName + " started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFile(fileSys, file1, 1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockFixer");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockFixer");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();
      
      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());
      
      corruptBlock(locs.get(0).getBlock());
      reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);
      
      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockFixer.filesFixed());
      
      // Stop RaidNode
      cnode.stop(); cnode.join(); cnode = null;

      // The block has successfully been reconstructed.
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

      // Now corrupt the generated block.
      locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());
      corruptBlock(locs.get(0).getBlock());
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
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 3, true);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", 6, true);
  }

  @Test
  public void testParityBlockFixLocal() throws Exception {
    implParityBlockFix("testParityBlockFixLocal", true);
  }

  /**
   * Corrupt a parity file and wait for it to get fixed.
   */
  protected void implParityBlockFix(String testName, boolean local)
    throws Exception {
    LOG.info("Test " + testName + " started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    Path parityFile = new Path("/destraid/user/dhruba/raidtest/file1");
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockFixer");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockFixer");
    }
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

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 1, 2};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock());
      reportCorruptBlocks(dfs, parityFile, corruptBlockIdxs, blockSize);

      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], parityFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockFixer.filesFixed());

      long checkCRC = getCRC(fileSys, parityFile);

      assertEquals("file not fixed",
                   parityCRC, checkCRC);

    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }

  @Test
  public void testParityHarBlockFixLocal() throws Exception {
    implParityHarBlockFix("testParityHarBlockFixLocal", true);
  }

  protected void implParityHarBlockFix(String testName, boolean local)
    throws Exception {
    LOG.info("Test " + testName + " started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, 0); // Time before har = 0 days.
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    // Parity file will have 7 blocks.
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                               1, 20, blockSize);
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    // HAR block size = 2 * src block size = 2 * parity block size.
    Configuration localConf = new Configuration(conf);
    localConf.setLong("har.block.size", blockSize * 2);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockFixer");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockFixer");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      Path harDirectory =
        new Path("/destraid/user/dhruba/raidtest/raidtest" +
                 RaidNode.HAR_SUFFIX);
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 1000 * 120) {
        if (fileSys.exists(harDirectory)) {
          break;
        }
        LOG.info("Test " + testName + " waiting for har");
        Thread.sleep(1000);
      }

      Path partFile = new Path(harDirectory, "part-0");
      long partCRC = getCRC(fileSys, partFile);
      FileStatus partStat = fileSys.getFileStatus(partFile);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, partFile.toUri().getPath(), 0, partStat.getLen());
      // 7 parity blocks => 4 har blocks.
      assertEquals("wrong number of har blocks",
                   4, locs.getLocatedBlocks().size());
      cnode.stop(); cnode.join();

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 3};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock());
      reportCorruptBlocks(dfs, partFile, corruptBlockIdxs,
        partStat.getBlockSize());

      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], partFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      start = System.currentTimeMillis();
      while (cnode.blockFixer.filesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockFixer.filesFixed());

      long checkCRC = getCRC(fileSys, partFile);

      assertEquals("file not fixed",
                   partCRC, checkCRC);
    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }


  protected static DistributedFileSystem getDFS(
        Configuration conf, FileSystem dfs) throws IOException {
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    FileSystem.closeAll();
    return (DistributedFileSystem) FileSystem.get(dfsUri, clientConf);
  }

  protected void mySetup(int stripeLength, int timeBeforeHar) throws Exception {

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
    conf.set("hdfs.raid.locations", "/destraid");

    conf.setBoolean("dfs.permissions", false);

    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
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

  protected void myTearDown() throws Exception {
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

  void corruptBlock(ExtendedBlock block) throws IOException {
    assertTrue("Could not corrupt block",
        dfs.corruptBlockOnDataNodes(block) > 0);
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

