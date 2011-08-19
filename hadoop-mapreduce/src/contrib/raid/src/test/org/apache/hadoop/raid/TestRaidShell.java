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
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.raid.RaidNode;


public class TestRaidShell extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestRaidShell");
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
  FileSystem fileSys = null;
  RaidNode cnode = null;
  Random rand = new Random();

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
    Path parityFile = new Path(destPath, "file1");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.blockfix.classname",
             "org.apache.hadoop.raid.LocalBlockFixer");
    cnode = RaidNode.createRaidNode(null, localConf);

    try {
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop();
      cnode.join();
      cnode = null;

      FileStatus srcStat = fileSys.getFileStatus(file1);
      LocatedBlocks locations = RaidDFSUtil.getBlockLocations(
        (DistributedFileSystem) fileSys, file1.toUri().getPath(),
        0, srcStat.getLen());

      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;

      // Corrupt blocks in different stripes. We can fix them.
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs) {
        LOG.info("Corrupting block " + locations.get(idx).getBlock());
        corruptBlock(locations.get(idx).getBlock());
      }
      TestBlockFixer.reportCorruptBlocks(fileSys, file1, corruptBlockIdxs,
        srcStat.getBlockSize());

      waitForCorruptBlocks(corruptBlockIdxs.length, dfs, file1);

      // Create RaidShell and fix the file.
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[2];
      args[0] = "-recoverBlocks";
      args[1] = file1.toUri().getPath();
      ToolRunner.run(shell, args);

      waitForCorruptBlocks(0, dfs, file1);

      assertTrue(TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

      // Now corrupt and fix the parity file.
      FileStatus parityStat = fileSys.getFileStatus(parityFile);
      long parityCrc = getCRC(fileSys, parityFile);
      locations = RaidDFSUtil.getBlockLocations(
        dfs, parityFile.toUri().getPath(), 0, parityStat.getLen());
      corruptBlock(locations.get(0).getBlock());
      TestBlockFixer.reportCorruptBlocks(fileSys, parityFile, new int[]{0},
        srcStat.getBlockSize());
      waitForCorruptBlocks(1, dfs, parityFile);

      args[1] = parityFile.toUri().getPath();
      ToolRunner.run(shell, args);

      waitForCorruptBlocks(0, dfs, file1);
      assertEquals(parityCrc, getCRC(fileSys, parityFile));

    } catch (Exception e) {
      LOG.info("Test testBlockFix Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testBlockFix completed.");
  }

  private void waitForCorruptBlocks(
    int numCorruptBlocks, DistributedFileSystem dfs, Path file)
    throws Exception {
    String path = file.toUri().getPath();
    FileStatus stat = dfs.getFileStatus(file);
    long start = System.currentTimeMillis();
    long actual = 0;
    do {
      actual = RaidDFSUtil.corruptBlocksInFile(
          dfs, path, 0, stat.getLen()).size();
      if (actual == numCorruptBlocks) break;
      if (System.currentTimeMillis() - start > 120000) break;
      LOG.info("Waiting for " + numCorruptBlocks + " corrupt blocks in " +
        path + ", found " + actual);
      Thread.sleep(1000);
    } while (true);
    assertEquals(numCorruptBlocks, actual);
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
    conf.set("hdfs.raid.locations", "/destraid");

    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);

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
}
