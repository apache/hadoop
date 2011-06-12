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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.RaidNode;

public class TestBlockFixerDistConcurrency extends TestBlockFixer {
  /**
   * tests that we can have 2 concurrent jobs fixing files 
   * (dist block fixer)
   */
  @Test
  public void testConcurrentJobs() throws Exception {
    LOG.info("Test testConcurrentJobs started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path file2 = new Path("/user/dhruba/raidtest/file2");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 20, blockSize);
    long crc2 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file2,
                                                          1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    long file2Len = fileSys.getFileStatus(file2).getLen();
    LOG.info("Test testConcurrentJobs created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockFixer");
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file2, destPath);
      cnode.stop(); cnode.join();

      FileStatus file1Stat = fileSys.getFileStatus(file1);
      FileStatus file2Stat = fileSys.getFileStatus(file2);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations(dfs, file1.toUri().getPath(),
                                      0, file1Stat.getLen());
      LocatedBlocks file2Loc =
        RaidDFSUtil.getBlockLocations(dfs, file2.toUri().getPath(),
                                      0, file2Stat.getLen());
      
      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());

      // corrupt file1
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(file1Loc.get(idx).getBlock());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockFixer blockFixer = (DistBlockFixer) cnode.blockFixer;
      long start = System.currentTimeMillis();

      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 1 to start");
        Thread.sleep(10);
      }
      assertEquals("job 1 not running", 1, blockFixer.jobsRunning());

      // corrupt file2
      for (int idx: corruptBlockIdxs)
        corruptBlock(file2Loc.get(idx).getBlock());
      reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      
      while (blockFixer.jobsRunning() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 2 to start");
        Thread.sleep(10);
      }
      assertEquals("2 jobs not running", 2, blockFixer.jobsRunning());

      while (blockFixer.filesFixed() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(10);
      }
      assertEquals("files not fixed", 2, blockFixer.filesFixed());

      dfs = getDFS(conf, dfs);
      
      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file2, file2Len, crc2));
    } catch (Exception e) {
      LOG.info("Test testConcurrentJobs exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }

  }

  /**
   * tests that the distributed block fixer obeys
   * the limit on how many files to fix simultaneously
   */
  @Test
  public void testMaxPendingFiles() throws Exception {
    LOG.info("Test testMaxPendingFiles started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path file2 = new Path("/user/dhruba/raidtest/file2");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 20, blockSize);
    long crc2 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file2,
                                                          1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    long file2Len = fileSys.getFileStatus(file2).getLen();
    LOG.info("Test testMaxPendingFiles created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockFixer");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingfiles", 1L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file2, destPath);
      cnode.stop(); cnode.join();

      FileStatus file1Stat = fileSys.getFileStatus(file1);
      FileStatus file2Stat = fileSys.getFileStatus(file2);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations(dfs, file1.toUri().getPath(),
                                      0, file1Stat.getLen());
      LocatedBlocks file2Loc =
        RaidDFSUtil.getBlockLocations(dfs, file2.toUri().getPath(),
                                      0, file2Stat.getLen());

      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockFixer.filesFixed());

      // corrupt file1
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(file1Loc.get(idx).getBlock());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockFixer blockFixer = (DistBlockFixer) cnode.blockFixer;
      long start = System.currentTimeMillis();

      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 1 to start");
        Thread.sleep(10);
      }
      assertEquals("job not running", 1, blockFixer.jobsRunning());

      // corrupt file2
      for (int idx: corruptBlockIdxs)
        corruptBlock(file2Loc.get(idx).getBlock());
      reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      
      // wait until both files are fixed
      while (blockFixer.filesFixed() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        // make sure the block fixer does not start a second job while
        // the first one is still running
        assertTrue("too many jobs running", blockFixer.jobsRunning() <= 1);
        Thread.sleep(10);
      }
      assertEquals("files not fixed", 2, blockFixer.filesFixed());

      dfs = getDFS(conf, dfs);
      
      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file2, file2Len, crc2));
    } catch (Exception e) {
      LOG.info("Test testMaxPendingFiles exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }

  }
}
