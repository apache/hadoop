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
import java.io.IOException;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.Reporter;


public class TestReedSolomonDecoder extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestReedSolomonDecoder");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static int NUM_DATANODES = 3;

  Configuration conf;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;

  public void testDecoder() throws Exception {
    mySetup();
    int stripeSize = 10;
    int paritySize = 4;
    long blockSize = 8192;
    Path file1 = new Path("/user/raidtest/file1");
    Path recoveredFile1 = new Path("/user/raidtest/file1.recovered");
    Path parityFile1 = new Path("/rsraid/user/raidtest/file1");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 25, blockSize);
    FileStatus file1Stat = fileSys.getFileStatus(file1);

    conf.setInt("raid.rsdecoder.bufsize", 512);
    conf.setInt("raid.rsencoder.bufsize", 512);

    try {
      // First encode the file.
      ReedSolomonEncoder encoder = new ReedSolomonEncoder(
        conf, stripeSize, paritySize);
      short parityRepl = 1;
      encoder.encodeFile(fileSys, file1, fileSys, parityFile1, parityRepl,
        Reporter.NULL);

      // Ensure there are no corrupt files yet.
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals(corruptFiles.length, 0);

      // Now corrupt the file.
      long corruptOffset = blockSize * 5;
      FileStatus srcStat = fileSys.getFileStatus(file1);
      LocatedBlocks locations = RaidDFSUtil.getBlockLocations(dfs,
          file1.toUri().getPath(), 0, srcStat.getLen());
      corruptBlock(locations.get(5).getBlock());
      corruptBlock(locations.get(6).getBlock());
      TestBlockFixer.reportCorruptBlocks(dfs, file1, new int[]{5, 6},
          srcStat.getBlockSize());

      // Ensure file is corrupted.
      corruptFiles = RaidDFSUtil.getCorruptFiles(dfs);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], file1.toString());

      // Fix the file.
      ReedSolomonDecoder decoder = new ReedSolomonDecoder(
        conf, stripeSize, paritySize);
      decoder.decodeFile(fileSys, file1, fileSys, parityFile1,
                corruptOffset, recoveredFile1);
      assertTrue(TestRaidDfs.validateFile(
                    fileSys, recoveredFile1, file1Stat.getLen(), crc1));
    } finally {
      myTearDown();
    }
  }

  void corruptBlock(ExtendedBlock block) throws IOException {
    assertTrue("Could not corrupt block",
        dfs.corruptBlockOnDataNodes(block) > 0);
  }

  private void mySetup() throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    // make all deletions not go through Trash
    conf.set("fs.shell.delete.classname", "org.apache.hadoop.hdfs.DFSClient");

    conf.setBoolean("dfs.permissions", false);

    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    String namenode = fileSys.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
  }

  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }
}
