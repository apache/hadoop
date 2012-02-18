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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.raid.RaidNode;


public class TestReedSolomonEncoder extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestReedSolomonEncoder");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static int NUM_DATANODES = 3;

  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;

  public void testEncoder() throws Exception {
    mySetup();
    int stripeSize = 10;
    int paritySize = 4;
    long blockSize = 8192;
    Path file1 = new Path("/user/raidtest/file1");
    Path parityFile1 = new Path("/rsraid/user/raidtest/file1");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 25, blockSize);
    try {
      ReedSolomonEncoder encoder = new ReedSolomonEncoder(
        conf, stripeSize, paritySize);
      short parityRepl = 1;
      encoder.encodeFile(fileSys, file1, fileSys, parityFile1, parityRepl,
        Reporter.NULL);

      FileStatus parityStat = fileSys.getFileStatus(parityFile1);
      assertEquals(4*8192*3, parityStat.getLen());

    } finally {
      myTearDown();
    }
  }

  private void mySetup() throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    // make all deletions not go through Trash
    conf.set("fs.shell.delete.classname", "org.apache.hadoop.hdfs.DFSClient");

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);

  }

  private void myTearDown() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }
}
