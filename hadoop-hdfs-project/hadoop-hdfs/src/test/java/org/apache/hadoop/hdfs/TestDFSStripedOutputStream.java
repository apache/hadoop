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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestDFSStripedOutputStream {
  public static final Log LOG = LogFactory.getLog(
      TestDFSStripedOutputStream.class);

  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.ALL);
  }

  private int dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private int parityBlocks = StripedFileTestUtil.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int stripesPerBlock = 4;
  private final int blockSize = cellSize * stripesPerBlock;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    int numDNs = dataBlocks + parityBlocks + 2;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/", null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testFileEmpty() throws Exception {
    testOneFile("/EmptyFile", 0);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws Exception {
    testOneFile("/SmallerThanOneCell", 1);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws Exception {
    testOneFile("/SmallerThanOneCell", cellSize - 1);
  }

  @Test
  public void testFileEqualsWithOneCell() throws Exception {
    testOneFile("/EqualsWithOneCell", cellSize);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize * dataBlocks - 1);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize + 123);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws Exception {
    testOneFile("/EqualsWithOneStripe", cellSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws Exception {
    testOneFile("/MoreThanOneStripe1", cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws Exception {
    testOneFile("/MoreThanOneStripe2", cellSize * dataBlocks
            + cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileLessThanFullBlockGroup() throws Exception {
    testOneFile("/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
  }

  @Test
  public void testFileFullBlockGroup() throws Exception {
    testOneFile("/FullBlockGroup", blockSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws Exception {
    testOneFile("/MoreThanABlockGroup1", blockSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws Exception {
    testOneFile("/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize+ 123);
  }


  @Test
  public void testFileMoreThanABlockGroup3() throws Exception {
    testOneFile("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
        + cellSize + 123);
  }

  private void testOneFile(String src, int writeBytes) throws Exception {
    src += "_" + writeBytes;
    Path testPath = new Path(src);

    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, testPath, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

    StripedFileTestUtil.checkData(fs, testPath, writeBytes,
        new ArrayList<DatanodeInfo>(), null);
  }
}
