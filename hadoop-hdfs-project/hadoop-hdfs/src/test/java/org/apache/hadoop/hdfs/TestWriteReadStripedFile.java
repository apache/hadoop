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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class TestWriteReadStripedFile {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestWriteReadStripedFile.class);
  private final ErasureCodingPolicy ecPolicy =
      SystemErasureCodingPolicies.getByID(
          SystemErasureCodingPolicies.RS_3_2_POLICY_ID);
  private final int cellSize = ecPolicy.getCellSize();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int numDNs = dataBlocks + parityBlocks;
  private final int stripesPerBlock = 2;
  private final int blockSize = stripesPerBlock * cellSize;
  private final int blockGroupSize = blockSize * dataBlocks;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf = new HdfsConfiguration();

  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(BlockPlacementPolicy.LOG, Level.TRACE);
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.mkdirs(new Path("/ec"));
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/ec",
        ecPolicy.getName());
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testFileEmpty() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/EmptyFile", 0);
    testOneFileUsingDFSStripedInputStream("/ec/EmptyFile2", 0, true);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneCell", 1);
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneCell2", 1, true);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneCell",
        cellSize - 1);
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneCell2",
        cellSize - 1, true);
  }

  @Test
  public void testFileEqualsWithOneCell() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/EqualsWithOneCell", cellSize);
    testOneFileUsingDFSStripedInputStream("/ec/EqualsWithOneCell2",
        cellSize, true);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneStripe",
        cellSize * dataBlocks - 1);
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneStripe2",
        cellSize * dataBlocks - 1, true);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneStripe",
        cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/ec/SmallerThanOneStripe2",
        cellSize + 123, true);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/EqualsWithOneStripe",
        cellSize * dataBlocks);
    testOneFileUsingDFSStripedInputStream("/ec/EqualsWithOneStripe2",
        cellSize * dataBlocks, true);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanOneStripe1",
        cellSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanOneStripe12",
        cellSize * dataBlocks + 123, true);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanOneStripe2",
        cellSize * dataBlocks + cellSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanOneStripe22",
        cellSize * dataBlocks + cellSize * dataBlocks + 123, true);
  }

  @Test
  public void testLessThanFullBlockGroup() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
    testOneFileUsingDFSStripedInputStream("/ec/LessThanFullBlockGroup2",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize, true);
  }

  @Test
  public void testFileFullBlockGroup() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/FullBlockGroup",
        blockSize * dataBlocks);
    testOneFileUsingDFSStripedInputStream("/ec/FullBlockGroup2",
        blockSize * dataBlocks, true);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup1",
        blockSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup12",
        blockSize * dataBlocks + 123, true);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup22",
        blockSize * dataBlocks + cellSize + 123, true);
  }


  @Test
  public void testFileMoreThanABlockGroup3() throws Exception {
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/ec/MoreThanABlockGroup32",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123, true);
  }

  private void testOneFileUsingDFSStripedInputStream(String src, int fileLength)
      throws Exception {
    testOneFileUsingDFSStripedInputStream(src, fileLength, false);
  }

  private void testOneFileUsingDFSStripedInputStream(String src, int fileLength,
      boolean withDataNodeFailure) throws Exception {
    final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
    Path srcPath = new Path(src);
    DFSTestUtil.writeFile(fs, srcPath, new String(expected));
    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

    StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

    if (withDataNodeFailure) {
      int dnIndex = 1; // TODO: StripedFileTestUtil.random.nextInt(dataBlocks);
      LOG.info("stop DataNode " + dnIndex);
      stopDataNode(srcPath, dnIndex);
    }

    byte[] smallBuf = new byte[1024];
    byte[] largeBuf = new byte[fileLength + 100];
    StripedFileTestUtil.verifyPread(fs, srcPath, fileLength, expected,
        largeBuf);

    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        largeBuf);
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength, ecPolicy,
        blockGroupSize);
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        ByteBuffer.allocate(fileLength + 100));
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        smallBuf);
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        ByteBuffer.allocate(1024));
  }

  private void stopDataNode(Path path, int failedDNIdx)
      throws IOException {
    BlockLocation[] locs = fs.getFileBlockLocations(path, 0, cellSize);
    if (locs != null && locs.length > 0) {
      String name = (locs[0].getNames())[failedDNIdx];
      for (DataNode dn : cluster.getDataNodes()) {
        int port = dn.getXferPort();
        if (name.contains(Integer.toString(port))) {
          dn.shutdown();
          break;
        }
      }
    }
  }

  @Test
  public void testWriteReadUsingWebHdfs() throws Exception {
    int fileLength = blockSize * dataBlocks + cellSize + 123;

    final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
    FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
        WebHdfsConstants.WEBHDFS_SCHEME);
    Path srcPath = new Path("/testWriteReadUsingWebHdfs");
    DFSTestUtil.writeFile(fs, srcPath, new String(expected));

    StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

    byte[] smallBuf = new byte[1024];
    byte[] largeBuf = new byte[fileLength + 100];
    StripedFileTestUtil
        .verifyPread(fs, srcPath, fileLength, expected, largeBuf, ecPolicy);

    StripedFileTestUtil
        .verifyStatefulRead(fs, srcPath, fileLength, expected, largeBuf);
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength, ecPolicy,
        blockGroupSize);
    StripedFileTestUtil
        .verifyStatefulRead(fs, srcPath, fileLength, expected, smallBuf);
    // webhdfs doesn't support bytebuffer read
  }

  @Test
  public void testConcat() throws Exception {
    final byte[] data =
        StripedFileTestUtil.generateBytes(blockSize * dataBlocks * 10 + 234);
    int totalLength = 0;

    Random r = new Random();
    Path target = new Path("/ec/testConcat_target");
    DFSTestUtil.writeFile(fs, target, Arrays.copyOfRange(data, 0, 123));
    totalLength += 123;

    int numFiles = 5;
    Path[] srcs = new Path[numFiles];
    for (int i = 0; i < numFiles; i++) {
      srcs[i] = new Path("/ec/testConcat_src_file_" + i);
      int srcLength = r.nextInt(blockSize * dataBlocks * 2) + 1;
      DFSTestUtil.writeFile(fs, srcs[i],
          Arrays.copyOfRange(data, totalLength, totalLength + srcLength));
      totalLength += srcLength;
    }

    fs.concat(target, srcs);
    StripedFileTestUtil.verifyStatefulRead(fs, target, totalLength,
        Arrays.copyOfRange(data, 0, totalLength), new byte[1024]);
  }

  @Test
  public void testConcatWithDifferentECPolicy() throws Exception {
    final byte[] data =
        StripedFileTestUtil.generateBytes(blockSize * dataBlocks);
    Path nonECFile = new Path("/non_ec_file");
    DFSTestUtil.writeFile(fs, nonECFile, data);
    Path target = new Path("/ec/non_ec_file");
    fs.rename(nonECFile, target);

    int numFiles = 2;
    Path[] srcs = new Path[numFiles];
    for (int i = 0; i < numFiles; i++) {
      srcs[i] = new Path("/ec/testConcat_src_file_"+i);
      DFSTestUtil.writeFile(fs, srcs[i], data);
    }
    try {
      fs.concat(target, srcs);
      Assert.fail("non-ec file shouldn't concat with ec file");
    } catch (RemoteException e){
      Assert.assertTrue(e.getMessage()
          .contains("have different erasure coding policy"));
    }
  }
}
