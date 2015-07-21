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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.blockSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.cellSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.dataBlocks;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.numDNs;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.stripesPerBlock;

public class TestWriteReadStripedFile {
  public static final Log LOG = LogFactory.getLog(TestWriteReadStripedFile.class);
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static Configuration conf = new HdfsConfiguration();

  static {
    ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
        .getLogger().setLevel(Level.ALL);
  }

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/",
        null, cellSize);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileEmpty() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EmptyFile", 0);
    testOneFileUsingDFSStripedInputStream("/EmptyFile2", 0, true);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell", 1);
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell2", 1, true);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell", cellSize - 1);
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell2", cellSize - 1,
        true);
  }

  @Test
  public void testFileEqualsWithOneCell() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneCell", cellSize);
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneCell2", cellSize, true);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe",
        cellSize * dataBlocks - 1);
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe2",
        cellSize * dataBlocks - 1, true);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe",
        cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe2",
        cellSize + 123, true);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneStripe",
        cellSize * dataBlocks);
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneStripe2",
        cellSize * dataBlocks, true);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe1",
        cellSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe12",
        cellSize * dataBlocks + 123, true);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe2",
        cellSize * dataBlocks + cellSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe22",
        cellSize * dataBlocks + cellSize * dataBlocks + 123, true);
  }

  @Test
  public void testLessThanFullBlockGroup() throws IOException {
    testOneFileUsingDFSStripedInputStream("/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
    testOneFileUsingDFSStripedInputStream("/LessThanFullBlockGroup2",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize, true);
  }

  @Test
  public void testFileFullBlockGroup() throws IOException {
    testOneFileUsingDFSStripedInputStream("/FullBlockGroup",
        blockSize * dataBlocks);
    testOneFileUsingDFSStripedInputStream("/FullBlockGroup2",
        blockSize * dataBlocks, true);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup1",
        blockSize * dataBlocks + 123);
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup12",
        blockSize * dataBlocks + 123, true);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup22",
        blockSize * dataBlocks + cellSize + 123, true);
  }


  @Test
  public void testFileMoreThanABlockGroup3() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123);
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup32",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123, true);
  }

  private void testOneFileUsingDFSStripedInputStream(String src, int fileLength)
      throws IOException {
    testOneFileUsingDFSStripedInputStream(src, fileLength, false);
  }

  private void testOneFileUsingDFSStripedInputStream(String src, int fileLength,
      boolean withDataNodeFailure) throws IOException {
    final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
    Path srcPath = new Path(src);
    DFSTestUtil.writeFile(fs, srcPath, new String(expected));

    StripedFileTestUtil.verifyLength(fs, srcPath, fileLength);

    if (withDataNodeFailure) {
      int dnIndex = 1; // TODO: StripedFileTestUtil.random.nextInt(dataBlocks);
      LOG.info("stop DataNode " + dnIndex);
      stopDataNode(srcPath, dnIndex);
    }

    byte[] smallBuf = new byte[1024];
    byte[] largeBuf = new byte[fileLength + 100];
    StripedFileTestUtil.verifyPread(fs, srcPath, fileLength, expected, largeBuf);

    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected,
        largeBuf);
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength);
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
    // TODO: HDFS-8797
    //StripedFileTestUtil.verifyPread(fs, srcPath, fileLength, expected, largeBuf);

    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected, largeBuf);
    StripedFileTestUtil.verifySeek(fs, srcPath, fileLength);
    StripedFileTestUtil.verifyStatefulRead(fs, srcPath, fileLength, expected, smallBuf);
    // webhdfs doesn't support bytebuffer read
  }
}
