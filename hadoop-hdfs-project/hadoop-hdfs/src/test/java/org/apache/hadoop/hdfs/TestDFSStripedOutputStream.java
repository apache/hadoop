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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Write.RECOVER_LEASE_ON_CLOSE_EXCEPTION_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities.StreamCapability;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.event.Level;

public class TestDFSStripedOutputStream {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestDFSStripedOutputStream.class);

  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.TRACE);
  }

  private ErasureCodingPolicy ecPolicy;
  private int dataBlocks;
  private int parityBlocks;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private int cellSize;
  private final int stripesPerBlock = 4;
  private int blockSize;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setup() throws IOException {
    /*
     * Initialize erasure coding policy.
     */
    ecPolicy = getEcPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = stripesPerBlock * cellSize;
    System.out.println("EC policy = " + ecPolicy);

    int numDNs = dataBlocks + parityBlocks + 2;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fs = cluster.getFileSystem();
    DFSTestUtil.enableAllECPolicies(fs);
    fs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());
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

  /**
   * {@link DFSStripedOutputStream} doesn't support hflush() or hsync() yet.
   * This test is to make sure that DFSStripedOutputStream doesn't throw any
   * {@link UnsupportedOperationException} on hflush() or hsync() so as to
   * comply with output stream spec.
   *
   * @throws Exception
   */
  @Test
  public void testStreamFlush() throws Exception {
    final byte[] bytes = StripedFileTestUtil.generateBytes(blockSize *
        dataBlocks * 3 + cellSize * dataBlocks + cellSize + 123);
    try (FSDataOutputStream os = fs.create(new Path("/ec-file-1"))) {
      assertFalse(
          "DFSStripedOutputStream should not have hflush() capability yet!",
          os.hasCapability(StreamCapability.HFLUSH.getValue()));
      assertFalse(
          "DFSStripedOutputStream should not have hsync() capability yet!",
          os.hasCapability(StreamCapability.HSYNC.getValue()));
      try (InputStream is = new ByteArrayInputStream(bytes)) {
        IOUtils.copyBytes(is, os, bytes.length);
        os.hflush();
        IOUtils.copyBytes(is, os, bytes.length);
        os.hsync();
        IOUtils.copyBytes(is, os, bytes.length);
      }
      assertTrue("stream is not a DFSStripedOutputStream",
          os.getWrappedStream() instanceof DFSStripedOutputStream);
      final DFSStripedOutputStream dfssos =
          (DFSStripedOutputStream) os.getWrappedStream();
      dfssos.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
    }
  }

  private void testOneFile(String src, int writeBytes) throws Exception {
    src += "_" + writeBytes;
    Path testPath = new Path(src);

    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, testPath, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

    StripedFileTestUtil.checkData(fs, testPath, writeBytes,
        new ArrayList<DatanodeInfo>(), null, blockSize * dataBlocks);
  }

  @Test
  public void testFileBlockSizeSmallerThanCellSize() throws Exception {
    final Path path = new Path("testFileBlockSizeSmallerThanCellSize");
    final byte[] bytes = StripedFileTestUtil.generateBytes(cellSize * 2);
    try {
      DFSTestUtil.writeFile(fs, path, bytes, cellSize / 2);
      fail("Creating a file with block size smaller than "
          + "ec policy's cell size should fail");
    } catch (IOException expected) {
      LOG.info("Caught expected exception", expected);
      GenericTestUtils
          .assertExceptionContains("less than the cell size", expected);
    }
  }

  @Test
  public void testExceptionInCloseECFileWithRecoverLease() throws Exception {
    Configuration config = new Configuration();
    config.setBoolean(RECOVER_LEASE_ON_CLOSE_EXCEPTION_KEY, true);
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), config);
    DFSClient spyClient = Mockito.spy(client);
    DFSOutputStream dfsOutputStream =
        spyClient.create("/testExceptionInCloseECFileWithRecoverLease",
            FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
            (short) 3, 1024*1024, null, 1024, null);
    assertTrue("stream should be a DFSStripedOutputStream",
        dfsOutputStream instanceof DFSStripedOutputStream);
    DFSOutputStream spyDFSOutputStream = Mockito.spy(dfsOutputStream);
    doThrow(new IOException("Emulated IOException in close"))
        .when(spyDFSOutputStream).completeFile(Mockito.any());
    try {
      spyDFSOutputStream.close();
      fail();
    } catch (IOException ioe) {
      assertTrue(spyDFSOutputStream.isLeaseRecovered());
      waitForFileClosed("/testExceptionInCloseECFileWithRecoverLease");
      assertTrue(isFileClosed("/testExceptionInCloseECFileWithRecoverLease"));
    }
  }

  @Test
  public void testExceptionInCloseECFileWithoutRecoverLease() throws Exception {
    Configuration config = new Configuration();
    DFSClient client =
        new DFSClient(cluster.getNameNode(0).getNameNodeAddress(), config);
    DFSClient spyClient = Mockito.spy(client);
    DFSOutputStream dfsOutputStream =
        spyClient.create("/testExceptionInCloseECFileWithoutRecoverLease",
            FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
            (short) 3, 1024*1024, null, 1024, null);
    assertTrue("stream should be a DFSStripedOutputStream",
        dfsOutputStream instanceof DFSStripedOutputStream);
    DFSOutputStream spyDFSOutputStream = Mockito.spy(dfsOutputStream);
    doThrow(new IOException("Emulated IOException in close"))
        .when(spyDFSOutputStream).completeFile(Mockito.any());
    try {
      spyDFSOutputStream.close();
      fail();
    } catch (IOException ioe) {
      assertFalse(spyDFSOutputStream.isLeaseRecovered());
      try {
        waitForFileClosed("/testExceptionInCloseECFileWithoutRecoverLease");
      } catch (TimeoutException e) {
        assertFalse(
            isFileClosed("/testExceptionInCloseECFileWithoutRecoverLease"));
      }
    }
  }

  private boolean isFileClosed(String path) throws IOException {
    return cluster.getFileSystem().isFileClosed(new Path(path));
  }

  private void waitForFileClosed(String path) throws Exception {
    GenericTestUtils.waitFor(() -> {
      boolean closed;
      try {
        closed = isFileClosed(path);
      } catch (IOException e) {
        return false;
      }
      return closed;
    }, 1000, 5000);
  }
}
