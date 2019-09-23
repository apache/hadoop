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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test striped file write operation with data node failures with fixed
 * parameter test cases.
 */
public class TestDFSStripedOutputStreamWithFailure extends
    TestDFSStripedOutputStreamWithFailureBase{
  public static final Logger LOG = LoggerFactory.getLogger(
      TestDFSStripedOutputStreamWithFailure.class);

  @Test(timeout=300000)
  public void testMultipleDatanodeFailure56() throws Exception {
    runTestWithMultipleFailure(getLength(56));
  }

  /**
   * Randomly pick a length and run tests with multiple data failures.
   * TODO: enable this later
   */
  //@Test(timeout=240000)
  public void testMultipleDatanodeFailureRandomLength() throws Exception {
    int lenIndex = RANDOM.nextInt(lengths.size());
    LOG.info("run testMultipleDatanodeFailureRandomLength with length index: "
        + lenIndex);
    runTestWithMultipleFailure(getLength(lenIndex));
  }

  @Test(timeout=240000)
  public void testBlockTokenExpired() throws Exception {
    // Make sure killPos is greater than the length of one stripe
    final int length = dataBlocks * cellSize * 3;
    final HdfsConfiguration conf = newHdfsConfiguration();

    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    for (int dn = 0; dn < dataBlocks + parityBlocks; dn += 2) {
      try {
        setup(conf);
        runTest(length, new int[]{length / 2}, new int[]{dn}, true);
      } catch (Exception e) {
        LOG.error("failed, dn=" + dn + ", length=" + length);
        throw e;
      } finally {
        tearDown();
      }
    }
  }

  @Test(timeout = 90000)
  public void testAddBlockWhenNoSufficientDataBlockNumOfNodes()
      throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    try {
      setup(conf);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // shutdown few datanodes to avoid getting sufficient data blocks number
      // of datanodes
      int numDatanodes = dataNodes.size();
      while (numDatanodes >= dataBlocks) {
        cluster.stopDataNode(0);
        numDatanodes--;
      }
      cluster.restartNameNodes();
      cluster.triggerHeartbeats();
      DatanodeInfo[] info = dfs.getClient().datanodeReport(
          DatanodeReportType.LIVE);
      assertEquals("Mismatches number of live Dns", numDatanodes, info.length);
      final Path dirFile = new Path(dir, "ecfile");
      LambdaTestUtils.intercept(
          IOException.class,
          "File " + dirFile + " could only be written to " +
              numDatanodes + " of the " + dataBlocks + " required nodes for " +
              ecPolicy.getName(),
          () -> {
            try (FSDataOutputStream out = dfs.create(dirFile, true)) {
              out.write("something".getBytes());
              out.flush();
            }
            return 0;
          });
    } finally {
      tearDown();
    }
  }

  private void testCloseWithExceptionsInStreamer(
      int numFailures, boolean shouldFail) throws Exception {
    assertTrue(numFailures <=
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    final Path dirFile = new Path(dir, "ecfile-" + numFailures);
    try (FSDataOutputStream out = dfs.create(dirFile, true)) {
      out.write("idempotent close".getBytes());

      // Expect to raise IOE on the first close call, but any following
      // close() should be no-op.
      LambdaTestUtils.intercept(IOException.class,
          out::close);

      assertTrue(out.getWrappedStream() instanceof DFSStripedOutputStream);
      DFSStripedOutputStream stripedOut =
          (DFSStripedOutputStream) out.getWrappedStream();
      for (int i = 0; i < numFailures; i++) {
        // Only inject 1 stream failure.
        stripedOut.getStripedDataStreamer(i).getLastException().set(
            new IOException("injected failure")
        );
      }
      if (shouldFail) {
        LambdaTestUtils.intercept(IOException.class, out::close);
      }

      // Close multiple times. All the following close() should have no
      // side-effect.
      out.close();
    }
  }

  // HDFS-12612
  @Test
  public void testIdempotentCloseWithFailedStreams() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    try {
      setup(conf);
      // shutdown few datanodes to avoid getting sufficient data blocks number
      // of datanodes.
      while (cluster.getDataNodes().size() >= dataBlocks) {
        cluster.stopDataNode(0);
      }
      cluster.restartNameNodes();
      cluster.triggerHeartbeats();

      testCloseWithExceptionsInStreamer(1, false);
      testCloseWithExceptionsInStreamer(ecPolicy.getNumParityUnits(), false);
      testCloseWithExceptionsInStreamer(ecPolicy.getNumParityUnits() + 1, true);
      testCloseWithExceptionsInStreamer(ecPolicy.getNumDataUnits(), true);
    } finally {
      tearDown();
    }
  }

  @Test
  public void testCloseAfterAbort() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    try {
      setup(conf);

      final Path dirFile = new Path(dir, "ecfile");
      FSDataOutputStream out = dfs.create(dirFile, true);
      assertTrue(out.getWrappedStream() instanceof DFSStripedOutputStream);
      DFSStripedOutputStream stripedOut =
          (DFSStripedOutputStream) out.getWrappedStream();
      stripedOut.abort();
      LambdaTestUtils.intercept(IOException.class,
          "Lease timeout", stripedOut::close);
    } finally {
      tearDown();
    }
  }

  @Test(timeout = 90000)
  public void testAddBlockWhenNoSufficientParityNumOfNodes()
      throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    try {
      setup(conf);
      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      // shutdown few data nodes to avoid writing parity blocks
      int killDns = (parityBlocks - 1);
      int numDatanodes = dataNodes.size() - killDns;
      for (int i = 0; i < killDns; i++) {
        cluster.stopDataNode(i);
      }
      cluster.restartNameNodes();
      cluster.triggerHeartbeats();
      DatanodeInfo[] info = dfs.getClient().datanodeReport(
          DatanodeReportType.LIVE);
      assertEquals("Mismatches number of live Dns", numDatanodes, info.length);
      Path srcPath = new Path(dir, "testAddBlockWhenNoSufficientParityNodes");
      int fileLength = cellSize - 1000;
      final byte[] expected = StripedFileTestUtil.generateBytes(fileLength);
      DFSTestUtil.writeFile(dfs, srcPath, new String(expected));
      LOG.info("writing finished. Seek and read the file to verify.");
      StripedFileTestUtil.verifySeek(dfs, srcPath, fileLength, ecPolicy,
          blockGroupSize);
    } finally {
      tearDown();
    }
  }

  /**
   * When the two DataNodes with partial data blocks fail.
   */
  @Test
  public void testCloseWithExceptionsInStreamer() throws Exception {
    final HdfsConfiguration conf = newHdfsConfiguration();

    final int[] fileLengths = {
        // Full stripe then partial on cell boundary
        cellSize * (dataBlocks * 2 - 2),
        // Full stripe and a partial on non-cell boundary
        (cellSize * dataBlocks) + 123,
    };
    // select the two DNs with partial block to kill
    int[] dnIndex = null;
    if (parityBlocks > 1) {
      dnIndex = new int[] {dataBlocks - 2, dataBlocks - 1};
    } else {
      dnIndex = new int[] {dataBlocks - 1};
    }
    for (int length : fileLengths) {
      final int[] killPos = getKillPositions(length, dnIndex.length);
      try {
        LOG.info("runTestWithMultipleFailure2: length==" + length + ", killPos="
            + Arrays.toString(killPos) + ", dnIndex="
            + Arrays.toString(dnIndex));
        setup(conf);
        runTest(length, killPos, dnIndex, false);
      } catch (Throwable e) {
        final String err = "failed, killPos=" + Arrays.toString(killPos)
            + ", dnIndex=" + Arrays.toString(dnIndex) + ", length=" + length;
        LOG.error(err);
        throw e;
      } finally {
        tearDown();
      }
    }
  }

  /**
   * Test writing very short EC files with many failures.
   */
  @Test
  public void runTestWithShortStripe() throws Exception {
    final HdfsConfiguration conf = newHdfsConfiguration();
    // Write a file with a 1 cell partial stripe
    final int length = cellSize - 123;
    // Kill all but one DN
    final int[] dnIndex = new int[dataBlocks + parityBlocks - 1];
    for (int i = 0; i < dnIndex.length; i++) {
      dnIndex[i] = i;
    }
    final int[] killPos = getKillPositions(length, dnIndex.length);

    try {
      LOG.info("runTestWithShortStripe: length==" + length + ", killPos="
          + Arrays.toString(killPos) + ", dnIndex="
          + Arrays.toString(dnIndex));
      setup(conf);
      runTest(length, killPos, dnIndex, false);
    } catch (Throwable e) {
      final String err = "failed, killPos=" + Arrays.toString(killPos)
          + ", dnIndex=" + Arrays.toString(dnIndex) + ", length=" + length;
      LOG.error(err);
      throw e;
    } finally {
      tearDown();
    }
  }
}
