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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test striped file write operation with data node failures.
 */
public class TestDFSStripedOutputStreamWithFailure {
  public static final Log LOG = LogFactory.getLog(
      TestDFSStripedOutputStreamWithFailure.class);
  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
    ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
        .getLogger().setLevel(Level.ALL);
  }

  private final int cellSize = 64 * 1024; //64k
  private final int stripesPerBlock = 4;
  private ErasureCodingPolicy ecPolicy;
  private int dataBlocks;
  private int parityBlocks;
  private int blockSize;
  private int blockGroupSize;

  private static final int FLUSH_POS =
      9 * DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT + 1;

  public ECSchema getEcSchema() {
    return StripedFileTestUtil.getDefaultECPolicy().getSchema();
  }

  /*
   * Initialize erasure coding policy.
   */
  @Before
  public void init() {
    ecPolicy = new ErasureCodingPolicy(getEcSchema(), cellSize);
    dataBlocks = ecPolicy.getNumDataUnits();
    parityBlocks = ecPolicy.getNumParityUnits();
    blockSize = cellSize * stripesPerBlock;
    blockGroupSize = blockSize * dataBlocks;
    dnIndexSuite = getDnIndexSuite();
    lengths = newLengths();
  }

  List<Integer> newLengths() {
    final List<Integer> lens = new ArrayList<>();
    lens.add(FLUSH_POS + 2);
    for(int b = 0; b <= 2; b++) {
      for(int c = 0; c < stripesPerBlock * dataBlocks; c++) {
        for(int delta = -1; delta <= 1; delta++) {
          final int length = b * blockGroupSize + c * cellSize + delta;
          System.out.println(lens.size() + ": length=" + length
              + ", (b, c, d) = (" + b + ", " + c + ", " + delta + ")");
          lens.add(length);
        }
      }
    }
    return lens;
  }

  private int[][] dnIndexSuite;

  private int[][] getDnIndexSuite() {
    final int maxNumLevel = 2;
    final int maxPerLevel = 8;
    List<List<Integer>> allLists = new ArrayList<>();
    int numIndex = parityBlocks;
    for (int i = 0; i < maxNumLevel && numIndex > 1; i++) {
      List<List<Integer>> lists =
          combinations(dataBlocks + parityBlocks, numIndex);
      if (lists.size() > maxPerLevel) {
        Collections.shuffle(lists);
        lists = lists.subList(0, maxPerLevel);
      }
      allLists.addAll(lists);
      numIndex--;
    }
    int[][] dnIndexArray = new int[allLists.size()][];
    for (int i = 0; i < dnIndexArray.length; i++) {
      int[] list = new int[allLists.get(i).size()];
      for (int j = 0; j < list.length; j++) {
        list[j] = allLists.get(i).get(j);
      }
      dnIndexArray[i] = list;
    }
    return dnIndexArray;
  }

  // get all combinations of k integers from {0,...,n-1}
  private static List<List<Integer>> combinations(int n, int k) {
    List<List<Integer>> res = new LinkedList<List<Integer>>();
    if (k >= 1 && n >= k) {
      getComb(n, k, new Stack<Integer>(), res);
    }
    return res;
  }

  private static void getComb(int n, int k, Stack<Integer> stack,
      List<List<Integer>> res) {
    if (stack.size() == k) {
      List<Integer> list = new ArrayList<Integer>(stack);
      res.add(list);
    } else {
      int next = stack.empty() ? 0 : stack.peek() + 1;
      while (next < n) {
        stack.push(next);
        getComb(n, k, stack, res);
        next++;
      }
    }
    if (!stack.empty()) {
      stack.pop();
    }
  }

  private int[] getKillPositions(int fileLen, int num) {
    int[] positions = new int[num];
    for (int i = 0; i < num; i++) {
      positions[i] = fileLen * (i + 1) / (num + 1);
    }
    return positions;
  }

  private List<Integer> lengths;

  Integer getLength(int i) {
    return i >= 0 && i < lengths.size() ? lengths.get(i): null;
  }

  private static final Random RANDOM = new Random();

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private final Path dir = new Path("/"
      + TestDFSStripedOutputStreamWithFailure.class.getSimpleName());

  private void setup(Configuration conf) throws IOException {
    System.out.println("NUM_DATA_BLOCKS  = " + dataBlocks);
    System.out.println("NUM_PARITY_BLOCKS= " + parityBlocks);
    System.out.println("CELL_SIZE        = " + cellSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(cellSize, "B", 2)
        + ")");
    System.out.println("BLOCK_SIZE       = " + blockSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(blockSize, "B", 2)
        + ")");
    System.out.println("BLOCK_GROUP_SIZE = " + blockGroupSize + " (=" +
        StringUtils.TraditionalBinaryPrefix.long2String(blockGroupSize, "B", 2)
        + ")");
    final int numDNs = dataBlocks + parityBlocks;
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    AddErasureCodingPolicyResponse[] res =
        dfs.addErasureCodingPolicies(new ErasureCodingPolicy[]{ecPolicy});
    ecPolicy = res[0].getPolicy();
    dfs.enableErasureCodingPolicy(ecPolicy.getName());
    DFSTestUtil.enableAllECPolicies(dfs);
    dfs.mkdirs(dir);
    dfs.setErasureCodingPolicy(dir, ecPolicy.getName());
  }

  private void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private HdfsConfiguration newHdfsConfiguration() {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    return conf;
  }

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

  void runTest(final int length) {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int dn = 0; dn < dataBlocks + parityBlocks; dn++) {
      try {
        LOG.info("runTest: dn=" + dn + ", length=" + length);
        setup(conf);
        runTest(length, new int[]{length / 2}, new int[]{dn}, false);
      } catch (Throwable e) {
        final String err = "failed, dn=" + dn + ", length=" + length
            + StringUtils.stringifyException(e);
        LOG.error(err);
        Assert.fail(err);
      } finally {
        tearDown();
      }
    }
  }

  void runTestWithMultipleFailure(final int length) throws Exception {
    final HdfsConfiguration conf = newHdfsConfiguration();
    for (int[] dnIndex : dnIndexSuite) {
      int[] killPos = getKillPositions(length, dnIndex.length);
      try {
        LOG.info("runTestWithMultipleFailure: length==" + length + ", killPos="
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
   * When the two DataNodes with partial data blocks fail.
   */
  @Test
  public void runTestWithDifferentLengths() throws Exception {
    assumeTrue("Skip this test case in the subclasses. Once is enough.",
        this.getClass().equals(TestDFSStripedOutputStreamWithFailure.class));

    final HdfsConfiguration conf = newHdfsConfiguration();

    final int[] fileLengths = {
        // Full stripe then partial on cell boundary
        cellSize * (dataBlocks * 2 - 2),
        // Full stripe and a partial on non-cell boundary
        (cellSize * dataBlocks) + 123,
    };
    try {
      for (int length: fileLengths) {
        // select the two DNs with partial block to kill
        final int[] dnIndex = {dataBlocks - 2, dataBlocks - 1};
        final int[] killPos = getKillPositions(length, dnIndex.length);
        try {
          LOG.info("runTestWithMultipleFailure2: length==" + length
              + ", killPos=" + Arrays.toString(killPos)
              + ", dnIndex=" + Arrays.toString(dnIndex));
          setup(conf);
          runTest(length, killPos, dnIndex, false);
        } catch (Throwable e) {
          final String err = "failed, killPos=" + Arrays.toString(killPos)
              + ", dnIndex=" + Arrays.toString(dnIndex) + ", length=" + length;
          LOG.error(err);
          throw e;
        }
      }
    } finally {
      tearDown();
    }
  }

  /**
   * Test writing very short EC files with many failures.
   */
  @Test
  public void runTestWithShortStripe() throws Exception {
    assumeTrue("Skip this test case in the subclasses. Once is enough.",
        this.getClass().equals(TestDFSStripedOutputStreamWithFailure.class));

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

  /**
   * runTest implementation.
   * @param length file length
   * @param killPos killing positions in ascending order
   * @param dnIndex DN index to kill when meets killing positions
   * @param tokenExpire wait token to expire when kill a DN
   * @throws Exception
   */
  private void runTest(final int length, final int[] killPos,
      final int[] dnIndex, final boolean tokenExpire) throws Exception {
    if (killPos[0] <= FLUSH_POS) {
      LOG.warn("killPos=" + Arrays.toString(killPos) + " <= FLUSH_POS="
          + FLUSH_POS + ", length=" + length + ", dnIndex="
          + Arrays.toString(dnIndex));
      return; //skip test
    }
    Preconditions.checkArgument(length > killPos[0], "length=%s <= killPos=%s",
        length, killPos);
    Preconditions.checkArgument(killPos.length == dnIndex.length);

    final Path p = new Path(dir, "dn" + Arrays.toString(dnIndex)
        + "len" + length + "kill" +  Arrays.toString(killPos));
    final String fullPath = p.toString();
    LOG.info("fullPath=" + fullPath);

    if (tokenExpire) {
      final NameNode nn = cluster.getNameNode();
      final BlockManager bm = nn.getNamesystem().getBlockManager();
      final BlockTokenSecretManager sm = bm.getBlockTokenSecretManager();

      // set a short token lifetime (6 second)
      SecurityTestUtil.setBlockTokenLifetime(sm, 6000L);
    }

    final AtomicInteger pos = new AtomicInteger();
    final FSDataOutputStream out = dfs.create(p);
    final DFSStripedOutputStream stripedOut
        = (DFSStripedOutputStream)out.getWrappedStream();

    // first GS of this block group which never proceeds blockRecovery
    long firstGS = -1;
    long oldGS = -1; // the old GS before bumping
    List<Long> gsList = new ArrayList<>();
    final List<DatanodeInfo> killedDN = new ArrayList<>();
    int numKilled = 0;
    for(; pos.get() < length;) {
      final int i = pos.getAndIncrement();
      if (numKilled < killPos.length && i == killPos[numKilled]) {
        assertTrue(firstGS != -1);
        final long gs = getGenerationStamp(stripedOut);
        if (numKilled == 0) {
          assertEquals(firstGS, gs);
        } else {
          //TODO: implement hflush/hsync and verify gs strict greater than oldGS
          assertTrue(gs >= oldGS);
        }
        oldGS = gs;

        if (tokenExpire) {
          DFSTestUtil.flushInternal(stripedOut);
          waitTokenExpires(out);
        }

        killedDN.add(
            killDatanode(cluster, stripedOut, dnIndex[numKilled], pos));
        numKilled++;
      }

      write(out, i);

      if (i % blockGroupSize == FLUSH_POS) {
        firstGS = getGenerationStamp(stripedOut);
        oldGS = firstGS;
      }
      if (i > 0 && (i + 1) % blockGroupSize == 0) {
        gsList.add(oldGS);
      }
    }
    gsList.add(oldGS);
    out.close();
    assertEquals(dnIndex.length, numKilled);

    StripedFileTestUtil.waitBlockGroupsReported(dfs, fullPath, numKilled);

    cluster.triggerBlockReports();
    StripedFileTestUtil.checkData(dfs, p, length, killedDN, gsList,
        blockGroupSize);
  }

  static void write(FSDataOutputStream out, int i) throws IOException {
    try {
      out.write(StripedFileTestUtil.getByte(i));
    } catch(IOException ioe) {
      throw new IOException("Failed at i=" + i, ioe);
    }
  }

  static long getGenerationStamp(DFSStripedOutputStream out)
      throws IOException {
    final long gs = out.getBlock().getGenerationStamp();
    LOG.info("getGenerationStamp returns " + gs);
    return gs;
  }

  static DatanodeInfo getDatanodes(StripedDataStreamer streamer) {
    for(;;) {
      DatanodeInfo[] datanodes = streamer.getNodes();
      if (datanodes == null) {
        // try peeking following block.
        final LocatedBlock lb = streamer.peekFollowingBlock();
        if (lb != null) {
          datanodes = lb.getLocations();
        }
      }

      if (datanodes != null) {
        Assert.assertEquals(1, datanodes.length);
        Assert.assertNotNull(datanodes[0]);
        return datanodes[0];
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        Assert.fail(StringUtils.stringifyException(ie));
        return null;
      }
    }
  }

  static DatanodeInfo killDatanode(MiniDFSCluster cluster,
      DFSStripedOutputStream out, final int dnIndex, final AtomicInteger pos) {
    final StripedDataStreamer s = out.getStripedDataStreamer(dnIndex);
    final DatanodeInfo datanode = getDatanodes(s);
    LOG.info("killDatanode " + dnIndex + ": " + datanode + ", pos=" + pos);
    if (datanode != null) {
      cluster.stopDataNode(datanode.getXferAddr());
    }
    return datanode;
  }


  private void waitTokenExpires(FSDataOutputStream out) throws IOException {
    Token<BlockTokenIdentifier> token = DFSTestUtil.getBlockToken(out);
    while (!SecurityTestUtil.isBlockTokenExpired(token)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
    }
  }

  int getBase() {
    final String name = getClass().getSimpleName();
    int i = name.length() - 1;
    for(; i >= 0 && Character.isDigit(name.charAt(i));){
      i--;
    }
    String number = name.substring(i + 1);
    try {
      return Integer.parseInt(number);
    } catch (Exception e) {
      return -1;
    }
  }

  private void run(int offset) {
    int base = getBase();
    assumeTrue(base >= 0);
    final int i = offset + base;
    final Integer length = getLength(i);
    if (length == null) {
      System.out.println("Skip test " + i + " since length=null.");
      return;
    }
    if (RANDOM.nextInt(16) != 0) {
      System.out.println("Test " + i + ", length=" + length
          + ", is not chosen to run.");
      return;
    }
    System.out.println("Run test " + i + ", length=" + length);
    runTest(length);
  }

  @Test(timeout = 240000)
  public void test0() {
    run(0);
  }

  @Test(timeout = 240000)
  public void test1() {
    run(1);
  }

  @Test(timeout = 240000)
  public void test2() {
    run(2);
  }

  @Test(timeout = 240000)
  public void test3() {
    run(3);
  }

  @Test(timeout = 240000)
  public void test4() {
    run(4);
  }

  @Test(timeout = 240000)
  public void test5() {
    run(5);
  }

  @Test(timeout = 240000)
  public void test6() {
    run(6);
  }

  @Test(timeout = 240000)
  public void test7() {
    run(7);
  }

  @Test(timeout = 240000)
  public void test8() {
    run(8);
  }

  @Test(timeout = 240000)
  public void test9() {
    run(9);
  }
}
