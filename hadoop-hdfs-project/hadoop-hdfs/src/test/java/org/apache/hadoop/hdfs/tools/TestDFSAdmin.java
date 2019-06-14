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
package org.apache.hadoop.hdfs.tools;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.TestRefreshUserMappings;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * set/clrSpaceQuote are tested in {@link org.apache.hadoop.hdfs.TestQuota}.
 */
public class TestDFSAdmin {
  private static final Log LOG = LogFactory.getLog(TestDFSAdmin.class);
  private Configuration conf = null;
  private MiniDFSCluster cluster;
  private DFSAdmin admin;
  private DataNode datanode;
  private NameNode namenode;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private String tempResource = null;
  private static final int NUM_DATANODES = 2;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getRandomizedTempPath());
    restartCluster();

    admin = new DFSAdmin(conf);
  }

  private void redirectStream() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  private void resetStream() {
    out.reset();
    err.reset();
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
    } finally {
      System.setOut(OLD_OUT);
      System.setErr(OLD_ERR);
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    resetStream();
    if (tempResource != null) {
      File f = new File(tempResource);
      FileUtils.deleteQuietly(f);
      tempResource = null;
    }
  }

  private void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    datanode = cluster.getDataNodes().get(0);
    namenode = cluster.getNameNode();
  }

  private void getReconfigurableProperties(String nodeType, String address,
      final List<String> outs, final List<String> errs) throws IOException {
    reconfigurationOutErrFormatter("getReconfigurableProperties", nodeType,
        address, outs, errs);
  }

  private void getReconfigurationStatus(String nodeType, String address,
      final List<String> outs, final List<String> errs) throws IOException {
    reconfigurationOutErrFormatter("getReconfigurationStatus", nodeType,
        address, outs, errs);
  }

  private void reconfigurationOutErrFormatter(String methodName,
      String nodeType, String address, final List<String> outs,
      final List<String> errs) throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream outStream = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream errStream = new PrintStream(bufErr);

    if (methodName.equals("getReconfigurableProperties")) {
      admin.getReconfigurableProperties(
          nodeType,
          address,
          outStream,
          errStream);
    } else if (methodName.equals("getReconfigurationStatus")) {
      admin.getReconfigurationStatus(nodeType, address, outStream, errStream);
    } else if (methodName.equals("startReconfiguration")) {
      admin.startReconfiguration(nodeType, address, outStream, errStream);
    }

    scanIntoList(bufOut, outs);
    scanIntoList(bufErr, errs);
  }

  private static void scanIntoList(
      final ByteArrayOutputStream baos,
      final List<String> list) {
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      list.add(scanner.nextLine());
    }
    scanner.close();
  }

  @Test(timeout = 30000)
  public void testGetDatanodeInfo() throws Exception {
    redirectStream();
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);

    for (int i = 0; i < cluster.getDataNodes().size(); i++) {
      resetStream();
      final DataNode dn = cluster.getDataNodes().get(i);
      final String addr = String.format(
          "%s:%d",
          dn.getXferAddress().getHostString(),
          dn.getIpcPort());
      final int ret = ToolRunner.run(dfsAdmin,
          new String[]{"-getDatanodeInfo", addr});
      assertEquals(0, ret);

      /* collect outputs */
      final List<String> outs = Lists.newArrayList();
      scanIntoList(out, outs);
      /* verify results */
      assertEquals(
          "One line per DataNode like: Uptime: XXX, Software version: x.y.z,"
              + " Config version: core-x.y.z,hdfs-x",
          1, outs.size());
      assertThat(outs.get(0),
          is(allOf(containsString("Uptime:"),
              containsString("Software version"),
              containsString("Config version"))));
    }
  }

  @Test(timeout = 30000)
  public void testGetVolumeReport() throws Exception {
    redirectStream();
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);

    for (int i = 0; i < cluster.getDataNodes().size(); i++) {
      resetStream();
      final DataNode dn = cluster.getDataNodes().get(i);
      final String addr = String.format("%s:%d", dn.getXferAddress()
          .getHostString(), dn.getIpcPort());
      final int ret = ToolRunner.run(dfsAdmin, new String[] {
          "-getVolumeReport", addr });
      assertEquals(0, ret);

      /* collect outputs */
      final List<String> outs = Lists.newArrayList();
      scanIntoList(out, outs);
      assertEquals(outs.get(0), "Active Volumes : 2");
    }
  }
  /**
   * Test that if datanode is not reachable, some DFSAdmin commands will fail
   * elegantly with non-zero ret error code along with exception error message.
   */
  @Test(timeout = 60000)
  public void testDFSAdminUnreachableDatanode() throws Exception {
    redirectStream();
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    for (String command : new String[]{"-getDatanodeInfo",
        "-evictWriters", "-getBalancerBandwidth"}) {
      // Connecting to Xfer port instead of IPC port will get
      // Datanode unreachable. java.io.EOFException
      final String dnDataAddr = datanode.getXferAddress().getHostString() + ":"
          + datanode.getXferPort();
      resetStream();
      final List<String> outs = Lists.newArrayList();
      final int ret = ToolRunner.run(dfsAdmin,
          new String[]{command, dnDataAddr});
      assertEquals(-1, ret);

      scanIntoList(out, outs);
      assertTrue("Unexpected " + command + " stdout: " + out, outs.isEmpty());
      assertTrue("Unexpected " + command + " stderr: " + err,
          err.toString().contains("Exception"));
    }
  }

  @Test(timeout = 30000)
  public void testDataNodeGetReconfigurableProperties() throws IOException {
    final int port = datanode.getIpcPort();
    final String address = "localhost:" + port;
    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    getReconfigurableProperties("datanode", address, outs, errs);
    assertEquals(3, outs.size());
    assertEquals(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, outs.get(1));
  }

  /**
   * Test reconfiguration and check the status outputs.
   * @param expectedSuccuss set true if the reconfiguration task should success.
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  private void testDataNodeGetReconfigurationStatus(boolean expectedSuccuss)
      throws IOException, InterruptedException, TimeoutException {
    ReconfigurationUtil ru = mock(ReconfigurationUtil.class);
    datanode.setReconfigurationUtil(ru);

    List<ReconfigurationUtil.PropertyChange> changes =
        new ArrayList<>();
    File newDir = new File(cluster.getDataDirectory(), "data_new");
    if (expectedSuccuss) {
      newDir.mkdirs();
    } else {
      // Inject failure.
      newDir.createNewFile();
    }
    changes.add(new ReconfigurationUtil.PropertyChange(
        DFS_DATANODE_DATA_DIR_KEY, newDir.toString(),
        datanode.getConf().get(DFS_DATANODE_DATA_DIR_KEY)));
    changes.add(new ReconfigurationUtil.PropertyChange(
        "randomKey", "new123", "old456"));
    when(ru.parseChangedProperties(any(Configuration.class),
        any(Configuration.class))).thenReturn(changes);

    final int port = datanode.getIpcPort();
    final String address = "localhost:" + port;

    assertThat(admin.startReconfiguration("datanode", address), is(0));

    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    awaitReconfigurationFinished("datanode", address, outs, errs);

    if (expectedSuccuss) {
      assertThat(outs.size(), is(4));
    } else {
      assertThat(outs.size(), is(6));
    }

    List<StorageLocation> locations = DataNode.getStorageLocations(
        datanode.getConf());
    if (expectedSuccuss) {
      assertThat(locations.size(), is(1));
      assertThat(new File(locations.get(0).getUri()), is(newDir));
      // Verify the directory is appropriately formatted.
      assertTrue(new File(newDir, Storage.STORAGE_DIR_CURRENT).isDirectory());
    } else {
      assertTrue(locations.isEmpty());
    }

    int offset = 1;
    if (expectedSuccuss) {
      assertThat(outs.get(offset),
          containsString("SUCCESS: Changed property " +
              DFS_DATANODE_DATA_DIR_KEY));
    } else {
      assertThat(outs.get(offset),
          containsString("FAILED: Change property " +
              DFS_DATANODE_DATA_DIR_KEY));
    }
    File dnDir0 = cluster.getInstanceStorageDir(0, 0);
    File dnDir1 = cluster.getInstanceStorageDir(0, 1);
    assertThat(outs.get(offset + 1), is(allOf(containsString("From:"),
                containsString(dnDir0.getName()),
                containsString(dnDir1.getName()))));
    assertThat(outs.get(offset + 2),
        is(not(anyOf(containsString(dnDir0.getName()),
            containsString(dnDir1.getName())))));
    assertThat(outs.get(offset + 2),
        is(allOf(containsString("To"), containsString("data_new"))));
  }

  @Test(timeout = 30000)
  public void testDataNodeGetReconfigurationStatus() throws IOException,
      InterruptedException, TimeoutException {
    testDataNodeGetReconfigurationStatus(true);
    restartCluster();
    testDataNodeGetReconfigurationStatus(false);
  }

  @Test(timeout = 30000)
  public void testNameNodeGetReconfigurableProperties() throws IOException {
    final String address = namenode.getHostAndPort();
    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    getReconfigurableProperties("namenode", address, outs, errs);
    assertEquals(9, outs.size());
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY, outs.get(1));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, outs.get(2));
    assertEquals(errs.size(), 0);
  }

  void awaitReconfigurationFinished(final String nodeType,
      final String address, final List<String> outs, final List<String> errs)
      throws TimeoutException, IOException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        outs.clear();
        errs.clear();
        try {
          getReconfigurationStatus(nodeType, address, outs, errs);
        } catch (IOException e) {
          LOG.error(String.format(
              "call getReconfigurationStatus on %s[%s] failed.", nodeType,
              address), e);
        }
        return !outs.isEmpty() && outs.get(0).contains("finished");

      }
    }, 100, 100 * 100);
  }

  @Test(timeout = 30000)
  public void testPrintTopology() throws Exception {
    redirectStream();

    /* init conf */
    final Configuration dfsConf = new HdfsConfiguration();
    final File baseDir = new File(
        PathUtils.getTestDir(getClass()),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

    final int numDn = 4;
    final String[] racks = {
        "/d1/r1", "/d1/r2",
        "/d2/r1", "/d2/r2"};

    /* init cluster using topology */
    try (MiniDFSCluster miniCluster = new MiniDFSCluster.Builder(dfsConf)
        .numDataNodes(numDn).racks(racks).build()) {

      miniCluster.waitActive();
      assertEquals(numDn, miniCluster.getDataNodes().size());
      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);

      resetStream();
      final int ret = ToolRunner.run(dfsAdmin, new String[] {"-printTopology"});

      /* collect outputs */
      final List<String> outs = Lists.newArrayList();
      scanIntoList(out, outs);

      /* verify results */
      assertEquals(0, ret);
      assertEquals(
          "There should be three lines per Datanode: the 1st line is"
              + " rack info, 2nd node info, 3rd empty line. The total"
              + " should be as a result of 3 * numDn.",
          12, outs.size());
      assertThat(outs.get(0),
          is(allOf(containsString("Rack:"), containsString("/d1/r1"))));
      assertThat(outs.get(3),
          is(allOf(containsString("Rack:"), containsString("/d1/r2"))));
      assertThat(outs.get(6),
          is(allOf(containsString("Rack:"), containsString("/d2/r1"))));
      assertThat(outs.get(9),
          is(allOf(containsString("Rack:"), containsString("/d2/r2"))));
    }
  }

  @Test(timeout = 30000)
  public void testNameNodeGetReconfigurationStatus() throws IOException,
      InterruptedException, TimeoutException {
    ReconfigurationUtil ru = mock(ReconfigurationUtil.class);
    namenode.setReconfigurationUtil(ru);
    final String address = namenode.getHostAndPort();

    List<ReconfigurationUtil.PropertyChange> changes =
        new ArrayList<>();
    changes.add(new ReconfigurationUtil.PropertyChange(
        DFS_HEARTBEAT_INTERVAL_KEY, String.valueOf(6),
        namenode.getConf().get(DFS_HEARTBEAT_INTERVAL_KEY)));
    changes.add(new ReconfigurationUtil.PropertyChange(
        "randomKey", "new123", "old456"));
    when(ru.parseChangedProperties(any(Configuration.class),
        any(Configuration.class))).thenReturn(changes);
    assertThat(admin.startReconfiguration("namenode", address), is(0));

    final List<String> outs = Lists.newArrayList();
    final List<String> errs = Lists.newArrayList();
    awaitReconfigurationFinished("namenode", address, outs, errs);

    // verify change
    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        6,
        namenode
          .getConf()
          .getLong(DFS_HEARTBEAT_INTERVAL_KEY,
                DFS_HEARTBEAT_INTERVAL_DEFAULT));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        6,
        namenode
          .getNamesystem()
          .getBlockManager()
          .getDatanodeManager()
          .getHeartbeatInterval());

    int offset = 1;
    assertThat(outs.get(offset), containsString("SUCCESS: Changed property "
        + DFS_HEARTBEAT_INTERVAL_KEY));
    assertThat(outs.get(offset + 1),
        is(allOf(containsString("From:"), containsString("3"))));
    assertThat(outs.get(offset + 2),
        is(allOf(containsString("To:"), containsString("6"))));
  }

  private static String scanIntoString(final ByteArrayOutputStream baos) {
    final StrBuilder sb = new StrBuilder();
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      sb.appendln(scanner.nextLine());
    }
    scanner.close();
    return sb.toString();
  }

  // get block details and check if the block is corrupt
  private void waitForCorruptBlock(MiniDFSCluster miniCluster,
      DFSClient client, Path file)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LocatedBlocks blocks = null;
        try {
          miniCluster.triggerBlockReports();
          blocks = client.getNamenode().getBlockLocations(file.toString(), 0,
              Long.MAX_VALUE);
        } catch (IOException e) {
          return false;
        }
        return blocks != null && blocks.get(0).isCorrupt();
      }
    }, 1000, 60000);
  }

  @Test(timeout = 180000)
  public void testReportCommand() throws Exception {
    tearDown();
    redirectStream();

    // init conf
    final Configuration dfsConf = new HdfsConfiguration();
    ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getByID(
        SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    dfsConf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getAbsolutePath(),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());
    final int numDn =
        ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();

    try(MiniDFSCluster miniCluster = new MiniDFSCluster
        .Builder(dfsConf)
        .numDataNodes(numDn).build()) {

      miniCluster.waitActive();
      assertEquals(numDn, miniCluster.getDataNodes().size());

      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      final DFSClient client = miniCluster.getFileSystem().getClient();

      // Verify report command for all counts to be zero
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn, 0, 0, client, 0L, 0L);

      final short replFactor = 1;
      final long fileLength = 512L;
      final DistributedFileSystem fs = miniCluster.getFileSystem();
      final Path file = new Path(baseDir, "/corrupted");
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
      DFSTestUtil.waitReplication(fs, file, replFactor);
      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file);
      LocatedBlocks lbs = miniCluster.getFileSystem().getClient().
          getNamenode().getBlockLocations(
          file.toString(), 0, fileLength);
      assertTrue("Unexpected block type: " + lbs.get(0),
          lbs.get(0) instanceof LocatedBlock);
      LocatedBlock locatedBlock = lbs.get(0);
      DatanodeInfo locatedDataNode = locatedBlock.getLocations()[0];
      LOG.info("Replica block located on: " + locatedDataNode);

      Path ecDir = new Path(baseDir, "ec");
      fs.mkdirs(ecDir);
      fs.getClient().setErasureCodingPolicy(ecDir.toString(),
          ecPolicy.getName());
      Path ecFile = new Path(ecDir, "ec-file");
      int stripesPerBlock = 2;
      int cellSize = ecPolicy.getCellSize();
      int blockSize = stripesPerBlock * cellSize;
      int blockGroupSize =  ecPolicy.getNumDataUnits() * blockSize;
      int totalBlockGroups = 1;
      DFSTestUtil.createStripedFile(miniCluster, ecFile, ecDir,
          totalBlockGroups, stripesPerBlock, false, ecPolicy);

      // Verify report command for all counts to be zero
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn, 0, 0, client, 0L, 0L);

      // Choose a DataNode to shutdown
      final List<DataNode> datanodes = miniCluster.getDataNodes();
      DataNode dataNodeToShutdown = null;
      for (DataNode dn : datanodes) {
        if (!dn.getDatanodeId().getDatanodeUuid().equals(
            locatedDataNode.getDatanodeUuid())) {
          dataNodeToShutdown = dn;
          break;
        }
      }
      assertTrue("Unable to choose a DataNode to shutdown!",
          dataNodeToShutdown != null);

      // Shut down the DataNode not hosting the replicated block
      LOG.info("Shutting down: " + dataNodeToShutdown);
      dataNodeToShutdown.shutdown();
      miniCluster.setDataNodeDead(dataNodeToShutdown.getDatanodeId());

      // Verify report command to show dead DataNode
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn - 1, 0, 0, client, 0L, 1L);

      // Corrupt the replicated block
      final int blockFilesCorrupted = miniCluster
          .corruptBlockOnDataNodes(block);
      assertEquals("Fail to corrupt all replicas for block " + block,
          replFactor, blockFilesCorrupted);

      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(),
            conf, true);
        fail("Should have failed to read the file with corrupted blocks.");
      } catch (ChecksumException ignored) {
        // expected exception reading corrupt blocks
      }

      // Increase replication factor, this should invoke transfer request.
      // Receiving datanode fails on checksum and reports it to namenode
      fs.setReplication(file, (short) (replFactor + 1));

      // get block details and check if the block is corrupt
      BlockManagerTestUtil.updateState(
          miniCluster.getNameNode().getNamesystem().getBlockManager());
      waitForCorruptBlock(miniCluster, client, file);

      // verify report command for corrupt replicated block
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn - 1, 1, 0, client, 0L, 1L);

      lbs = miniCluster.getFileSystem().getClient().
          getNamenode().getBlockLocations(
          ecFile.toString(), 0, blockGroupSize);
      assertTrue("Unexpected block type: " + lbs.get(0),
          lbs.get(0) instanceof LocatedStripedBlock);
      LocatedStripedBlock bg =
          (LocatedStripedBlock)(lbs.get(0));

      miniCluster.getNamesystem().writeLock();
      try {
        BlockManager bm = miniCluster.getNamesystem().getBlockManager();
        bm.findAndMarkBlockAsCorrupt(bg.getBlock(), bg.getLocations()[0],
            "STORAGE_ID", "TEST");
        BlockManagerTestUtil.updateState(bm);
      } finally {
        miniCluster.getNamesystem().writeUnlock();
      }
      waitForCorruptBlock(miniCluster, client, file);

      // verify report command for corrupt replicated block
      // and EC block group
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn - 1, 1, 1, client, 0L, 0L);
    }
  }

  @Test(timeout = 300000L)
  public void testListOpenFiles() throws Exception {
    redirectStream();

    final Configuration dfsConf = new HdfsConfiguration();
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    dfsConf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    dfsConf.setLong(DFSConfigKeys.DFS_NAMENODE_LIST_OPENFILES_NUM_RESPONSES, 5);
    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getAbsolutePath(),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

    final int numDataNodes = 3;
    final int numClosedFiles = 25;
    final int numOpenFiles = 15;

    try(MiniDFSCluster miniCluster = new MiniDFSCluster
        .Builder(dfsConf)
        .numDataNodes(numDataNodes).build()) {
      final short replFactor = 1;
      final long fileLength = 512L;
      final FileSystem fs = miniCluster.getFileSystem();
      final Path parentDir = new Path("/tmp/files/");

      fs.mkdirs(parentDir);
      HashSet<Path> closedFileSet = new HashSet<>();
      for (int i = 0; i < numClosedFiles; i++) {
        Path file = new Path(parentDir, "closed-file-" + i);
        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        closedFileSet.add(file);
      }

      HashMap<Path, FSDataOutputStream> openFilesMap = new HashMap<>();
      for (int i = 0; i < numOpenFiles; i++) {
        Path file = new Path(parentDir, "open-file-" + i);
        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        FSDataOutputStream outputStream = fs.append(file);
        openFilesMap.put(file, outputStream);
      }

      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[]{"-listOpenFiles"}));
      verifyOpenFilesListing(closedFileSet, openFilesMap);

      for (int count = 0; count < numOpenFiles; count++) {
        closedFileSet.addAll(DFSTestUtil.closeOpenFiles(openFilesMap, 1));
        resetStream();
        assertEquals(0, ToolRunner.run(dfsAdmin,
            new String[]{"-listOpenFiles"}));
        verifyOpenFilesListing(closedFileSet, openFilesMap);
      }

      // test -listOpenFiles command with option <path>
      openFilesMap.clear();
      Path file;
      HashMap<Path, FSDataOutputStream> openFiles1 = new HashMap<>();
      HashMap<Path, FSDataOutputStream> openFiles2 = new HashMap<>();
      for (int i = 0; i < numOpenFiles; i++) {
        if (i % 2 == 0) {
          file = new Path(new Path("/tmp/files/a"), "open-file-" + i);
        } else {
          file = new Path(new Path("/tmp/files/b"), "open-file-" + i);
        }

        DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
        FSDataOutputStream outputStream = fs.append(file);

        if (i % 2 == 0) {
          openFiles1.put(file, outputStream);
        } else {
          openFiles2.put(file, outputStream);
        }
        openFilesMap.put(file, outputStream);
      }

      resetStream();
      // list all open files
      assertEquals(0,
          ToolRunner.run(dfsAdmin, new String[] {"-listOpenFiles"}));
      verifyOpenFilesListing(null, openFilesMap);

      resetStream();
      // list open files under directory path /tmp/files/a
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", "/tmp/files/a"}));
      verifyOpenFilesListing(null, openFiles1);

      resetStream();
      // list open files without input path
      assertEquals(-1, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path"}));
      // verify the error
      String outStr = scanIntoString(err);
      assertTrue(outStr.contains("listOpenFiles: option"
          + " -path requires 1 argument"));

      resetStream();
      // list open files with empty path
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", ""}));
      // all the open files will be listed
      verifyOpenFilesListing(null, openFilesMap);

      resetStream();
      // list invalid path file
      assertEquals(0, ToolRunner.run(dfsAdmin,
          new String[] {"-listOpenFiles", "-path", "/invalid_path"}));
      outStr = scanIntoString(out);
      for (Path openFilePath : openFilesMap.keySet()) {
        assertThat(outStr, not(containsString(openFilePath.toString())));
      }
      DFSTestUtil.closeOpenFiles(openFilesMap, openFilesMap.size());
    }
  }

  private void verifyOpenFilesListing(HashSet<Path> closedFileSet,
      HashMap<Path, FSDataOutputStream> openFilesMap) {
    final String outStr = scanIntoString(out);
    LOG.info("dfsadmin -listOpenFiles output: \n" + out);
    if (closedFileSet != null) {
      for (Path closedFilePath : closedFileSet) {
        assertThat(outStr,
            not(containsString(closedFilePath.toString() +
                System.lineSeparator())));
      }
    }

    for (Path openFilePath : openFilesMap.keySet()) {
      assertThat(outStr, is(containsString(openFilePath.toString() +
          System.lineSeparator())));
    }
  }

  private void verifyNodesAndCorruptBlocks(
      final int numDn,
      final int numLiveDn,
      final int numCorruptBlocks,
      final int numCorruptECBlockGroups,
      final DFSClient client,
      final Long highestPriorityLowRedundancyReplicatedBlocks,
      final Long highestPriorityLowRedundancyECBlocks)
      throws IOException {

    /* init vars */
    final String outStr = scanIntoString(out);
    final String expectedLiveNodesStr = String.format(
        "Live datanodes (%d)",
        numLiveDn);
    final String expectedCorruptedBlocksStr = String.format(
        "Blocks with corrupt replicas: %d",
        numCorruptBlocks);
    final String expectedCorruptedECBlockGroupsStr = String.format(
        "Block groups with corrupt internal blocks: %d",
        numCorruptECBlockGroups);
    final String highestPriorityLowRedundancyReplicatedBlocksStr
        = String.format(
        "\tLow redundancy blocks with highest priority " +
            "to recover: %d",
        highestPriorityLowRedundancyReplicatedBlocks);
    final String highestPriorityLowRedundancyECBlocksStr = String.format(
        "\tLow redundancy blocks with highest priority " +
            "to recover: %d",
        highestPriorityLowRedundancyReplicatedBlocks);

    // verify nodes and corrupt blocks
    assertThat(outStr, is(allOf(
        containsString(expectedLiveNodesStr),
        containsString(expectedCorruptedBlocksStr),
        containsString(expectedCorruptedECBlockGroupsStr),
        containsString(highestPriorityLowRedundancyReplicatedBlocksStr),
        containsString(highestPriorityLowRedundancyECBlocksStr))));

    assertEquals(
        numDn,
        client.getDatanodeStorageReport(DatanodeReportType.ALL).length);
    assertEquals(
        numLiveDn,
        client.getDatanodeStorageReport(DatanodeReportType.LIVE).length);
    assertEquals(
        numDn - numLiveDn,
        client.getDatanodeStorageReport(DatanodeReportType.DEAD).length);
    assertEquals(numCorruptBlocks + numCorruptECBlockGroups,
        client.getCorruptBlocksCount());
    assertEquals(numCorruptBlocks, client.getNamenode()
        .getReplicatedBlockStats().getCorruptBlocks());
    assertEquals(highestPriorityLowRedundancyReplicatedBlocks, client.getNamenode()
        .getReplicatedBlockStats().getHighestPriorityLowRedundancyBlocks());
    assertEquals(numCorruptECBlockGroups, client.getNamenode()
        .getECBlockGroupStats().getCorruptBlockGroups());
    assertEquals(highestPriorityLowRedundancyECBlocks, client.getNamenode()
        .getECBlockGroupStats().getHighestPriorityLowRedundancyBlocks());
  }

  @Test
  public void testSetBalancerBandwidth() throws Exception {
    redirectStream();

    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    String outStr;

    // Test basic case: 10000
    assertEquals(0, ToolRunner.run(dfsAdmin,
        new String[]{"-setBalancerBandwidth", "10000"}));
    outStr = scanIntoString(out);
    assertTrue("Did not set bandwidth!", outStr.contains("Balancer " +
        "bandwidth is set to 10000"));

    // Test parsing with units
    resetStream();
    assertEquals(0, ToolRunner.run(dfsAdmin,
        new String[]{"-setBalancerBandwidth", "10m"}));
    outStr = scanIntoString(out);
    assertTrue("Did not set bandwidth!", outStr.contains("Balancer " +
        "bandwidth is set to 10485760"));

    resetStream();
    assertEquals(0, ToolRunner.run(dfsAdmin,
        new String[]{"-setBalancerBandwidth", "10k"}));
    outStr = scanIntoString(out);
    assertTrue("Did not set bandwidth!", outStr.contains("Balancer " +
        "bandwidth is set to 10240"));

    // Test negative numbers
    assertEquals(-1, ToolRunner.run(dfsAdmin,
        new String[]{"-setBalancerBandwidth", "-10000"}));
    assertEquals(-1, ToolRunner.run(dfsAdmin,
        new String[]{"-setBalancerBandwidth", "-10m"}));
  }

  @Test(timeout = 300000L)
  public void testCheckNumOfBlocksInReportCommand() throws Exception {
    DistributedFileSystem dfs = cluster.getFileSystem();
    Path path = new Path("/tmp.txt");

    DatanodeInfo[] dn = dfs.getDataNodeStats();
    assertEquals(dn.length, NUM_DATANODES);
    // Block count should be 0, as no files are created
    int actualBlockCount = 0;
    for (DatanodeInfo d : dn) {
      actualBlockCount += d.getNumBlocks();
    }
    assertEquals(0, actualBlockCount);

    // Create a file with 2 blocks
    DFSTestUtil.createFile(dfs, path, 1024, (short) 1, 0);
    int expectedBlockCount = 2;

    // Wait for One Heartbeat
    Thread.sleep(3 * 1000);

    dn = dfs.getDataNodeStats();
    assertEquals(dn.length, NUM_DATANODES);

    // Block count should be 2, as file is created with block count 2
    actualBlockCount = 0;
    for (DatanodeInfo d : dn) {
      actualBlockCount += d.getNumBlocks();
    }
    assertEquals(expectedBlockCount, actualBlockCount);
  }

  @Test
  public void testRefreshProxyUser() throws Exception {
    Path dirPath = new Path("/testdir1");
    Path subDirPath = new Path("/testdir1/subdir1");
    UserGroupInformation loginUserUgi =  UserGroupInformation.getLoginUser();
    String proxyUser = "fakeuser";
    String realUser = loginUserUgi.getShortUserName();

    UserGroupInformation proxyUgi =
        UserGroupInformation.createProxyUserForTesting(proxyUser,
            loginUserUgi, loginUserUgi.getGroupNames());

    // create a directory as login user and re-assign it to proxy user
    loginUserUgi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        cluster.getFileSystem().mkdirs(dirPath);
        cluster.getFileSystem().setOwner(dirPath, proxyUser,
            proxyUgi.getPrimaryGroupName());
        return 0;
      }
    });

    // try creating subdirectory inside the directory as proxy user,
    // This should fail because of the current user hasn't still been proxied
    try {
      proxyUgi.doAs(new PrivilegedExceptionAction<Integer>() {
        @Override public Integer run() throws Exception {
          cluster.getFileSystem().mkdirs(subDirPath);
          return 0;
        }
      });
    } catch (RemoteException re) {
      Assert.assertTrue(re.unwrapRemoteException()
          instanceof AccessControlException);
      Assert.assertTrue(re.unwrapRemoteException().getMessage()
          .equals("User: " + realUser +
              " is not allowed to impersonate " + proxyUser));
    }

    // refresh will look at configuration on the server side
    // add additional resource with the new value
    // so the server side will pick it up
    String userKeyGroups = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(realUser);
    String userKeyHosts = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserIpConfKey(realUser);
    String rsrc = "testGroupMappingRefresh_rsrc.xml";
    tempResource = TestRefreshUserMappings.addNewConfigResource(rsrc,
        userKeyGroups, "*", userKeyHosts, "*");

    String[] args = new String[]{"-refreshSuperUserGroupsConfiguration"};
    admin.run(args);

    // After proxying the fakeuser, the mkdir should work
    proxyUgi.doAs(new PrivilegedExceptionAction<Integer>() {
      @Override
      public Integer run() throws Exception {
        cluster.getFileSystem().mkdirs(dirPath);
        return 0;
      }
    });
  }
}
