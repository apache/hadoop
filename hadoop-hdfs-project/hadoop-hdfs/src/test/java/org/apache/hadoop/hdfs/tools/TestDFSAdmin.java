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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
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

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 3);
    restartCluster();

    admin = new DFSAdmin();
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
  }

  private void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
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
    assertThat(outs.get(offset + 1),
        is(allOf(containsString("From:"), containsString("data1"),
            containsString("data2"))));
    assertThat(outs.get(offset + 2),
        is(not(anyOf(containsString("data1"), containsString("data2")))));
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
    assertEquals(6, outs.size());
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
}