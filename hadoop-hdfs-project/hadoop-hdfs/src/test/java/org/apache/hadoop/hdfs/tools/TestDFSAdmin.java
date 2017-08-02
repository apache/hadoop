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

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

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
  }

  private List<String> getReconfigureStatus(String nodeType, String address)
      throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream outStream = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream errStream = new PrintStream(bufErr);
    admin.getReconfigurationStatus(nodeType, address, outStream, errStream);
    List<String> outputs = Lists.newArrayList();
    scanIntoList(bufOut, outputs);
    return outputs;
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

  /**
   * Test reconfiguration and check the status outputs.
   * @param expectedSuccuss set true if the reconfiguration task should success.
   * @throws IOException
   * @throws InterruptedException
   */
  private void testGetReconfigurationStatus(boolean expectedSuccuss)
      throws IOException, InterruptedException {
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

    List<String> outputs = null;
    int count = 100;
    while (count > 0) {
      outputs = getReconfigureStatus("datanode", address);
      if (!outputs.isEmpty() && outputs.get(0).contains("finished")) {
        break;
      }
      count--;
      Thread.sleep(100);
    }
    assertTrue(count > 0);
    if (expectedSuccuss) {
      assertThat(outputs.size(), is(4));
    } else {
      assertThat(outputs.size(), is(6));
    }

    List<StorageLocation> locations = DataNode.getStorageLocations(
        datanode.getConf());
    if (expectedSuccuss) {
      assertThat(locations.size(), is(1));
      assertThat(locations.get(0).getFile(), is(newDir));
      // Verify the directory is appropriately formatted.
      assertTrue(new File(newDir, Storage.STORAGE_DIR_CURRENT).isDirectory());
    } else {
      assertTrue(locations.isEmpty());
    }

    int offset = 1;
    if (expectedSuccuss) {
      assertThat(outputs.get(offset),
          containsString("SUCCESS: Changed property " +
              DFS_DATANODE_DATA_DIR_KEY));
    } else {
      assertThat(outputs.get(offset),
          containsString("FAILED: Change property " +
              DFS_DATANODE_DATA_DIR_KEY));
    }
    assertThat(outputs.get(offset + 1),
        is(allOf(containsString("From:"), containsString("data1"),
            containsString("data2"))));
    assertThat(outputs.get(offset + 2),
        is(not(anyOf(containsString("data1"), containsString("data2")))));
    assertThat(outputs.get(offset + 2),
        is(allOf(containsString("To"), containsString("data_new"))));
  }

  @Test(timeout = 30000)
  public void testGetReconfigurationStatus()
      throws IOException, InterruptedException {
    testGetReconfigurationStatus(true);
    restartCluster();
    testGetReconfigurationStatus(false);
  }

  private List<String> getReconfigurationAllowedProperties(
      String nodeType, String address)
      throws IOException {
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream outStream = new PrintStream(bufOut);
    ByteArrayOutputStream bufErr = new ByteArrayOutputStream();
    PrintStream errStream = new PrintStream(bufErr);
    admin.getReconfigurableProperties(nodeType, address, outStream, errStream);
    List<String> outputs = Lists.newArrayList();
    scanIntoList(bufOut, outputs);
    return outputs;
  }

  @Test(timeout = 30000)
  public void testGetReconfigAllowedProperties() throws IOException {
    final int port = datanode.getIpcPort();
    final String address = "localhost:" + port;
    List<String> outputs =
        getReconfigurationAllowedProperties("datanode", address);
    assertEquals(3, outputs.size());
    assertEquals(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        outputs.get(1));
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

  @Test(timeout = 120000)
  public void testReportCommand() throws Exception {
    redirectStream();

    /* init conf */
    final Configuration dfsConf = new HdfsConfiguration();
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        500); // 0.5s
    dfsConf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    final Path baseDir = new Path(
        PathUtils.getTestDir(getClass()).getAbsolutePath(),
        GenericTestUtils.getMethodName());
    dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString());

    final int numDn = 3;

    /* init cluster */
    try(MiniDFSCluster miniCluster = new MiniDFSCluster
        .Builder(dfsConf)
        .numDataNodes(numDn).build()) {

      miniCluster.waitActive();
      assertEquals(numDn, miniCluster.getDataNodes().size());

      /* local vars */
      final DFSAdmin dfsAdmin = new DFSAdmin(dfsConf);
      final DFSClient client = miniCluster.getFileSystem().getClient();

      /* run and verify report command */
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn, 0, client);

      /* shut down one DN */
      final List<DataNode> datanodes = miniCluster.getDataNodes();
      final DataNode last = datanodes.get(datanodes.size() - 1);
      last.shutdown();
      miniCluster.setDataNodeDead(last.getDatanodeId());

      /* run and verify report command */
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn - 1, 0, client);

      /* corrupt one block */
      final short replFactor = 1;
      final long fileLength = 512L;
      final FileSystem fs = miniCluster.getFileSystem();
      final Path file = new Path(baseDir, "/corrupted");
      DFSTestUtil.createFile(fs, file, fileLength, replFactor, 12345L);
      DFSTestUtil.waitReplication(fs, file, replFactor);

      final ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file);
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

      /*
       * Increase replication factor, this should invoke transfer request.
       * Receiving datanode fails on checksum and reports it to namenode
       */
      fs.setReplication(file, (short) (replFactor + 1));

      /* get block details and check if the block is corrupt */
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

      BlockManagerTestUtil.updateState(
          miniCluster.getNameNode().getNamesystem().getBlockManager());

      /* run and verify report command */
      resetStream();
      assertEquals(0, ToolRunner.run(dfsAdmin, new String[] {"-report"}));
      verifyNodesAndCorruptBlocks(numDn, numDn - 1, 1, client);
    }
  }

  @Test(timeout = 300000L)
  public void testListOpenFiles() throws Exception {
    redirectStream();

    final Configuration dfsConf = new HdfsConfiguration();
    dfsConf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    dfsConf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
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
    }
  }

  private void verifyOpenFilesListing(HashSet<Path> closedFileSet,
      HashMap<Path, FSDataOutputStream> openFilesMap) {
    final String outStr = scanIntoString(out);
    LOG.info("dfsadmin -listOpenFiles output: \n" + out);
    for (Path closedFilePath : closedFileSet) {
      assertThat(outStr, not(containsString(closedFilePath.toString() + "\n")));
    }
    for (Path openFilePath : openFilesMap.keySet()) {
      assertThat(outStr, is(containsString(openFilePath.toString() + "\n")));
    }
  }

  private void verifyNodesAndCorruptBlocks(
      final int numDn,
      final int numLiveDn,
      final int numCorruptBlocks,
      final DFSClient client) throws IOException {

    /* init vars */
    final String outStr = scanIntoString(out);
    final String expectedLiveNodesStr = String.format(
        "Live datanodes (%d)",
        numLiveDn);
    final String expectedCorruptedBlocksStr = String.format(
        "Blocks with corrupt replicas: %d",
        numCorruptBlocks);

    /* verify nodes and corrupt blocks */
    assertThat(outStr, is(allOf(
        containsString(expectedLiveNodesStr),
        containsString(expectedCorruptedBlocksStr))));

    assertEquals(
        numDn,
        client.getDatanodeStorageReport(DatanodeReportType.ALL).length);
    assertEquals(
        numLiveDn,
        client.getDatanodeStorageReport(DatanodeReportType.LIVE).length);
    assertEquals(
        numDn - numLiveDn,
        client.getDatanodeStorageReport(DatanodeReportType.DEAD).length);
    assertEquals(numCorruptBlocks, client.getCorruptBlocksCount());
  }
}
