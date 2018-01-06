/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;


import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.CANCEL;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.EXECUTE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.HELP;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.NODE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.OUTFILE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.PLAN;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.QUERY;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.REPORT;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.DiskBalancerTestUtil;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.tools.DiskBalancerCLI;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

/**
 * Tests various CLI commands of DiskBalancer.
 */
public class TestDiskBalancerCommand {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private MiniDFSCluster cluster;
  private URI clusterJson;
  private Configuration conf = new HdfsConfiguration();

  private final static int DEFAULT_BLOCK_SIZE = 1024;
  private final static int FILE_LEN = 200 * 1024;
  private final static long CAPCACITY = 300 * 1024;
  private final static long[] CAPACITIES = new long[] {CAPCACITY, CAPCACITY};

  @Before
  public void setUp() throws Exception {
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .storagesPerDatanode(2).build();
    cluster.waitActive();

    clusterJson = getClass().getResource(
        "/diskBalancer/data-cluster-64node-3disk.json").toURI();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      // Just make sure we can shutdown datanodes.
      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        cluster.getDataNodes().get(i).shutdown();
      }
      cluster.shutdown();
    }
  }

  /**
   * Tests if it's allowed to submit and execute plan when Datanode is in status
   * other than REGULAR.
   */
  @Test(timeout = 60000)
  public void testSubmitPlanInNonRegularStatus() throws Exception {
    final int numDatanodes = 1;
    MiniDFSCluster miniCluster = null;
    final Configuration hdfsConf = new HdfsConfiguration();

    try {
      /* new cluster with imbalanced capacity */
      miniCluster = DiskBalancerTestUtil.newImbalancedCluster(
          hdfsConf,
          numDatanodes,
          CAPACITIES,
          DEFAULT_BLOCK_SIZE,
          FILE_LEN,
          StartupOption.ROLLBACK);

      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      try {
        /* run execute command */
        final String cmdLine = String.format(
            "hdfs diskbalancer -%s %s",
            EXECUTE,
            planFileFullName);
        runCommand(cmdLine, hdfsConf, miniCluster);
      } catch(RemoteException e) {
        assertThat(e.getClassName(), containsString("DiskBalancerException"));
        assertThat(e.toString(),
            is(allOf(
                containsString("Datanode is in special state"),
                containsString("Disk balancing not permitted."))));
      }
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  /**
   * Tests running multiple commands under on setup. This mainly covers
   * {@link org.apache.hadoop.hdfs.server.diskbalancer.command.Command#close}
   */
  @Test(timeout = 60000)
  public void testRunMultipleCommandsUnderOneSetup() throws Exception {

    final int numDatanodes = 1;
    MiniDFSCluster miniCluster = null;
    final Configuration hdfsConf = new HdfsConfiguration();

    try {
      /* new cluster with imbalanced capacity */
      miniCluster = DiskBalancerTestUtil.newImbalancedCluster(
          hdfsConf,
          numDatanodes,
          CAPACITIES,
          DEFAULT_BLOCK_SIZE,
          FILE_LEN);

      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      /* run execute command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s",
          EXECUTE,
          planFileFullName);
      runCommand(cmdLine, hdfsConf, miniCluster);
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  private String runAndVerifyPlan(
      final MiniDFSCluster miniCluster,
      final Configuration hdfsConf) throws Exception {
    String cmdLine = "";
    List<String> outputs = null;
    final DataNode dn = miniCluster.getDataNodes().get(0);

    /* run plan command */
    cmdLine = String.format(
        "hdfs diskbalancer -%s %s",
        PLAN,
        dn.getDatanodeUuid());
    outputs = runCommand(cmdLine, hdfsConf, miniCluster);

    /* get path of plan file*/
    final String planFileName = dn.getDatanodeUuid();

    /* verify plan command */
    assertEquals(
        "There must be two lines: the 1st is writing plan to...,"
            + " the 2nd is actual full path of plan file.",
        2, outputs.size());
    assertThat(outputs.get(1), containsString(planFileName));

    /* get full path of plan file*/
    final String planFileFullName = outputs.get(1);
    return planFileFullName;
  }

  /* test basic report */
  @Test(timeout = 60000)
  public void testReportSimple() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(containsString("No top limit specified"),
            containsString("using default top value"), containsString("100"))));
    assertThat(
        outputs.get(2),
        is(allOf(
            containsString("Reporting top"),
            containsString("64"),
            containsString(
                "DataNode(s) benefiting from running DiskBalancer"))));
    assertThat(
        outputs.get(32),
        is(allOf(containsString("30/64 null[null:0]"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"),
            containsString("9 volumes with node data density 1.97"))));

  }

  /* test basic report with negative top limit */
  @Test(timeout = 60000)
  public void testReportWithNegativeTopLimit()
      throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top -32";
    thrown.expect(java.lang.IllegalArgumentException.class);
    thrown.expectMessage("Top limit input should be a positive numeric value");
    runCommand(cmdLine);
  }
  /* test less than 64 DataNode(s) as total, e.g., -report -top 32 */
  @Test(timeout = 60000)
  public void testReportLessThanTotal() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top 32";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(
            containsString("Reporting top"),
            containsString("32"),
            containsString(
                "DataNode(s) benefiting from running DiskBalancer"))));
    assertThat(
        outputs.get(31),
        is(allOf(containsString("30/32 null[null:0]"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"),
            containsString("9 volumes with node data density 1.97"))));
  }

  /**
   * This test simulates DiskBalancerCLI Report command run from a shell
   * with a generic option 'fs'.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testReportWithGenericOptionFS() throws Exception {
    final String topReportArg = "5";
    final String reportArgs = String.format("-%s file:%s -%s -%s %s",
        "fs", clusterJson.getPath(),
        REPORT, "top", topReportArg);
    final String cmdLine = String.format("%s", reportArgs);
    final List<String> outputs = runCommand(cmdLine);

    assertThat(outputs.get(0), containsString("Processing report command"));
    assertThat(outputs.get(1),
        is(allOf(containsString("Reporting top"), containsString(topReportArg),
            containsString(
                "DataNode(s) benefiting from running DiskBalancer"))));
  }

  /* test more than 64 DataNode(s) as total, e.g., -report -top 128 */
  @Test(timeout = 60000)
  public void testReportMoreThanTotal() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top 128";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(
            containsString("Reporting top"),
            containsString("64"),
            containsString(
                "DataNode(s) benefiting from running DiskBalancer"))));
    assertThat(
        outputs.get(31),
        is(allOf(containsString("30/64 null[null:0]"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"),
            containsString("9 volumes with node data density 1.97"))));

  }

  /* test invalid top limit, e.g., -report -top xx */
  @Test(timeout = 60000)
  public void testReportInvalidTopLimit() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top xx";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(containsString("Top limit input is not numeric"),
            containsString("using default top value"), containsString("100"))));
    assertThat(
        outputs.get(2),
        is(allOf(
            containsString("Reporting top"),
            containsString("64"),
            containsString(
                "DataNode(s) benefiting from running DiskBalancer"))));
    assertThat(
        outputs.get(32),
        is(allOf(containsString("30/64 null[null:0]"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"),
            containsString("9 volumes with node data density 1.97"))));
  }

  @Test(timeout = 60000)
  public void testReportNode() throws Exception {
    final String cmdLine =
        "hdfs diskbalancer -report -node " +
            "a87654a9-54c7-4693-8dd9-c9c7021dc340";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(containsString("Reporting volume information for DataNode"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"))));
    assertThat(
        outputs.get(2),
        is(allOf(containsString("null[null:0]"),
            containsString("a87654a9-54c7-4693-8dd9-c9c7021dc340"),
            containsString("9 volumes with node data density 1.97"))));
    assertThat(
        outputs.get(3),
        is(allOf(containsString("DISK"),
            containsString("/tmp/disk/KmHefYNURo"),
            containsString("0.20 used: 39160240782/200000000000"),
            containsString("0.80 free: 160839759218/200000000000"))));
    assertThat(
        outputs.get(4),
        is(allOf(containsString("DISK"),
            containsString("/tmp/disk/Mxfcfmb24Y"),
            containsString("0.92 used: 733099315216/800000000000"),
            containsString("0.08 free: 66900684784/800000000000"))));
    assertThat(
        outputs.get(5),
        is(allOf(containsString("DISK"),
            containsString("/tmp/disk/xx3j3ph3zd"),
            containsString("0.72 used: 289544224916/400000000000"),
            containsString("0.28 free: 110455775084/400000000000"))));
    assertThat(
        outputs.get(6),
        is(allOf(containsString("RAM_DISK"),
            containsString("/tmp/disk/BoBlQFxhfw"),
            containsString("0.60 used: 477590453390/800000000000"),
            containsString("0.40 free: 322409546610/800000000000"))));
    assertThat(
        outputs.get(7),
        is(allOf(containsString("RAM_DISK"),
            containsString("/tmp/disk/DtmAygEU6f"),
            containsString("0.34 used: 134602910470/400000000000"),
            containsString("0.66 free: 265397089530/400000000000"))));
    assertThat(
        outputs.get(8),
        is(allOf(containsString("RAM_DISK"),
            containsString("/tmp/disk/MXRyYsCz3U"),
            containsString("0.55 used: 438102096853/800000000000"),
            containsString("0.45 free: 361897903147/800000000000"))));
    assertThat(
        outputs.get(9),
        is(allOf(containsString("SSD"),
            containsString("/tmp/disk/BGe09Y77dI"),
            containsString("0.89 used: 890446265501/1000000000000"),
            containsString("0.11 free: 109553734499/1000000000000"))));
    assertThat(
        outputs.get(10),
        is(allOf(containsString("SSD"),
            containsString("/tmp/disk/JX3H8iHggM"),
            containsString("0.31 used: 2782614512957/9000000000000"),
            containsString("0.69 free: 6217385487043/9000000000000"))));
    assertThat(
        outputs.get(11),
        is(allOf(containsString("SSD"),
            containsString("/tmp/disk/uLOYmVZfWV"),
            containsString("0.75 used: 1509592146007/2000000000000"),
            containsString("0.25 free: 490407853993/2000000000000"))));
  }

  @Test(timeout = 60000)
  public void testReportNodeWithoutJson() throws Exception {
    String dataNodeUuid = cluster.getDataNodes().get(0).getDatanodeUuid();
    final String planArg = String.format("-%s -%s %s",
        REPORT, NODE, dataNodeUuid);
    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(containsString("Reporting volume information for DataNode"),
            containsString(dataNodeUuid))));
    assertThat(
        outputs.get(2),
        is(allOf(containsString(dataNodeUuid),
            containsString("2 volumes with node data density 0.00"))));
    assertThat(
        outputs.get(3),
        is(allOf(containsString("DISK"),
            containsString("/dfs/data/data1"),
            containsString("0.00"),
            containsString("1.00"))));
    assertThat(
        outputs.get(4),
        is(allOf(containsString("DISK"),
            containsString("/dfs/data/data2"),
            containsString("0.00"),
            containsString("1.00"))));
  }

  @Test(timeout = 60000)
  public void testReadClusterFromJson() throws Exception {
    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        conf);
    DiskBalancerCluster diskBalancerCluster = new DiskBalancerCluster(
        jsonConnector);
    diskBalancerCluster.readClusterInfo();
    assertEquals(64, diskBalancerCluster.getNodes().size());
  }

  /* test -plan  DataNodeID */
  @Test(timeout = 60000)
  public void testPlanNode() throws Exception {
    final String planArg = String.format("-%s %s", PLAN,
        cluster.getDataNodes().get(0).getDatanodeUuid());

    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", planArg);
    runCommand(cmdLine, cluster);
  }

  /* test -plan  DataNodeID */
  @Test(timeout = 60000)
  public void testPlanJsonNode() throws Exception {
    final String planArg = String.format("-%s %s", PLAN,
        "a87654a9-54c7-4693-8dd9-c9c7021dc340");
    final Path testPath = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());
    final String cmdLine = String
        .format(
            "hdfs diskbalancer -out %s %s", testPath, planArg);
    runCommand(cmdLine);
  }

  /* Test that illegal arguments are handled correctly*/
  @Test(timeout = 60000)
  public void testIllegalArgument() throws Exception {
    final String planArg = String.format("-%s %s", PLAN,
        "a87654a9-54c7-4693-8dd9-c9c7021dc340");

    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s -report", planArg);
    // -plan and -report cannot be used together.
    // tests the validate command line arguments function.
    thrown.expect(java.lang.IllegalArgumentException.class);
    runCommand(cmdLine);
  }

  @Test(timeout = 60000)
  public void testCancelCommand() throws Exception {
    final String cancelArg = String.format("-%s %s", CANCEL, "nosuchplan");
    final String nodeArg = String.format("-%s %s", NODE,
        cluster.getDataNodes().get(0).getDatanodeUuid());

    // Port:Host format is expected. So cancel command will throw.
    thrown.expect(java.lang.IllegalArgumentException.class);
    final String cmdLine = String
        .format(
            "hdfs diskbalancer  %s %s", cancelArg, nodeArg);
    runCommand(cmdLine);
  }

  /*
   Makes an invalid query attempt to non-existent Datanode.
   */
  @Test(timeout = 60000)
  public void testQueryCommand() throws Exception {
    final String queryArg = String.format("-%s %s", QUERY,
        cluster.getDataNodes().get(0).getDatanodeUuid());
    thrown.expect(java.net.UnknownHostException.class);
    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", queryArg);
    runCommand(cmdLine);
  }

  @Test(timeout = 60000)
  public void testHelpCommand() throws Exception {
    final String helpArg = String.format("-%s", HELP);
    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", helpArg);
    runCommand(cmdLine);
  }

  @Test
  public void testPrintFullPathOfPlan()
      throws Exception {
    final Path parent = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());

    MiniDFSCluster miniCluster = null;
    try {
      Configuration hdfsConf = new HdfsConfiguration();
      List<String> outputs = null;

      /* new cluster with imbalanced capacity */
      miniCluster = DiskBalancerTestUtil.newImbalancedCluster(
          hdfsConf,
          1,
          CAPACITIES,
          DEFAULT_BLOCK_SIZE,
          FILE_LEN);

      /* run plan command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s -%s %s",
          PLAN,
          miniCluster.getDataNodes().get(0).getDatanodeUuid(),
          OUTFILE,
          parent);
      outputs = runCommand(cmdLine, hdfsConf, miniCluster);

      /* get full path */
      final String planFileFullName = new Path(
          parent,
          miniCluster.getDataNodes().get(0).getDatanodeUuid()).toString();

      /* verify the path of plan */
      assertEquals(
          "There must be two lines: the 1st is writing plan to,"
              + " the 2nd is actual full path of plan file.",
          2, outputs.size());
      assertThat(outputs.get(0), containsString("Writing plan to"));
      assertThat(outputs.get(1), containsString(planFileFullName));
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  private List<String> runCommandInternal(
      final String cmdLine,
      final Configuration clusterConf) throws Exception {
    String[] cmds = StringUtils.split(cmdLine, ' ');
    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bufOut);

    Tool diskBalancerTool = new DiskBalancerCLI(clusterConf, out);
    ToolRunner.run(clusterConf, diskBalancerTool, cmds);

    Scanner scanner = new Scanner(bufOut.toString());
    List<String> outputs = Lists.newArrayList();
    while (scanner.hasNextLine()) {
      outputs.add(scanner.nextLine());
    }
    return outputs;
  }

  private List<String> runCommandInternal(final String cmdLine)
      throws Exception {
    return runCommandInternal(cmdLine, conf);
  }

  private List<String> runCommand(final String cmdLine) throws Exception {
    FileSystem.setDefaultUri(conf, clusterJson);
    return runCommandInternal(cmdLine);
  }

  private List<String> runCommand(final String cmdLine,
                                  MiniDFSCluster miniCluster) throws Exception {
    FileSystem.setDefaultUri(conf, miniCluster.getURI());
    return runCommandInternal(cmdLine);
  }

  private List<String> runCommand(
      final String cmdLine,
      Configuration clusterConf,
      MiniDFSCluster miniCluster) throws Exception {
    FileSystem.setDefaultUri(clusterConf, miniCluster.getURI());
    return runCommandInternal(cmdLine, clusterConf);
  }

  /**
   * Making sure that we can query the node without having done a submit.
   * @throws Exception
   */
  @Test
  public void testDiskBalancerQueryWithoutSubmit() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int numDatanodes = 2;
    MiniDFSCluster miniDFSCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes).build();
    try {
      miniDFSCluster.waitActive();
      DataNode dataNode = miniDFSCluster.getDataNodes().get(0);
      final String queryArg = String.format("-query localhost:%d", dataNode
          .getIpcPort());
      final String cmdLine = String.format("hdfs diskbalancer %s",
          queryArg);
      runCommand(cmdLine);
    } finally {
      miniDFSCluster.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testGetNodeList() throws Exception {
    ClusterConnector jsonConnector =
        ConnectorFactory.getCluster(clusterJson, conf);
    DiskBalancerCluster diskBalancerCluster =
        new DiskBalancerCluster(jsonConnector);
    diskBalancerCluster.readClusterInfo();

    int nodeNum = 5;
    StringBuilder listArg = new StringBuilder();
    for (int i = 0; i < nodeNum; i++) {
      listArg.append(diskBalancerCluster.getNodes().get(i).getDataNodeUUID())
          .append(",");
    }

    ReportCommand command = new ReportCommand(conf, null);
    command.setCluster(diskBalancerCluster);
    List<DiskBalancerDataNode> nodeList = command.getNodes(listArg.toString());
    assertEquals(nodeNum, nodeList.size());
  }

  @Test(timeout = 60000)
  public void testReportCommandWithMultipleNodes() throws Exception {
    String dataNodeUuid1 = cluster.getDataNodes().get(0).getDatanodeUuid();
    String dataNodeUuid2 = cluster.getDataNodes().get(1).getDatanodeUuid();
    final String planArg = String.format("-%s -%s %s,%s",
        REPORT, NODE, dataNodeUuid1, dataNodeUuid2);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);
    verifyOutputsOfReportCommand(outputs, dataNodeUuid1, dataNodeUuid2, true);
  }

  private void verifyOutputsOfReportCommand(List<String> outputs,
      String dataNodeUuid1, String dataNodeUuid2, boolean inputNodesStr) {
    assertThat(outputs.get(0), containsString("Processing report command"));
    if (inputNodesStr) {
      assertThat(outputs.get(1),
          is(allOf(containsString("Reporting volume information for DataNode"),
              containsString(dataNodeUuid1), containsString(dataNodeUuid2))));
    }

    // Since the order of input nodes will be disrupted when parse
    // the node string, we should compare UUID with both output lines.
    assertTrue(outputs.get(2).contains(dataNodeUuid1)
        || outputs.get(6).contains(dataNodeUuid1));
    assertTrue(outputs.get(2).contains(dataNodeUuid2)
        || outputs.get(6).contains(dataNodeUuid2));
  }

  @Test(timeout = 60000)
  public void testReportCommandWithInvalidNode() throws Exception {
    String dataNodeUuid1 = cluster.getDataNodes().get(0).getDatanodeUuid();
    String invalidNode = "invalidNode";
    final String planArg = String.format("-%s -%s %s,%s",
        REPORT, NODE, dataNodeUuid1, invalidNode);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    assertThat(
        outputs.get(0),
        containsString("Processing report command"));
    assertThat(
        outputs.get(1),
        is(allOf(containsString("Reporting volume information for DataNode"),
            containsString(dataNodeUuid1), containsString(invalidNode))));

    String invalidNodeInfo =
        String.format("The node(s) '%s' not found. "
            + "Please make sure that '%s' exists in the cluster."
            , invalidNode, invalidNode);
    assertTrue(outputs.get(2).contains(invalidNodeInfo));
  }

  @Test(timeout = 60000)
  public void testReportCommandWithNullNodes() throws Exception {
    // don't input nodes
    final String planArg = String.format("-%s -%s ,", REPORT, NODE);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    String invalidNodeInfo = "The number of input nodes is 0. "
        + "Please input the valid nodes.";
    assertTrue(outputs.get(2).contains(invalidNodeInfo));
  }

  @Test(timeout = 60000)
  public void testReportCommandWithReadingHostFile() throws Exception {
    final String testDir = GenericTestUtils.getTestDir().getAbsolutePath();
    File includeFile = new File(testDir, "diskbalancer.include");
    String filePath = testDir + "/diskbalancer.include";

    String dataNodeUuid1 = cluster.getDataNodes().get(0).getDatanodeUuid();
    String dataNodeUuid2 = cluster.getDataNodes().get(1).getDatanodeUuid();

    FileWriter fw = new FileWriter(filePath);
    fw.write("#This-is-comment\n");
    fw.write(dataNodeUuid1 + "\n");
    fw.write(dataNodeUuid2 + "\n");
    fw.close();

    final String planArg = String.format("-%s -%s file://%s",
        REPORT, NODE, filePath);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    verifyOutputsOfReportCommand(outputs, dataNodeUuid1, dataNodeUuid2, false);
    includeFile.delete();
  }

  @Test(timeout = 60000)
  public void testReportCommandWithInvalidHostFilePath() throws Exception {
    final String testDir = GenericTestUtils.getTestDir().getAbsolutePath();
    String invalidFilePath = testDir + "/diskbalancer-invalid.include";

    final String planArg = String.format("-%s -%s file://%s",
        REPORT, NODE, invalidFilePath);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    String invalidNodeInfo = String.format(
        "The input host file path 'file://%s' is not a valid path.", invalidFilePath);
    assertTrue(outputs.get(2).contains(invalidNodeInfo));
  }
}
