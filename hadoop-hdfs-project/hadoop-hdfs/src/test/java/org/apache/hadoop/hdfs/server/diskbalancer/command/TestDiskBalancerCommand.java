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


import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.CANCEL;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.EXECUTE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.HELP;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.NODE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.OUTFILE;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.PLAN;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.QUERY;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.REPORT;
import static org.apache.hadoop.hdfs.tools.DiskBalancerCLI.SKIPDATECHECK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
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
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests various CLI commands of DiskBalancer.
 */
public class TestDiskBalancerCommand {

  private MiniDFSCluster cluster;
  private URI clusterJson;
  private Configuration conf = new HdfsConfiguration();

  private final static int DEFAULT_BLOCK_SIZE = 1024;
  private final static int FILE_LEN = 200 * 1024;
  private final static long CAPCACITY = 300 * 1024;
  private final static long[] CAPACITIES = new long[] {CAPCACITY, CAPCACITY};

  @BeforeEach
  public void setUp() throws Exception {
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .storagesPerDatanode(2).build();
    cluster.waitActive();

    clusterJson = getClass().getResource(
        "/diskBalancer/data-cluster-64node-3disk.json").toURI();
  }

  @AfterEach
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
  @Test
  @Timeout(value = 60)
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
        assertThat(e.getClassName()).contains("DiskBalancerException");
        assertThat(e.toString()).contains("Datanode is in special state")
            .contains("Disk balancing not permitted.");
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
  @Test
  @Timeout(value = 120)
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



  @Test
  @Timeout(value = 600)
  public void testDiskBalancerExecuteOptionPlanValidityWithException() throws
      Exception {
    final int numDatanodes = 1;

    final Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    hdfsConf.set(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "0d");

    /* new cluster with imbalanced capacity */
    final MiniDFSCluster miniCluster = DiskBalancerTestUtil.
        newImbalancedCluster(
        hdfsConf,
        numDatanodes,
        CAPACITIES,
        DEFAULT_BLOCK_SIZE,
        FILE_LEN);

    try {
      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      /* run execute command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s",
          EXECUTE,
          planFileFullName);

      LambdaTestUtils.intercept(
          RemoteException.class,
          "DiskBalancerException",
          "Plan was generated more than 0d ago",
          () -> {
            runCommand(cmdLine, hdfsConf, miniCluster);
          });
    }  finally{
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  @Test
  @Timeout(value = 600)
  public void testDiskBalancerExecutePlanValidityWithOutUnitException()
      throws
      Exception {
    final int numDatanodes = 1;

    final Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    hdfsConf.set(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "0");

    /* new cluster with imbalanced capacity */
    final MiniDFSCluster miniCluster = DiskBalancerTestUtil.
        newImbalancedCluster(
            hdfsConf,
            numDatanodes,
            CAPACITIES,
            DEFAULT_BLOCK_SIZE,
            FILE_LEN);

    try {
      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      /* run execute command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s",
          EXECUTE,
          planFileFullName);

      LambdaTestUtils.intercept(
          RemoteException.class,
          "DiskBalancerException",
          "Plan was generated more than 0ms ago",
          () -> {
            runCommand(cmdLine, hdfsConf, miniCluster);
          });
    }  finally{
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  @Test
  @Timeout(value = 600)
  public void testDiskBalancerForceExecute() throws
      Exception {
    final int numDatanodes = 1;

    final Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    hdfsConf.set(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "0d");

    /* new cluster with imbalanced capacity */
    final MiniDFSCluster miniCluster = DiskBalancerTestUtil.
        newImbalancedCluster(
            hdfsConf,
            numDatanodes,
            CAPACITIES,
            DEFAULT_BLOCK_SIZE,
            FILE_LEN);

    try {
      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      /* run execute command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s -%s",
          EXECUTE,
          planFileFullName,
          SKIPDATECHECK);

      // Disk Balancer should execute the plan, as skipDateCheck Option is
      // specified
      runCommand(cmdLine, hdfsConf, miniCluster);
    }  finally{
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }


  @Test
  @Timeout(value = 600)
  public void testDiskBalancerExecuteOptionPlanValidity() throws Exception {
    final int numDatanodes = 1;

    final Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    hdfsConf.set(DFSConfigKeys.DFS_DISK_BALANCER_PLAN_VALID_INTERVAL, "600s");

    /* new cluster with imbalanced capacity */
    final MiniDFSCluster miniCluster = DiskBalancerTestUtil.
        newImbalancedCluster(
            hdfsConf,
            numDatanodes,
            CAPACITIES,
            DEFAULT_BLOCK_SIZE,
            FILE_LEN);

    try {
      /* get full path of plan */
      final String planFileFullName = runAndVerifyPlan(miniCluster, hdfsConf);

      /* run execute command */
      final String cmdLine = String.format(
          "hdfs diskbalancer -%s %s",
          EXECUTE,
          planFileFullName);

      // Plan is valid for 600 seconds, sleeping for 10seconds, so now
      // diskbalancer should execute the plan
      sleep(10000);
      runCommand(cmdLine, hdfsConf, miniCluster);
    }  finally{
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
    assertEquals(2, outputs.size(), "There must be two lines: the 1st is writing plan to...,"
        + " the 2nd is actual full path of plan file.");
    assertThat(outputs.get(1)).contains(planFileName);

    /* get full path of plan file*/
    final String planFileFullName = outputs.get(1);
    return planFileFullName;
  }

  /* test exception on invalid arguments */
  @Test
  @Timeout(value = 60)
  public void testExceptionOnInvalidArguments() throws Exception {
    final String cmdLine = "hdfs diskbalancer random1 -report random2 random3";
    HadoopIllegalArgumentException ex = assertThrows(HadoopIllegalArgumentException.class, () -> {
      runCommand(cmdLine);
    });
    assertTrue(ex.getMessage().contains(
        "Invalid or extra Arguments: [random1, random2, random3]"));
  }

  /* test basic report */
  @Test
  @Timeout(value = 60)
  public void testReportSimple() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0)).
        contains("Processing report command");
    assertThat(
        outputs.get(1))
        .contains("No top limit specified")
        .contains("using default top value")
        .contains("100");
    assertThat(
        outputs.get(2))
        .contains("Reporting top")
        .contains("64")
        .contains("DataNode(s) benefiting from running DiskBalancer");
    assertThat(
        outputs.get(32))
        .contains("30/64 null[null:0]")
        .contains("a87654a9-54c7-4693-8dd9-c9c7021dc340")
        .contains("9 volumes with node data density 1.97");

  }

  /* test basic report with negative top limit */
  @Test
  @Timeout(value = 60)
  public void testReportWithNegativeTopLimit()
      throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top -32";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
      runCommand(cmdLine);
    });
    assertTrue(ex.getMessage().contains("Top limit input should be a positive numeric value"));
  }
  /* test less than 64 DataNode(s) as total, e.g., -report -top 32 */
  @Test
  @Timeout(value = 60)
  public void testReportLessThanTotal() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top 32";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0))
        .contains("Processing report command");
    assertThat(
        outputs.get(1))
        .contains(
            "Reporting top",
            "32",
            "DataNode(s) benefiting from running DiskBalancer"
        );
    assertThat(outputs.get(31))
        .contains(
            "30/32 null[null:0]",
            "a87654a9-54c7-4693-8dd9-c9c7021dc340",
            "9 volumes with node data density 1.97"
        );
  }

  /**
   * This test simulates DiskBalancerCLI Report command run from a shell
   * with a generic option 'fs'.
   * @throws Exception
   */
  @Test
  @Timeout(value = 60)
  public void testReportWithGenericOptionFS() throws Exception {
    final String topReportArg = "5";
    final String reportArgs = String.format("-%s file:%s -%s -%s %s",
        "fs", clusterJson.getPath(),
        REPORT, "top", topReportArg);
    final String cmdLine = String.format("%s", reportArgs);
    final List<String> outputs = runCommand(cmdLine);

    assertThat(outputs.get(0)).contains("Processing report command");
    assertThat(outputs.get(1))
        .contains(
            "Reporting top",
            topReportArg,
            "DataNode(s) benefiting from running DiskBalancer");
  }

  /* test more than 64 DataNode(s) as total, e.g., -report -top 128 */
  @Test
  @Timeout(value = 60)
  public void testReportMoreThanTotal() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top 128";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0)).contains("Processing report command");
    assertThat(outputs.get(1))
        .contains(
            "Reporting top",
            "64",
            "DataNode(s) benefiting from running DiskBalancer"
        );
    assertThat(outputs.get(31))
        .contains(
            "30/64 null[null:0]",
            "a87654a9-54c7-4693-8dd9-c9c7021dc340",
            "9 volumes with node data density 1.97"
        );
  }

  /* test invalid top limit, e.g., -report -top xx */
  @Test
  @Timeout(value = 60)
  public void testReportInvalidTopLimit() throws Exception {
    final String cmdLine = "hdfs diskbalancer -report -top xx";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0)).contains("Processing report command");
    assertThat(outputs.get(1))
        .contains(
            "Top limit input is not numeric",
            "using default top value",
            "100"
        );
    assertThat(outputs.get(2))
        .contains(
            "Reporting top",
            "64",
            "DataNode(s) benefiting from running DiskBalancer"
        );
    assertThat(outputs.get(32))
        .contains(
            "30/64 null[null:0]",
            "a87654a9-54c7-4693-8dd9-c9c7021dc340",
            "9 volumes with node data density 1.97"
        );
  }

  @Test
  @Timeout(value = 60)
  public void testReportNode() throws Exception {
    final String cmdLine =
        "hdfs diskbalancer -report -node " +
            "a87654a9-54c7-4693-8dd9-c9c7021dc340";
    final List<String> outputs = runCommand(cmdLine);

    assertThat(
        outputs.get(0)).contains("Processing report command");
    assertThat(outputs.get(1))
        .contains(
            "Reporting volume information for DataNode",
            "a87654a9-54c7-4693-8dd9-c9c7021dc340"
        );
    assertThat(outputs.get(2))
        .contains(
            "null[null:0]",
            "a87654a9-54c7-4693-8dd9-c9c7021dc340",
            "9 volumes with node data density 1.97"
        );
    assertThat(outputs.get(3))
        .contains(
            "DISK",
            "/tmp/disk/KmHefYNURo",
            "0.20 used: 39160240782/200000000000",
            "0.80 free: 160839759218/200000000000"
        );
    assertThat(outputs.get(4))
        .contains(
            "DISK",
            "/tmp/disk/Mxfcfmb24Y",
            "0.92 used: 733099315216/800000000000",
            "0.08 free: 66900684784/800000000000"
        );
    assertThat(outputs.get(5))
        .contains(
            "DISK",
            "/tmp/disk/xx3j3ph3zd",
            "0.72 used: 289544224916/400000000000",
            "0.28 free: 110455775084/400000000000"
        );
    assertThat(outputs.get(6))
        .contains(
            "RAM_DISK",
            "/tmp/disk/BoBlQFxhfw",
            "0.60 used: 477590453390/800000000000",
            "0.40 free: 322409546610/800000000000"
        );
    assertThat(outputs.get(7))
        .contains(
            "RAM_DISK",
            "/tmp/disk/DtmAygEU6f",
            "0.34 used: 134602910470/400000000000",
            "0.66 free: 265397089530/400000000000"
        );
    assertThat(outputs.get(8))
        .contains(
            "RAM_DISK",
            "/tmp/disk/MXRyYsCz3U",
            "0.55 used: 438102096853/800000000000",
            "0.45 free: 361897903147/800000000000"
        );
    assertThat(outputs.get(9))
        .contains(
            "SSD",
            "/tmp/disk/BGe09Y77dI",
            "0.89 used: 890446265501/1000000000000",
            "0.11 free: 109553734499/1000000000000"
        );
    assertThat(outputs.get(10))
        .contains(
            "SSD",
            "/tmp/disk/JX3H8iHggM",
            "0.31 used: 2782614512957/9000000000000",
            "0.69 free: 6217385487043/9000000000000"
        );
    assertThat(outputs.get(11))
        .contains(
            "SSD",
            "/tmp/disk/uLOYmVZfWV",
            "0.75 used: 1509592146007/2000000000000",
            "0.25 free: 490407853993/2000000000000"
        );
  }

  @Test
  @Timeout(value = 60)
  public void testReportNodeWithoutJson() throws Exception {
    String dataNodeUuid = cluster.getDataNodes().get(0).getDatanodeUuid();
    final String planArg = String.format("-%s -%s %s",
        REPORT, NODE, dataNodeUuid);
    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    assertThat(
        outputs.get(0)).contains("Processing report command");
    assertThat(outputs.get(1))
        .contains(
            "Reporting volume information for DataNode",
            dataNodeUuid
        );
    assertThat(outputs.get(2))
        .contains(
            dataNodeUuid,
            "2 volumes with node data density 0.00"
        );
    assertThat(outputs.get(3))
        .contains(
            "DISK",
            new Path(cluster.getInstanceStorageDir(0, 0).getAbsolutePath()).toString(),
            "0.00",
            "1.00"
        );
    assertThat(outputs.get(4))
        .contains(
            "DISK",
            new Path(cluster.getInstanceStorageDir(0, 1).getAbsolutePath()).toString(),
            "0.00",
            "1.00"
        );
  }

  @Test
  @Timeout(value = 60)
  public void testReadClusterFromJson() throws Exception {
    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        conf);
    DiskBalancerCluster diskBalancerCluster = new DiskBalancerCluster(
        jsonConnector);
    diskBalancerCluster.readClusterInfo();
    assertEquals(64, diskBalancerCluster.getNodes().size());
  }

  /* test -plan  DataNodeID */
  @Test
  @Timeout(value = 60)
  public void testPlanNode() throws Exception {
    final String planArg = String.format("-%s %s", PLAN,
        cluster.getDataNodes().get(0).getDatanodeUuid());

    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s", planArg);
    runCommand(cmdLine, cluster);
  }

  /* test -plan  DataNodeID */
  @Test
  @Timeout(value = 60)
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
  @Test
  @Timeout(value = 60)
  public void testIllegalArgument() throws Exception {
    final String planArg = String.format("-%s %s", PLAN,
        "a87654a9-54c7-4693-8dd9-c9c7021dc340");

    final String cmdLine = String
        .format(
            "hdfs diskbalancer %s -report", planArg);
    // -plan and -report cannot be used together.
    // tests the validate command line arguments function.
    assertThrows(java.lang.IllegalArgumentException.class, () -> {
      runCommand(cmdLine);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testCancelCommand() throws Exception {
    final String cancelArg = String.format("-%s %s", CANCEL, "nosuchplan");
    final String nodeArg = String.format("-%s %s", NODE,
        cluster.getDataNodes().get(0).getDatanodeUuid());

    // Port:Host format is expected. So cancel command will throw.
    assertThrows(java.lang.IllegalArgumentException.class, () -> {
      final String cmdLine = String
          .format(
              "hdfs diskbalancer  %s %s", cancelArg, nodeArg);
      runCommand(cmdLine);
    });
  }

  /*
   Makes an invalid query attempt to non-existent Datanode.
   */
  @Test
  @Timeout(value = 60)
  public void testQueryCommand() throws Exception {
    final String queryArg = String.format("-%s %s", QUERY,
        cluster.getDataNodes().get(0).getDatanodeUuid());

    assertThrows(java.net.UnknownHostException.class, () -> {
      final String cmdLine = String
          .format(
              "hdfs diskbalancer %s", queryArg);
      runCommand(cmdLine);
    });
  }

  @Test
  @Timeout(value = 60)
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
    String parent = GenericTestUtils.getRandomizedTempPath();

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
      assertEquals(2, outputs.size(), "There must be two lines: the 1st is writing plan to,"
          + " the 2nd is actual full path of plan file.");
      assertThat(outputs.get(0)).contains("Writing plan to");
      assertThat(outputs.get(1)).contains(planFileFullName);
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
   * Making sure that we can query the multiple nodes without having done a submit.
   * @throws Exception
   */
  @Test
  public void testDiskBalancerQueryWithoutSubmitAndMultipleNodes() throws Exception {
    Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int numDatanodes = 2;
    File basedir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster miniDFSCluster = new MiniDFSCluster.Builder(hdfsConf, basedir)
        .numDataNodes(numDatanodes).build();
    try {
      miniDFSCluster.waitActive();
      DataNode dataNode1 = miniDFSCluster.getDataNodes().get(0);
      DataNode dataNode2 = miniDFSCluster.getDataNodes().get(1);
      final String queryArg = String.format("-query localhost:%d,localhost:%d", dataNode1
          .getIpcPort(), dataNode2.getIpcPort());
      final String cmdLine = String.format("hdfs diskbalancer %s", queryArg);
      List<String> outputs = runCommand(cmdLine);
      assertEquals(12, outputs.size());
      assertTrue(
          outputs.get(1).contains("localhost:" + dataNode1.getIpcPort())
              || outputs.get(6).contains("localhost:" + dataNode1.getIpcPort()),
          "Expected outputs: " + outputs);
      assertTrue(
          outputs.get(1).contains("localhost:" + dataNode2.getIpcPort())
              || outputs.get(6).contains("localhost:" + dataNode2.getIpcPort()),
          "Expected outputs: " + outputs);
    } finally {
      miniDFSCluster.shutdown();
    }
  }

  /**
   * Making sure that we can query the node without having done a submit.
   * @throws Exception
   */
  @Test
  public void testDiskBalancerQueryWithoutSubmit() throws Exception {
    Configuration hdfsConf = new HdfsConfiguration();
    hdfsConf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
    final int numDatanodes = 2;
    File basedir = new File(GenericTestUtils.getRandomizedTempPath());
    MiniDFSCluster miniDFSCluster = new MiniDFSCluster.Builder(hdfsConf, basedir)
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

  @Test
  @Timeout(value = 60)
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

  @Test
  @Timeout(value = 60)
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
    assertThat(outputs.get(0)).contains("Processing report command");
    if (inputNodesStr) {
      assertThat(outputs.get(1)).contains("Reporting volume information for DataNode")
          .contains(dataNodeUuid1, dataNodeUuid2);
    }

    // Since the order of input nodes will be disrupted when parse
    // the node string, we should compare UUID with both output lines.
    assertTrue(outputs.get(2).contains(dataNodeUuid1)
        || outputs.get(6).contains(dataNodeUuid1));
    assertTrue(outputs.get(2).contains(dataNodeUuid2)
        || outputs.get(6).contains(dataNodeUuid2));
  }

  @Test
  @Timeout(value = 60)
  public void testReportCommandWithInvalidNode() throws Exception {
    String dataNodeUuid1 = cluster.getDataNodes().get(0).getDatanodeUuid();
    String invalidNode = "invalidNode";
    final String planArg = String.format("-%s -%s %s,%s",
        REPORT, NODE, dataNodeUuid1, invalidNode);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    assertThat(
        outputs.get(0)).contains("Processing report command");
    assertThat(
        outputs.get(1)).contains("Reporting volume information for DataNode",
        dataNodeUuid1, invalidNode);

    String invalidNodeInfo =
        String.format("The node(s) '%s' not found. "
            + "Please make sure that '%s' exists in the cluster.", invalidNode, invalidNode);
    assertTrue(outputs.get(2).contains(invalidNodeInfo));
  }

  @Test
  @Timeout(value = 60)
  public void testReportCommandWithNullNodes() throws Exception {
    // don't input nodes
    final String planArg = String.format("-%s -%s ,", REPORT, NODE);
    final String cmdLine = String.format("hdfs diskbalancer %s", planArg);
    List<String> outputs = runCommand(cmdLine, cluster);

    String invalidNodeInfo = "The number of input nodes is 0. "
        + "Please input the valid nodes.";
    assertTrue(outputs.get(2).contains(invalidNodeInfo));
  }

  @Test
  @Timeout(value = 60)
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

  @Test
  @Timeout(value = 60)
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
