/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.diskbalancer.command;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests various CLI commands of DiskBalancer.
 */
public class TestDiskBalancerCommand {
  private MiniDFSCluster cluster;
  private URI clusterJson;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
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
      cluster.getDataNodes().get(0).shutdown();
      cluster.shutdown();
    }
  }

  /* test basic report */
  @Test(timeout=60000)
  public void testReportSimple() throws Exception {
    final String cmdLine = String.format("hdfs diskbalancer -fs %s -report",
        clusterJson.toString());
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

  /* test less than 64 DataNode(s) as total, e.g., -report -top 32 */
  @Test(timeout=60000)
  public void testReportLessThanTotal() throws Exception {
    final String cmdLine = String.format(
        "hdfs diskbalancer -fs %s -report -top 32", clusterJson.toString());
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

  /* test more than 64 DataNode(s) as total, e.g., -report -top 128 */
  @Test(timeout=60000)
  public void testReportMoreThanTotal() throws Exception {
    final String cmdLine = String.format(
        "hdfs diskbalancer -fs %s -report -top 128", clusterJson.toString());
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
  @Test(timeout=60000)
  public void testReportInvalidTopLimit() throws Exception {
    final String cmdLine = String.format(
        "hdfs diskbalancer -fs %s -report -top xx", clusterJson.toString());
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

  /* test -report -node DataNodeID */
  @Test(timeout=60000)
  public void testReportNode() throws Exception {
    final String cmdLine = String
        .format(
            "hdfs diskbalancer -fs %s -report -node "
                + "a87654a9-54c7-4693-8dd9-c9c7021dc340",
            clusterJson.toString());
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

  @Test(timeout=60000)
  public void testReadClusterFromJson() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);

    ClusterConnector jsonConnector = ConnectorFactory.getCluster(clusterJson,
        conf);
    DiskBalancerCluster diskBalancerCluster = new DiskBalancerCluster(
        jsonConnector);
    diskBalancerCluster.readClusterInfo();
    assertEquals(64, diskBalancerCluster.getNodes().size());
  }

  private List<String> runCommand(final String cmdLine) throws Exception {

    String[] cmds = StringUtils.split(cmdLine, ' ');
    Configuration conf = new HdfsConfiguration();
    org.apache.hadoop.hdfs.tools.DiskBalancer db =
        new org.apache.hadoop.hdfs.tools.DiskBalancer(conf);

    ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bufOut);
    db.run(cmds, out);

    Scanner scanner = new Scanner(bufOut.toString());
    List<String> outputs = Lists.newArrayList();
    while (scanner.hasNextLine()) {
      outputs.add(scanner.nextLine());
    }
    return outputs;
  }
}
