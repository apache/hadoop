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

package org.apache.hadoop.yarn.client.cli;

import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestClusterCLI {

  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;
  private YarnClient client = mock(YarnClient.class);

  @Before
  public void setup() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut = spy(new PrintStream(sysOutStream));
    sysErrStream = new ByteArrayOutputStream();
    sysErr = spy(new PrintStream(sysErrStream));
    System.setOut(sysOut);
  }
  
  @Test
  public void testGetClusterNodeLabels() throws Exception {
    when(client.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("label1"),
            NodeLabel.newInstance("label2")));
    ClusterCLI cli = createAndGetClusterCLI();
    
    int rc =
        cli.run(new String[] { ClusterCLI.CMD, "-" + ClusterCLI.LIST_LABELS_CMD });
    assertEquals(0, rc);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.print("Node Labels: <label1:exclusivity=true>,<label2:exclusivity=true>");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }

  @Test
  public void testGetClusterNodeAttributes() throws Exception {
    when(client.getClusterAttributes()).thenReturn(ImmutableSet
        .of(NodeAttributeInfo.newInstance(NodeAttributeKey.newInstance("GPU"),
            NodeAttributeType.STRING), NodeAttributeInfo
            .newInstance(NodeAttributeKey.newInstance("CPU"),
                NodeAttributeType.STRING)));
    ClusterCLI cli = createAndGetClusterCLI();

    int rc = cli.run(new String[] {ClusterCLI.CMD,
        "-" + ClusterCLI.LIST_CLUSTER_ATTRIBUTES});
    assertEquals(0, rc);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("rm.yarn.io/GPU(STRING)");
    pw.println("rm.yarn.io/CPU(STRING)");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }

  @Test
  public void testGetClusterNodeLabelsWithLocalAccess() throws Exception {
    when(client.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("remote1"),
            NodeLabel.newInstance("remote2")));
    ClusterCLI cli = createAndGetClusterCLI();
    ClusterCLI.localNodeLabelsManager = mock(CommonNodeLabelsManager.class);
    when(ClusterCLI.localNodeLabelsManager.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("local1"),
            NodeLabel.newInstance("local2")));

    int rc =
        cli.run(new String[] { ClusterCLI.CMD,
            "-" + ClusterCLI.LIST_LABELS_CMD,
            "-" + ClusterCLI.DIRECTLY_ACCESS_NODE_LABEL_STORE });
    assertEquals(0, rc);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    // it should return local* instead of remote*
    pw.print("Node Labels: <local1:exclusivity=true>,<local2:exclusivity=true>");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  
  @Test
  public void testGetEmptyClusterNodeLabels() throws Exception {
    when(client.getClusterNodeLabels()).thenReturn(new ArrayList<NodeLabel>());
    ClusterCLI cli = createAndGetClusterCLI();

    int rc =
        cli.run(new String[] { ClusterCLI.CMD, "-" + ClusterCLI.LIST_LABELS_CMD });
    assertEquals(0, rc);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.print("Node Labels: ");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  
  @Test
  public void testHelp() throws Exception {
    ClusterCLI cli = createAndGetClusterCLI();

    int rc =
        cli.run(new String[] { "cluster", "--help" });
    assertEquals(0, rc);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: yarn cluster");
    pw.println(" -dnl,--directly-access-node-label-store   This is DEPRECATED, will be");
    pw.println("                                           removed in future releases.");
    pw.println("                                           Directly access node label");
    pw.println("                                           store, with this option, all");
    pw.println("                                           node label related operations");
    pw.println("                                           will NOT connect RM. Instead,");
    pw.println("                                           they will access/modify stored");
    pw.println("                                           node labels directly. By");
    pw.println("                                           default, it is false (access");
    pw.println("                                           via RM). AND PLEASE NOTE: if");
    pw.println("                                           you configured");
    pw.println("                                           yarn.node-labels.fs-store.root-");
    pw.println("                                           dir to a local directory");
    pw.println("                                           (instead of NFS or HDFS), this");
    pw.println("                                           option will only work when the");
    pw.println("                                           command run on the machine");
    pw.println("                                           where RM is running. Also, this");
    pw.println("                                           option is UNSTABLE, could be");
    pw.println("                                           removed in future releases.");
    pw.println(" -h,--help                                 Displays help for all commands.");
    pw.println(" -lna,--list-node-attributes               List cluster node-attribute");
    pw.println("                                           collection");
    pw.println(" -lnl,--list-node-labels                   List cluster node-label");
    pw.println("                                           collection");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }

  private ClusterCLI createAndGetClusterCLI() {
    ClusterCLI cli = new ClusterCLI() {
      @Override protected void createAndStartYarnClient() {
      }
    };
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    return cli;
  }
}