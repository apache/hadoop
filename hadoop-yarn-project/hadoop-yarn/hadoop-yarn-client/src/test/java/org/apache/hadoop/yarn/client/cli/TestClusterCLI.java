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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.HashSet;

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
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(
        ImmutableSet.of("label1", "label2"));
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    
    int rc =
        cli.run(new String[] { ClusterCLI.CMD, "-" + ClusterCLI.LIST_LABELS_CMD });
    assertEquals(0, rc);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.print("Node Labels: label1,label2");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  
  @Test
  public void testGetClusterNodeLabelsWithLocalAccess() throws Exception {
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(
        ImmutableSet.of("remote1", "remote2"));
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    ClusterCLI.localNodeLabelsManager = mock(CommonNodeLabelsManager.class);
    when(ClusterCLI.localNodeLabelsManager.getClusterNodeLabels())
        .thenReturn(ImmutableSet.of("local1", "local2"));

    int rc =
        cli.run(new String[] { ClusterCLI.CMD,
            "-" + ClusterCLI.LIST_LABELS_CMD,
            "-" + ClusterCLI.DIRECTLY_ACCESS_NODE_LABEL_STORE });
    assertEquals(0, rc);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    // it should return local* instead of remote*
    pw.print("Node Labels: local1,local2");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  
  @Test
  public void testGetEmptyClusterNodeLabels() throws Exception {
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(new HashSet<String>());
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);

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
    ClusterCLI cli = new ClusterCLI();
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);

    int rc =
        cli.run(new String[] { "cluster", "--help" });
    assertEquals(0, rc);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: yarn cluster");
    pw.println(" -dnl,--directly-access-node-label-store   Directly access node label");
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
    pw.println(" -lnl,--list-node-labels                   List cluster node-label");
    pw.println("                                           collection");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
}