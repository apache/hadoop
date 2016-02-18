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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.DummyCommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;

public class TestRMAdminCLI {

  private ResourceManagerAdministrationProtocol admin;
  private HAServiceProtocol haadmin;
  private RMAdminCLI rmAdminCLI;
  private RMAdminCLI rmAdminCLIWithHAEnabled;
  private CommonNodeLabelsManager dummyNodeLabelsManager;
  private boolean remoteAdminServiceAccessed = false;

  @SuppressWarnings("static-access")
  @Before
  public void configure() throws IOException, YarnException {
    remoteAdminServiceAccessed = false;
    admin = mock(ResourceManagerAdministrationProtocol.class);
    when(admin.addToClusterNodeLabels(any(AddToClusterNodeLabelsRequest.class)))
        .thenAnswer(new Answer<AddToClusterNodeLabelsResponse>() {

          @Override
          public AddToClusterNodeLabelsResponse answer(
              InvocationOnMock invocation) throws Throwable {
            remoteAdminServiceAccessed = true;
            return AddToClusterNodeLabelsResponse.newInstance();
          }
        });

    haadmin = mock(HAServiceProtocol.class);
    when(haadmin.getServiceStatus()).thenReturn(new HAServiceStatus(
        HAServiceProtocol.HAServiceState.INITIALIZING));

    final HAServiceTarget haServiceTarget = mock(HAServiceTarget.class);
    when(haServiceTarget.getProxy(any(Configuration.class), anyInt()))
        .thenReturn(haadmin);
    rmAdminCLI = new RMAdminCLI(new Configuration()) {
      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol()
          throws IOException {
        return admin;
      }

      @Override
      protected HAServiceTarget resolveTarget(String rmId) {
        return haServiceTarget;
      }
    };
    initDummyNodeLabelsManager();
    rmAdminCLI.localNodeLabelsManager = dummyNodeLabelsManager;

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    rmAdminCLIWithHAEnabled = new RMAdminCLI(conf) {

      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol()
          throws IOException {
        return admin;
      }

      @Override
      protected HAServiceTarget resolveTarget(String rmId) {
        return haServiceTarget;
      }
    };
  }
  
  private void initDummyNodeLabelsManager() {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    dummyNodeLabelsManager = new DummyCommonNodeLabelsManager() {
      @Override
      public void replaceLabelsOnNode(
          Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
        Iterator<NodeId> iterator = replaceLabelsToNode.keySet().iterator();
        while(iterator.hasNext()) {
          NodeId nodeId=iterator.next();
          if(nodeId.getHost().endsWith("=")){
            throw new IOException("Parsing of Input String failed");
          }
        }
        super.replaceLabelsOnNode(replaceLabelsToNode);
      }
    };
    dummyNodeLabelsManager.init(conf);
  }
  
  @Test(timeout=500)
  public void testRefreshQueues() throws Exception {
    String[] args = { "-refreshQueues" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshQueues(any(RefreshQueuesRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshUserToGroupsMappings() throws Exception {
    String[] args = { "-refreshUserToGroupsMappings" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshUserToGroupsMappings(
        any(RefreshUserToGroupsMappingsRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    String[] args = { "-refreshSuperUserGroupsConfiguration" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshSuperUserGroupsConfiguration(
        any(RefreshSuperUserGroupsConfigurationRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshAdminAcls() throws Exception {
    String[] args = { "-refreshAdminAcls" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshAdminAcls(any(RefreshAdminAclsRequest.class));
  }

  @Test(timeout = 5000)
  public void testRefreshClusterMaxPriority() throws Exception {
    String[] args = { "-refreshClusterMaxPriority" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshClusterMaxPriority(
        any(RefreshClusterMaxPriorityRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshServiceAcl() throws Exception {
    String[] args = { "-refreshServiceAcl" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshServiceAcls(any(RefreshServiceAclsRequest.class));
  }

  @Test(timeout=500)
  public void testUpdateNodeResource() throws Exception {
    String nodeIdStr = "0.0.0.0:0";
    int memSize = 2048;
    int cores = 2;
    String[] args = { "-updateNodeResource", nodeIdStr,
        Integer.toString(memSize), Integer.toString(cores) };
    assertEquals(0, rmAdminCLI.run(args));
    ArgumentCaptor<UpdateNodeResourceRequest> argument =
        ArgumentCaptor.forClass(UpdateNodeResourceRequest.class);
    verify(admin).updateNodeResource(argument.capture());
    UpdateNodeResourceRequest request = argument.getValue();
    Map<NodeId, ResourceOption> resourceMap = request.getNodeResourceMap();
    NodeId nodeId = ConverterUtils.toNodeId(nodeIdStr);
    Resource expectedResource = Resources.createResource(memSize, cores);
    ResourceOption resource = resourceMap.get(nodeId);
    assertNotNull("resource for " + nodeIdStr + " shouldn't be null.",
        resource);
    assertEquals("resource value for " + nodeIdStr + " is not as expected.",
        ResourceOption.newInstance(expectedResource,
            ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT),
        resource);
  }

  @Test(timeout=500)
  public void testUpdateNodeResourceWithInvalidValue() throws Exception {
    String nodeIdStr = "0.0.0.0:0";
    int memSize = -2048;
    int cores = 2;
    String[] args = { "-updateNodeResource", nodeIdStr,
        Integer.toString(memSize), Integer.toString(cores) };
    // execution of command line is expected to be failed
    assertEquals(-1, rmAdminCLI.run(args));
    // verify admin protocol never calls. 
    verify(admin,times(0)).updateNodeResource(
        any(UpdateNodeResourceRequest.class));
  }

  @Test(timeout=500)
  public void testRefreshNodes() throws Exception {
    String[] args = { "-refreshNodes" };
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshNodes(any(RefreshNodesRequest.class));
  }

  @Test
  public void testRefreshNodesWithGracefulTimeout() throws Exception {
    // graceful decommission before timeout
    String[] args = { "-refreshNodes", "-g", "1" };
    CheckForDecommissioningNodesResponse response = Records
        .newRecord(CheckForDecommissioningNodesResponse.class);
    HashSet<NodeId> decomNodes = new HashSet<NodeId>();
    response.setDecommissioningNodes(decomNodes);
    when(admin.checkForDecommissioningNodes(any(
        CheckForDecommissioningNodesRequest.class))).thenReturn(response);
    assertEquals(0, rmAdminCLI.run(args));
    verify(admin).refreshNodes(
        RefreshNodesRequest.newInstance(DecommissionType.GRACEFUL));

    // Forceful decommission when timeout occurs
    String[] focefulDecomArgs = { "-refreshNodes", "-g", "1" };
    decomNodes = new HashSet<NodeId>();
    response.setDecommissioningNodes(decomNodes);
    decomNodes.add(NodeId.newInstance("node1", 100));
    response.setDecommissioningNodes(decomNodes);
    when(admin.checkForDecommissioningNodes(any(
        CheckForDecommissioningNodesRequest.class))).thenReturn(response);
    assertEquals(0, rmAdminCLI.run(focefulDecomArgs));
    verify(admin).refreshNodes(
        RefreshNodesRequest.newInstance(DecommissionType.FORCEFUL));

    // invalid graceful timeout parameter
    String[] invalidArgs = { "-refreshNodes", "-ginvalid", "invalid" };
    assertEquals(-1, rmAdminCLI.run(invalidArgs));

    // invalid timeout
    String[] invalidTimeoutArgs = { "-refreshNodes", "-g", "invalid" };
    assertEquals(-1, rmAdminCLI.run(invalidTimeoutArgs));

    // negative timeout
    String[] negativeTimeoutArgs = { "-refreshNodes", "-g", "-1000" };
    assertEquals(-1, rmAdminCLI.run(negativeTimeoutArgs));
  }

  @Test(timeout=500)
  public void testGetGroups() throws Exception {
    when(admin.getGroupsForUser(eq("admin"))).thenReturn(
        new String[] {"group1", "group2"});
    PrintStream origOut = System.out;
    PrintStream out = mock(PrintStream.class);
    System.setOut(out);
    try {
      String[] args = { "-getGroups", "admin" };
      assertEquals(0, rmAdminCLI.run(args));
      verify(admin).getGroupsForUser(eq("admin"));
      verify(out).println(argThat(new ArgumentMatcher<StringBuilder>() {
        @Override
        public boolean matches(Object argument) {
          return ("" + argument).equals("admin : group1 group2");
        }
      }));
    } finally {
      System.setOut(origOut);
    }
  }

  @Test(timeout = 500)
  public void testTransitionToActive() throws Exception {
    String[] args = {"-transitionToActive", "rm1"};

    // RM HA is disabled.
    // transitionToActive should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).transitionToActive(
        any(HAServiceProtocol.StateChangeRequestInfo.class));

    // Now RM HA is enabled.
    // transitionToActive should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).transitionToActive(
        any(HAServiceProtocol.StateChangeRequestInfo.class));
    // HAAdmin#isOtherTargetNodeActive should check state of non-target node.
    verify(haadmin, times(1)).getServiceStatus();
  }

  @Test(timeout = 500)
  public void testTransitionToStandby() throws Exception {
    String[] args = {"-transitionToStandby", "rm1"};

    // RM HA is disabled.
    // transitionToStandby should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).transitionToStandby(
        any(HAServiceProtocol.StateChangeRequestInfo.class));

    // Now RM HA is enabled.
    // transitionToActive should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).transitionToStandby(
        any(HAServiceProtocol.StateChangeRequestInfo.class));
  }

  @Test(timeout = 500)
  public void testGetServiceState() throws Exception {
    String[] args = {"-getServiceState", "rm1"};

    // RM HA is disabled.
    // getServiceState should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).getServiceStatus();

    // Now RM HA is enabled.
    // getServiceState should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).getServiceStatus();
  }

  @Test(timeout = 500)
  public void testCheckHealth() throws Exception {
    String[] args = {"-checkHealth", "rm1"};

    // RM HA is disabled.
    // getServiceState should not be executed
    assertEquals(-1, rmAdminCLI.run(args));
    verify(haadmin, never()).monitorHealth();

    // Now RM HA is enabled.
    // getServiceState should be executed
    assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
    verify(haadmin).monitorHealth();
  }

  /**
   * Test printing of help messages
   */
  @Test(timeout=500)
  public void testHelp() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));
    try {
      String[] args = { "-help" };
      assertEquals(0, rmAdminCLI.run(args));
      oldOutPrintStream.println(dataOut);
      assertTrue(dataOut
          .toString()
          .contains(
              "rmadmin is the command to execute YARN administrative commands."));
      assertTrue(dataOut
          .toString()
          .contains(
              "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in " +
              "seconds]]] [-refreshNodesResources] [-refreshSuperUserGroups" +
              "Configuration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup " +
              "[username]] [-addToClusterNodeLabels " +
              "<\"label1(exclusive=true),label2(exclusive=false),label3\">] " +
              "[-removeFromClusterNodeLabels <label1,label2,label3>] " +
              "[-replaceLabelsOnNode " +
              "<\"node1[:port]=label1,label2 node2[:port]=label1\">] " +
              "[-directlyAccessNodeLabelStore] [-updateNodeResource " +
              "[NodeID] [MemSize] [vCores] ([OvercommitTimeout]) " +
              "[-help [cmd]]"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshQueues: Reload the queues' acls, states and scheduler " +
              "specific properties."));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshNodes [-g [timeout in seconds]]: Refresh the hosts information at the " +
              "ResourceManager."));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshNodesResources: Refresh resources of NodeManagers at the " +
              "ResourceManager."));
      assertTrue(dataOut.toString().contains(
          "-refreshUserToGroupsMappings: Refresh user-to-groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshSuperUserGroupsConfiguration: Refresh superuser proxy" +
              " groups mappings"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshAdminAcls: Refresh acls for administration of " +
              "ResourceManager"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-refreshServiceAcl: Reload the service-level authorization" +
              " policy file"));
      assertTrue(dataOut
          .toString()
          .contains(
              "-help [cmd]: Displays help for the given command or all " +
              "commands if none"));

      testError(new String[] { "-help", "-refreshQueues" },
          "Usage: yarn rmadmin [-refreshQueues]", dataErr, 0);
      testError(new String[] { "-help", "-refreshNodes" },
          "Usage: yarn rmadmin [-refreshNodes [-g [timeout in seconds]]]", dataErr, 0);
      testError(new String[] { "-help", "-refreshNodesResources" },
          "Usage: yarn rmadmin [-refreshNodesResources]", dataErr, 0);
      testError(new String[] { "-help", "-refreshUserToGroupsMappings" },
          "Usage: yarn rmadmin [-refreshUserToGroupsMappings]", dataErr, 0);
      testError(
          new String[] { "-help", "-refreshSuperUserGroupsConfiguration" },
          "Usage: yarn rmadmin [-refreshSuperUserGroupsConfiguration]",
          dataErr, 0);
      testError(new String[] { "-help", "-refreshAdminAcls" },
          "Usage: yarn rmadmin [-refreshAdminAcls]", dataErr, 0);
      testError(new String[] { "-help", "-refreshServiceAcl" },
          "Usage: yarn rmadmin [-refreshServiceAcl]", dataErr, 0);
      testError(new String[] { "-help", "-getGroups" },
          "Usage: yarn rmadmin [-getGroups [username]]", dataErr, 0);
      testError(new String[] { "-help", "-transitionToActive" },
          "Usage: yarn rmadmin [-transitionToActive [--forceactive]" +
          " <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-transitionToStandby" },
          "Usage: yarn rmadmin [-transitionToStandby <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-getServiceState" },
          "Usage: yarn rmadmin [-getServiceState <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-checkHealth" },
          "Usage: yarn rmadmin [-checkHealth <serviceId>]", dataErr, 0);
      testError(new String[] { "-help", "-failover" },
          "Usage: yarn rmadmin " +
              "[-failover [--forcefence] [--forceactive] " +
              "<serviceId> <serviceId>]",
          dataErr, 0);

      testError(new String[] { "-help", "-badParameter" },
          "Usage: yarn rmadmin", dataErr, 0);
      testError(new String[] { "-badParameter" },
          "badParameter: Unknown command", dataErr, -1);

      // Test -help when RM HA is enabled
      assertEquals(0, rmAdminCLIWithHAEnabled.run(args));
      oldOutPrintStream.println(dataOut);
      String expectedHelpMsg = 
          "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in seconds]]] "
              + "[-refreshNodesResources] [-refreshSuperUserGroupsConfiguration] "
              + "[-refreshUserToGroupsMappings] "
              + "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup"
              + " [username]] [-addToClusterNodeLabels <\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">]"
              + " [-removeFromClusterNodeLabels <label1,label2,label3>] [-replaceLabelsOnNode "
              + "<\"node1[:port]=label1,label2 node2[:port]=label1\">] [-directlyAccessNodeLabelStore] "
              + "[-updateNodeResource [NodeID] [MemSize] [vCores] ([OvercommitTimeout]) "
              + "[-transitionToActive [--forceactive] <serviceId>] "
              + "[-transitionToStandby <serviceId>] "
              + "[-getServiceState <serviceId>] [-checkHealth <serviceId>] [-help [cmd]]";
      String actualHelpMsg = dataOut.toString();
      assertTrue(String.format("Help messages: %n " + actualHelpMsg + " %n doesn't include expected " +
          "messages: %n" + expectedHelpMsg), actualHelpMsg.contains(expectedHelpMsg
              ));
    } finally {
      System.setOut(oldOutPrintStream);
      System.setErr(oldErrPrintStream);
    }
  }

  @Test(timeout=500)
  public void testException() throws Exception {
    PrintStream oldErrPrintStream = System.err;
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(dataErr));
    try {
      when(admin.refreshQueues(any(RefreshQueuesRequest.class)))
          .thenThrow(new IOException("test exception"));
      String[] args = { "-refreshQueues" };

      assertEquals(-1, rmAdminCLI.run(args));
      verify(admin).refreshQueues(any(RefreshQueuesRequest.class));
      assertTrue(dataErr.toString().contains("refreshQueues: test exception"));
    } finally {
      System.setErr(oldErrPrintStream);
    }
  }
  
  @Test
  public void testAccessLocalNodeLabelManager() throws Exception {
    assertFalse(dummyNodeLabelsManager.getServiceState() == STATE.STOPPED);
    
    String[] args =
        { "-addToClusterNodeLabels", "x,y", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x", "y")));
    
    // reset localNodeLabelsManager
    dummyNodeLabelsManager.removeFromClusterNodeLabels(ImmutableSet.of("x", "y"));
    
    // change the sequence of "-directlyAccessNodeLabelStore" and labels,
    // should not matter
    args =
        new String[] { "-addToClusterNodeLabels",
            "-directlyAccessNodeLabelStore", "x,y" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x", "y")));
    
    // local node labels manager will be close after running
    assertTrue(dummyNodeLabelsManager.getServiceState() == STATE.STOPPED);
  }
  
  @Test
  public void testAccessRemoteNodeLabelManager() throws Exception {
    String[] args =
        { "-addToClusterNodeLabels", "x,y" };
    assertEquals(0, rmAdminCLI.run(args));
    
    // localNodeLabelsManager shouldn't accessed
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().isEmpty());
    
    // remote node labels manager accessed
    assertTrue(remoteAdminServiceAccessed);
  }
  
  @Test
  public void testAddToClusterNodeLabels() throws Exception {
    // successfully add labels
    String[] args =
        { "-addToClusterNodeLabels", "x", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x")));
    
    // no labels, should fail
    args = new String[] { "-addToClusterNodeLabels" };
    assertTrue(0 != rmAdminCLI.run(args));
    
    // no labels, should fail
    args =
        new String[] { "-addToClusterNodeLabels",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-addToClusterNodeLabels", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-addToClusterNodeLabels", " , " };
    assertTrue(0 != rmAdminCLI.run(args));

    // successfully add labels
    args =
        new String[] { "-addToClusterNodeLabels", ",x,,",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x")));
  }
  
  @Test
  public void testAddToClusterNodeLabelsWithExclusivitySetting()
      throws Exception {
    // Parenthese not match
    String[] args = new String[] { "-addToClusterNodeLabels", "x(" };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-addToClusterNodeLabels", "x)" };
    assertTrue(0 != rmAdminCLI.run(args));

    // Not expected key=value specifying inner parentese
    args = new String[] { "-addToClusterNodeLabels", "x(key=value)" };
    assertTrue(0 != rmAdminCLI.run(args));

    // Not key is expected, but value not
    args = new String[] { "-addToClusterNodeLabels", "x(exclusive=)" };
    assertTrue(0 != rmAdminCLI.run(args));

    // key=value both set
    args =
        new String[] { "-addToClusterNodeLabels",
            "w,x(exclusive=true), y(exclusive=false),z()",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 == rmAdminCLI.run(args));

    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("w"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("x"));
    assertFalse(dummyNodeLabelsManager.isExclusiveNodeLabel("y"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("z"));

    // key=value both set, and some spaces need to be handled
    args =
        new String[] { "-addToClusterNodeLabels",
            "a (exclusive= true) , b( exclusive =false),c  ",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 == rmAdminCLI.run(args));

    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("a"));
    assertFalse(dummyNodeLabelsManager.isExclusiveNodeLabel("b"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("c"));
  }

  @Test
  public void testRemoveFromClusterNodeLabels() throws Exception {
    // Successfully remove labels
    dummyNodeLabelsManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    String[] args =
        { "-removeFromClusterNodeLabels", "x,,y",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().isEmpty());
    
    // no labels, should fail
    args = new String[] { "-removeFromClusterNodeLabels" };
    assertTrue(0 != rmAdminCLI.run(args));
    
    // no labels, should fail
    args =
        new String[] { "-removeFromClusterNodeLabels",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-removeFromClusterNodeLabels", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail at client validation
    args = new String[] { "-removeFromClusterNodeLabels", ", " };
    assertTrue(0 != rmAdminCLI.run(args));
  }
  
  @Test
  public void testReplaceLabelsOnNode() throws Exception {
    // Successfully replace labels
    dummyNodeLabelsManager
        .addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "Y"));
    String[] args =
        { "-replaceLabelsOnNode",
            "node1:8000,x node2:8000=y node3,x node4=Y",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node1", 8000)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node2", 8000)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node3", 0)));
    assertTrue(dummyNodeLabelsManager.getNodeLabels().containsKey(
        NodeId.newInstance("node4", 0)));

    // no labels, should fail
    args = new String[] { "-replaceLabelsOnNode" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail
    args =
        new String[] { "-replaceLabelsOnNode", "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));

    // no labels, should fail
    args = new String[] { "-replaceLabelsOnNode", " " };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-replaceLabelsOnNode", ", " };
    assertTrue(0 != rmAdminCLI.run(args));
  }
  
  @Test
  public void testReplaceMultipleLabelsOnSingleNode() throws Exception {
    // Successfully replace labels
    dummyNodeLabelsManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    String[] args =
        { "-replaceLabelsOnNode", "node1,x,y",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 != rmAdminCLI.run(args));
  }

  @Test
  public void testRemoveLabelsOnNodes() throws Exception {
    // Successfully replace labels
    dummyNodeLabelsManager
        .addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    String[] args = { "-replaceLabelsOnNode", "node1=x node2=y",
        "-directlyAccessNodeLabelStore" };
    assertTrue(0 == rmAdminCLI.run(args));

    args = new String[] { "-replaceLabelsOnNode", "node1= node2=",
        "-directlyAccessNodeLabelStore" };
    assertTrue("Labels should get replaced even '=' is used ",
        0 == rmAdminCLI.run(args));
  }

  private void testError(String[] args, String template,
      ByteArrayOutputStream data, int resultCode) throws Exception {
    int actualResultCode = rmAdminCLI.run(args);
    assertEquals("Expected result code: " + resultCode + 
        ", actual result code is: " + actualResultCode, resultCode, actualResultCode);
    assertTrue(String.format("Expected error message: %n" + template + 
        " is not included in messages: %n" + data.toString()), 
        data.toString().contains(template));
    data.reset();
  }

  @Test
  public void testRMHAErrorUsage() throws Exception {
    ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
    rmAdminCLIWithHAEnabled.setErrOut(new PrintStream(errOutBytes));
    try {
      String[] args = { "-failover" };
      assertEquals(-1, rmAdminCLIWithHAEnabled.run(args));
      String errOut = new String(errOutBytes.toByteArray(), Charsets.UTF_8);
      errOutBytes.reset();
      assertTrue(errOut.contains("Usage: rmadmin"));
    } finally {
      rmAdminCLIWithHAEnabled.setErrOut(System.err);
    }
  }

}
