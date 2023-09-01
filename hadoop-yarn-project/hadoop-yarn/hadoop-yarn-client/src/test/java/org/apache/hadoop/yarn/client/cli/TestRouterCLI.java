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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRouterCLI {

  private ResourceManagerAdministrationProtocol admin;
  private RouterCLI rmAdminCLI;
  private final static int SUBCLUSTER_NUM = 4;

  @Before
  public void setup() throws Exception {

    admin = mock(ResourceManagerAdministrationProtocol.class);

    when(admin.deregisterSubCluster(any(DeregisterSubClusterRequest.class)))
        .thenAnswer((Answer<DeregisterSubClusterResponse>) invocationOnMock -> {
          // Step1. parse subClusterId.
          Object obj = invocationOnMock.getArgument(0);
          DeregisterSubClusterRequest request = (DeregisterSubClusterRequest) obj;
          String subClusterId = request.getSubClusterId();

          if (StringUtils.isNotBlank(subClusterId)) {
            return generateSubClusterDataBySCId(subClusterId);
          } else {
            return generateAllSubClusterData();
          }
        });

    when(admin.saveFederationQueuePolicy(any(SaveFederationQueuePolicyRequest.class)))
        .thenAnswer((Answer<SaveFederationQueuePolicyResponse>) invocationOnMock -> {
          // Step1. parse subClusterId.
          Object obj = invocationOnMock.getArgument(0);
          SaveFederationQueuePolicyRequest request = (SaveFederationQueuePolicyRequest) obj;
          return SaveFederationQueuePolicyResponse.newInstance("success");
        });

    Configuration config = new Configuration();
    config.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);

    rmAdminCLI = new RouterCLI(config) {
      @Override
      protected ResourceManagerAdministrationProtocol createAdminProtocol() {
        return admin;
      }
    };
  }

  private DeregisterSubClusterResponse generateSubClusterDataBySCId(String subClusterId) {
    // Step2. generate return data.
    String lastHeartBeatTime = new Date().toString();
    DeregisterSubClusters deregisterSubClusters =
        DeregisterSubClusters.newInstance(subClusterId, "SUCCESS", lastHeartBeatTime,
        "Heartbeat Time > 30 minutes", "SC_LOST");
    List<DeregisterSubClusters> deregisterSubClusterList = new ArrayList<>();
    deregisterSubClusterList.add(deregisterSubClusters);

    // Step3. return data.
    return DeregisterSubClusterResponse.newInstance(deregisterSubClusterList);
  }

  private DeregisterSubClusterResponse generateAllSubClusterData() {
    List<DeregisterSubClusters> deregisterSubClusterList = new ArrayList<>();
    for (int i = 1; i <= SUBCLUSTER_NUM; i++) {
      String subClusterId = "SC-" + i;
      String lastHeartBeatTime = new Date().toString();
      DeregisterSubClusters deregisterSubClusters =
          DeregisterSubClusters.newInstance(subClusterId, "SUCCESS", lastHeartBeatTime,
          "Heartbeat Time > 30 minutes", "SC_LOST");
      deregisterSubClusterList.add(deregisterSubClusters);
    }

    return DeregisterSubClusterResponse.newInstance(deregisterSubClusterList);
  }

  @Test
  public void testHelp() throws Exception {
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    System.setErr(new PrintStream(dataErr));

    String[] args = {"-help"};
    rmAdminCLI.run(args);
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-help", "-deregisterSubCluster"};
    rmAdminCLI.run(args);

    args = new String[]{"-help", "-policy"};
    rmAdminCLI.run(args);
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    oldOutPrintStream.println(dataOut);
    String[] args = {"-deregisterSubCluster", "-sc", "SC-1"};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-deregisterSubCluster", "--subClusterId", "SC-1"};
    assertEquals(0, rmAdminCLI.run(args));
  }

  @Test
  public void testDeregisterSubClusters() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    oldOutPrintStream.println(dataOut);

    String[] args = {"-deregisterSubCluster"};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-deregisterSubCluster", "-sc"};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-deregisterSubCluster", "--sc", ""};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-deregisterSubCluster", "--subClusterId"};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-deregisterSubCluster", "--subClusterId", ""};
    assertEquals(0, rmAdminCLI.run(args));

  }

  @Test
  public void testParsePolicy() throws Exception {
    // Case1, If policy is empty.
    String errMsg1 = "The policy cannot be empty or the policy is incorrect. \n" +
        " Required information to provide: queue,router weight,amrm weight,headroomalpha \n" +
        " eg. root.a;SC-1:0.7,SC-2:0.3;SC-1:0.7,SC-2:0.3;1.0";
    LambdaTestUtils.intercept(YarnException.class, errMsg1, () ->  rmAdminCLI.parsePolicy(""));

    // Case2, If policy is incomplete, We need 4 items, but only 2 of them are provided.
    LambdaTestUtils.intercept(YarnException.class, errMsg1,
        () ->  rmAdminCLI.parsePolicy("root.a;SC-1:0.1,SC-2:0.9;"));

    // Case3, If policy is incomplete, The weight of a subcluster is missing.
    String errMsg2 = "The subClusterWeight cannot be empty, " +
        "and the subClusterWeight size must be 2. (eg.SC-1,0.2)";
    LambdaTestUtils.intercept(YarnException.class, errMsg2,
        () ->  rmAdminCLI.parsePolicy("root.a;SC-1:0.1,SC-2;SC-1:0.1,SC-2;0.3,1.0"));

    // Case4, The policy is complete, but the sum of weights for each subcluster is not equal to 1.
    String errMsg3 = "The sum of ratios for all subClusters must be equal to 1.";
    LambdaTestUtils.intercept(YarnException.class, errMsg3,
        () ->  rmAdminCLI.parsePolicy("root.a;SC-1:0.1,SC-2:0.8;SC-1:0.1,SC-2;0.3,1.0"));

    // If policy is root.a;SC-1:0.7,SC-2:0.3;SC-1:0.7,SC-2:0.3;1.0
    String policy = "root.a;SC-1:0.7,SC-2:0.3;SC-1:0.6,SC-2:0.4;1.0";
    SaveFederationQueuePolicyRequest request = rmAdminCLI.parsePolicy(policy);
    FederationQueueWeight federationQueueWeight = request.getFederationQueueWeight();
    assertNotNull(federationQueueWeight);
    assertEquals("SC-1:0.7,SC-2:0.3", federationQueueWeight.getRouterWeight());
    assertEquals("SC-1:0.6,SC-2:0.4", federationQueueWeight.getAmrmWeight());
    assertEquals("1.0", federationQueueWeight.getHeadRoomAlpha());
  }

  @Test
  public void testSavePolicy() throws Exception {
    PrintStream oldOutPrintStream = System.out;
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(dataOut));
    oldOutPrintStream.println(dataOut);

    String[] args = {"-policy", "-s", "root.a;SC-1:0.1,SC-2:0.9;SC-1:0.7,SC-2:0.3;1.0"};
    assertEquals(0, rmAdminCLI.run(args));

    args = new String[]{"-policy", "-save", "root.a;SC-1:0.1,SC-2:0.9;SC-1:0.7,SC-2:0.3;1.0"};
    assertEquals(0, rmAdminCLI.run(args));
  }

  @Test
  public void testParsePoliciesByXml() throws Exception {
    String filePath =
        TestRouterCLI.class.getClassLoader().getResource("federation-weights.xml").getFile();
    List<FederationQueueWeight> federationQueueWeights = rmAdminCLI.parsePoliciesByXml(filePath);
    assertNotNull(federationQueueWeights);
    assertEquals(2, federationQueueWeights.size());

    // Queue1: root.a
    FederationQueueWeight queueWeight1 = federationQueueWeights.get(0);
    assertNotNull(queueWeight1);
    assertEquals("root.a", queueWeight1.getQueue());
    assertEquals("SC-1:0.7,SC-2:0.3", queueWeight1.getAmrmWeight());
    assertEquals("SC-1:0.6,SC-2:0.4", queueWeight1.getRouterWeight());

    // Queue2: root.b
    FederationQueueWeight queueWeight2 = federationQueueWeights.get(1);
    assertNotNull(queueWeight2);
    assertEquals("root.b", queueWeight2.getQueue());
    assertEquals("SC-1:0.8,SC-2:0.2", queueWeight2.getAmrmWeight());
    assertEquals("SC-1:0.6,SC-2:0.4", queueWeight2.getRouterWeight());
  }
}
