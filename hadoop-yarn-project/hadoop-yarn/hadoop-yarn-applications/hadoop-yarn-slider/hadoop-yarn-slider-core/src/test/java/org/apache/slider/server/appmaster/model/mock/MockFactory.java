/*
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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.Resource;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.slider.api.ResourceKeys.COMPONENT_PLACEMENT_POLICY;

/**
 * Factory for creating things.
 */
public class MockFactory implements MockRoles {

  public static final int NODE_FAILURE_THRESHOLD = 2;

  public static final MockFactory INSTANCE = new MockFactory();

  /**
   * Basic role.
   */
  public static final ProviderRole PROVIDER_ROLE0 = new ProviderRole(
      MockRoles.ROLE0,
      0,
      PlacementPolicy.DEFAULT,
      NODE_FAILURE_THRESHOLD,
      1,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION);
  /**
   * role 1 is strict. timeout should be irrelevant; same as failures
   */
  public static final ProviderRole PROVIDER_ROLE1 = new ProviderRole(
      MockRoles.ROLE1,
      1,
      PlacementPolicy.STRICT,
      NODE_FAILURE_THRESHOLD,
      1,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION);

  /**
   * role 2: longer delay.
   */
  public static final ProviderRole PROVIDER_ROLE2 = new ProviderRole(
      MockRoles.ROLE2,
      2,
      PlacementPolicy.ANYWHERE,
      NODE_FAILURE_THRESHOLD,
      2,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION);

  /**
   * Patch up a "role2" role to have anti-affinity set.
   */
  public static final ProviderRole AAROLE_2 = new ProviderRole(
      MockRoles.ROLE2,
      2,
      PlacementPolicy.ANTI_AFFINITY_REQUIRED,
      NODE_FAILURE_THRESHOLD,
      2,
      null);

  /**
   * Patch up a "role1" role to have anti-affinity set and GPI as the label.
   */
  public static final ProviderRole AAROLE_1_GPU = new ProviderRole(
      MockRoles.ROLE1,
      1,
      PlacementPolicy.ANTI_AFFINITY_REQUIRED,
      NODE_FAILURE_THRESHOLD,
      1,
      MockRoles.LABEL_GPU);

  private int appIdCount;
  private int attemptIdCount;
  private int containerIdCount;

  private ApplicationId appId = newAppId();
  private ApplicationAttemptId attemptId = newApplicationAttemptId(appId);

  /**
   * List of roles.
   */
  public static final List<ProviderRole> ROLES = Arrays.asList(
          PROVIDER_ROLE0,
          PROVIDER_ROLE1,
          PROVIDER_ROLE2
      );

  public static final int ROLE_COUNT = ROLES.size();

  MockContainerId newContainerId() {
    return newContainerId(attemptId);
  }

  MockContainerId newContainerId(ApplicationAttemptId attemptId0) {
    MockContainerId cid = new MockContainerId(attemptId0, containerIdCount++);
    return cid;
  }

  MockApplicationAttemptId newApplicationAttemptId(ApplicationId appId0) {
    MockApplicationAttemptId id = new MockApplicationAttemptId(appId0,
        attemptIdCount++);
    return id;
  }

  MockApplicationId newAppId() {
    MockApplicationId id = new MockApplicationId();
    id.setId(appIdCount++);
    return id;
  }

  public MockNodeId newNodeId(String host) {
    return new MockNodeId(host);
  }

  MockContainer newContainer(ContainerId cid) {
    MockContainer c = new MockContainer();
    c.setId(cid);
    return c;
  }

  public MockContainer newContainer() {
    return newContainer(newContainerId());
  }

  public MockContainer newContainer(NodeId nodeId, Priority priority) {
    MockContainer container = newContainer(newContainerId());
    container.setNodeId(nodeId);
    container.setPriority(priority);
    return container;
  }

  /**
   * Build a new container  using the request to supply priority and resource.
   * @param req request
   * @param host hostname to assign to
   * @return the container
   */
  public MockContainer newContainer(AMRMClient.ContainerRequest req, String
      host) {
    MockContainer container = newContainer(newContainerId());
    container.setResource(req.getCapability());
    container.setPriority(req.getPriority());
    container.setNodeId(new MockNodeId(host));
    return container;
  }

  /**
   * Create a new instance with the given components definined in the
   * resources section.
   * @param r1
   * @param r2
   * @param r3
   * @return
   */
  public Application newApplication(long r1, long r2, long r3) {
    Application application = new Application();
    application.setLaunchCommand("sleep 60");
    application.setResource(new Resource().memory("256"));
    application.getConfiguration().setProperty(ResourceKeys
        .NODE_FAILURE_THRESHOLD, Integer.toString(NODE_FAILURE_THRESHOLD));
    List<Component> components = application.getComponents();
    Component c1 = new Component().name(ROLE0).numberOfContainers(r1);
    c1.getConfiguration().setProperty(COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.DEFAULT));
    Component c2 = new Component().name(ROLE1).numberOfContainers(r2);
    c2.getConfiguration().setProperty(COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.STRICT));
    Component c3 = new Component().name(ROLE2).numberOfContainers(r3);
    c3.getConfiguration().setProperty(COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.ANYWHERE));
    components.add(c1);
    components.add(c2);
    components.add(c3);
    return application;
  }

  public MockResource newResource(int memory, int vcores) {
    return new MockResource(memory, vcores);
  }

  ContainerStatus newContainerStatus() {
    return newContainerStatus(null, null, "", 0);
  }

  ContainerStatus newContainerStatus(ContainerId containerId,
      ContainerState containerState, String diagnostics, int exitStatus) {
    return ContainerStatus.newInstance(containerId, containerState,
        diagnostics, exitStatus);
  }

  /**
   * Create a single instance.
   * @param hostname
   * @param nodeState
   * @param label
   */
  public NodeReport newNodeReport(String hostname, NodeState nodeState,
      String label) {
    NodeId nodeId = NodeId.newInstance(hostname, 80);
    Integer.valueOf(hostname, 16);
    return newNodeReport(hostname, nodeId, nodeState, label);
  }

  NodeReport newNodeReport(
      String hostname,
      NodeId nodeId,
      NodeState nodeState,
      String label) {
    NodeReport report = new NodeReportPBImpl();
    HashSet<String> nodeLabels = new HashSet<>();
    nodeLabels.add(label);
    report.setNodeId(nodeId);
    report.setNodeLabels(nodeLabels);
    report.setNodeState(nodeState);
    report.setHttpAddress("http$hostname:80");
    return report;
  }

  /**
   * Create a list of instances -one for each hostname.
   * @param hostnames hosts
   * @return
   */
  public List<NodeReport> createNodeReports(
      List<String> hostnames, NodeState nodeState, String label) {
    if (nodeState == null) {
      nodeState = NodeState.RUNNING;
    }
    List<NodeReport> reports = new ArrayList<>();
    for (String name : hostnames) {
      reports.add(newNodeReport(name, nodeState, label));
    }
    return reports;
  }

}
