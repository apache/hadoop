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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.ContainerOutcome;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.NodeMap;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.utils.SliderTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

/**
 * Base for app state tests.
 */
public abstract class BaseMockAppStateTest extends SliderTestBase implements
    MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseMockAppStateTest.class);
  protected static final List<ContainerId> EMPTY_ID_LIST = Collections
      .emptyList();

  protected final MockFactory factory = MockFactory.INSTANCE;
  protected MockAppState appState;
  protected MockYarnEngine engine;
  protected FileSystem fs;
  protected SliderFileSystem sliderFileSystem;
  protected File historyWorkDir;
  protected Path historyPath;
  protected MockApplicationId applicationId;
  protected MockApplicationAttemptId applicationAttemptId;
  protected StateAccessForProviders stateAccess;

  /**
   * Override point: called in setup() to create the YARN engine; can
   * be changed for different sizes and options.
   * @return
   */
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8, 8);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    YarnConfiguration conf = SliderUtils.createConfiguration();
    fs = FileSystem.get(new URI("file:///"), conf);
    sliderFileSystem = new SliderFileSystem(fs, conf);
    engine = createYarnEngine();
    initApp();
  }

  /**
   * Initialize the application.
   * This uses the binding information supplied by {@link #buildBindingInfo()}.
   */
  protected void initApp()
      throws IOException, BadConfigException, BadClusterStateException {
    String historyDirName = getTestName();
    applicationId = new MockApplicationId(1, 0);
    applicationAttemptId = new MockApplicationAttemptId(applicationId, 1);

    historyWorkDir = new File("target/history", historyDirName);
    historyPath = new Path(historyWorkDir.toURI());
    fs.delete(historyPath, true);
    appState = new MockAppState(buildBindingInfo());
    stateAccess = new ProviderAppState(getTestName(), appState);
  }

  /**
   * Build the binding info from the default constructor values,
   * the roles from {@link #factory}, and an instance definition.
   * from {@link #buildApplication()} ()}
   * @return
   */
  protected AppStateBindingInfo buildBindingInfo() {
    AppStateBindingInfo binding = new AppStateBindingInfo();
    binding.application = buildApplication();
    //binding.roles = new ArrayList<>(factory.ROLES);
    binding.fs = fs;
    binding.historyPath = historyPath;
    binding.nodeReports = engine.getNodeReports();
    return binding;
  }

  /**
   * Override point, define the instance definition.
   * @return the instance definition
   */
  public Application buildApplication() {
    return factory.newApplication(0, 0, 0).name(getTestName());
  }

  /**
   * Get the test name ... defaults to method name
   * @return the method name
   */
  public String getTestName() {
    return methodName.getMethodName();
  }

  public RoleStatus getRole0Status() {
    return lookupRole(ROLE0);
  }

  public RoleStatus lookupRole(String role) {
    return appState.lookupRoleStatus(role);
  }

  public RoleStatus getRole1Status() {
    return lookupRole(ROLE1);
  }

  public RoleStatus getRole2Status() {
    return lookupRole(ROLE2);
  }

  /**
   * Build a role instance from a container assignment.
   * @param assigned
   * @return the instance
   */
  public RoleInstance roleInstance(ContainerAssignment assigned) {
    Container target = assigned.container;
    RoleInstance failedInstance =
        assigned.role.getProviderRole().failedInstances.poll();
    RoleInstance ri;
    if (failedInstance != null) {
      ri = new RoleInstance(target, failedInstance);
    } else {
      ri = new RoleInstance(target, assigned.role.getProviderRole());
    }
    ri.roleId = assigned.role.getPriority();
    ri.role = assigned.role.getName();
    return ri;
  }

  public NodeInstance nodeInstance(long age, int live0, int live1, int live2) {
    NodeInstance ni = new NodeInstance(String.format("age%d-[%d,%d,%d]", age,
        live0, live1, live2), MockFactory.ROLE_COUNT);
    ni.getOrCreate(getRole0Status().getKey()).setLastUsed(age);
    ni.getOrCreate(getRole0Status().getKey()).setLive(live0);
    if (live1 > 0) {
      ni.getOrCreate(getRole1Status().getKey()).setLive(live1);
    }
    if (live2 > 0) {
      ni.getOrCreate(getRole2Status().getKey()).setLive(live2);
    }
    return ni;
  }

  /**
   * Create a container status event.
   * @param c container
   * @return a status
   */
  ContainerStatus containerStatus(Container c) {
    return containerStatus(c.getId());
  }

  /**
   * Create a container status instance for the given ID, declaring
   * that it was shut down by the application itself.
   * @param cid container Id
   * @return the instance
   */
  public ContainerStatus containerStatus(ContainerId cid) {
    ContainerStatus status = containerStatus(cid,
        LauncherExitCodes.EXIT_CLIENT_INITIATED_SHUTDOWN);
    return status;
  }

  public ContainerStatus containerStatus(ContainerId cid, int exitCode) {
    ContainerStatus status = ContainerStatus.newInstance(
        cid,
        ContainerState.COMPLETE,
        "",
        exitCode);
    return status;
  }

  /**
   * Create nodes and bring them to the started state.
   * @return a list of roles
   */
  protected List<RoleInstance> createAndStartNodes()
      throws TriggerClusterTeardownException, SliderInternalStateException {
    return createStartAndStopNodes(new ArrayList<>());
  }

  /**
   * Create, Start and stop nodes.
   * @param completionResults List filled in with the status on all completed
   *                          nodes
   * @return the nodes
   */
  public List<RoleInstance> createStartAndStopNodes(
      List<AppState.NodeCompletionResult> completionResults)
      throws TriggerClusterTeardownException, SliderInternalStateException {
    List<ContainerId> released = new ArrayList<>();
    List<RoleInstance> instances = createAndSubmitNodes(released);
    processSubmissionOperations(instances, completionResults, released);
    return instances;
  }

  /**
   * Process the start/stop operations.
   * @param instances
   * @param completionResults
   * @param released
   */
  public void processSubmissionOperations(
      List<RoleInstance> instances,
      List<AppState.NodeCompletionResult> completionResults,
      List<ContainerId> released) {
    for (RoleInstance instance : instances) {
      LOG.debug("Started {} on {}", instance.role, instance.id);
      assertNotNull(appState.onNodeManagerContainerStarted(instance
          .getContainerId()));
    }
    releaseContainers(completionResults,
        released,
        ContainerState.COMPLETE,
        "released",
        0
    );
  }

  /**
   * Release a list of containers, updating the completion results.
   * @param completionResults
   * @param containerIds
   * @param containerState
   * @param exitText
   * @param containerExitCode
   * @return
   */
  public void releaseContainers(
      List<AppState.NodeCompletionResult> completionResults,
      List<ContainerId> containerIds,
      ContainerState containerState,
      String exitText,
      int containerExitCode) {
    for (ContainerId id : containerIds) {
      ContainerStatus status = ContainerStatus.newInstance(id,
          containerState,
          exitText,
          containerExitCode);
      completionResults.add(appState.onCompletedContainer(status));
    }
  }

  /**
   * Create nodes and submit them.
   * @return a list of roles
   */
  public List<RoleInstance> createAndSubmitNodes()
      throws TriggerClusterTeardownException, SliderInternalStateException {
    return createAndSubmitNodes(new ArrayList<>());
  }

  /**
   * Create nodes and submit them.
   * @return a list of roles
   */
  public List<RoleInstance> createAndSubmitNodes(List<ContainerId> containerIds)
      throws TriggerClusterTeardownException, SliderInternalStateException {
    return createAndSubmitNodes(containerIds, new ArrayList<>());
  }

  /**
   * Create nodes and submit them.
   * @return a list of roles allocated
   */
  public List<RoleInstance> createAndSubmitNodes(
      List<ContainerId> containerIds,
      List<AbstractRMOperation> operationsOut)
      throws TriggerClusterTeardownException, SliderInternalStateException {
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes();
    return submitOperations(ops, containerIds, operationsOut);
  }

  public List<RoleInstance> submitOperations(
      List<AbstractRMOperation> operationsIn,
      List<ContainerId> released) {
    return submitOperations(operationsIn, released, new ArrayList<>());
  }

  /**
   * Process the RM operations and send <code>onContainersAllocated</code>
   * events to the app state.
   * @param operationsIn list of incoming ops
   * @param released released containers
   * @return list of outbound operations
   */
  public List<RoleInstance> submitOperations(
      List<AbstractRMOperation> operationsIn,
      List<ContainerId> released,
      List<AbstractRMOperation> operationsOut) {
    List<Container> allocatedContainers = engine.execute(operationsIn,
        released);
    List<ContainerAssignment> assignments = new ArrayList<>();
    appState.onContainersAllocated(allocatedContainers, assignments,
        operationsOut);

    List<RoleInstance> roles = new ArrayList<>();
    for (ContainerAssignment assigned : assignments) {
      Container container = assigned.container;
      RoleInstance ri = roleInstance(assigned);
      //tell the app it arrived
      LOG.debug("Start submitted {} on ${}", ri.role, container.getId());
      appState.containerStartSubmitted(container, ri);
      roles.add(ri);
    }
    return roles;
  }

  /**
   * Add the AM to the app state.
   */
  protected void addAppMastertoAppState() {
//    appState.buildAppMasterNode(
//        new MockContainerId(applicationAttemptId, 999999L),
//        "appmaster",
//        0,
//        null);
  }

  /**
   * Extract the list of container IDs from the list of role instances.
   * @param instances instance list
   * @param role role to look up
   * @return the list of CIDs
   */
  public List<ContainerId> extractContainerIds(
      List<RoleInstance> instances,
      String role) {
    List<ContainerId> ids = new ArrayList<>();
    for (RoleInstance ri : instances) {
      if (ri.role.equals(role)) {
        ids.add(ri.getContainerId());
      }
    }
    return ids;
  }

  /**
   * Record a node as failing.
   * @param node
   * @param id
   * @param count
   * @return the entry
   */
  public NodeEntry recordAsFailed(NodeInstance node, int id, int count) {
    NodeEntry entry = node.getOrCreate(id);
    for (int i = 1; i <= count; i++) {
      entry.containerCompleted(
          false,
          ContainerOutcome.Failed);
    }
    return entry;
  }

  protected void recordAllFailed(int id, int count, List<NodeInstance> nodes) {
    for (NodeInstance node : nodes) {
      recordAsFailed(node, id, count);
    }
  }

  /**
   * Get the container request of an indexed entry. Includes some assertions
   * for better diagnostics
   * @param ops operation list
   * @param index index in the list
   * @return the request.
   */
  public AMRMClient.ContainerRequest getRequest(List<AbstractRMOperation> ops,
      int index) {
    assertTrue(index < ops.size());
    AbstractRMOperation op = ops.get(index);
    assertTrue(op instanceof ContainerRequestOperation);
    return ((ContainerRequestOperation) op).getRequest();
  }

  /**
   * Get the cancel request of an indexed entry. Includes some assertions for
   * better diagnostics
   * @param ops operation list
   * @param index index in the list
   * @return the request.
   */
  public AMRMClient.ContainerRequest getCancel(List<AbstractRMOperation> ops,
      int index) {
    assertTrue(index < ops.size());
    AbstractRMOperation op = ops.get(index);
    assertTrue(op instanceof CancelSingleRequest);
    return ((CancelSingleRequest) op).getRequest();
  }

  /**
   * Get the single request of a list of operations; includes the check for
   * the size.
   * @param ops operations list of size 1
   * @return the request within the first ContainerRequestOperation
   */
  public AMRMClient.ContainerRequest getSingleRequest(
      List<AbstractRMOperation> ops) {
    assertEquals(1, ops.size());
    return getRequest(ops, 0);
  }

  /**
   * Get the single request of a list of operations; includes the check for
   * the size.
   * @param ops operations list of size 1
   * @return the request within the first operation
   */
  public AMRMClient.ContainerRequest getSingleCancel(
      List<AbstractRMOperation> ops) {
    assertEquals(1, ops.size());
    return getCancel(ops, 0);
  }

  /**
   * Get the single release of a list of operations; includes the check for
   * the size.
   * @param ops operations list of size 1
   * @return the request within the first operation
   */
  public ContainerReleaseOperation getSingleRelease(
      List<AbstractRMOperation> ops) {
    assertEquals(1, ops.size());
    AbstractRMOperation op = ops.get(0);
    assertTrue(op instanceof ContainerReleaseOperation);
    return (ContainerReleaseOperation) op;
  }

  /**
   * Get the node information as a large JSON String.
   * @return
   */
  protected String nodeInformationSnapshotAsString()
      throws UnsupportedEncodingException, JsonProcessingException {
    return prettyPrintAsJson(stateAccess.getNodeInformationSnapshot());
  }

  /**
   * Scan through all containers and assert that the assignment is AA.
   * @param index role index
   */
  protected void assertAllContainersAA(int index) {
    for (Entry<String, NodeInstance> nodeMapEntry : cloneNodemap().entrySet()) {
      String name = nodeMapEntry.getKey();
      NodeInstance ni = nodeMapEntry.getValue();
      NodeEntry nodeEntry = ni.get(index);
      assertTrue("too many instances on node " + name, nodeEntry == null ||
          nodeEntry.isAntiAffinityConstraintHeld());
    }
  }

  /**
   * Get a snapshot of the nodemap of the application state.
   * @return a cloned nodemap
   */
  protected NodeMap cloneNodemap() {
    return appState.getRoleHistory().cloneNodemap();
  }

  /**
   * Issue a nodes updated event.
   * @param report report to notify
   * @return response of AM
   */
  protected AppState.NodeUpdatedOutcome updateNodes(NodeReport report) {
    return appState.onNodesUpdated(Collections.singletonList(report));
  }
}
