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

package org.apache.slider.server.appmaster.rpc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.ServiceNotReadyException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.persist.AggregateConfSerDeser;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.server.appmaster.AppMasterActionOperations;
import org.apache.slider.server.appmaster.actions.ActionFlexCluster;
import org.apache.slider.server.appmaster.actions.ActionHalt;
import org.apache.slider.server.appmaster.actions.ActionKillContainer;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.actions.ActionUpgradeContainers;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.services.security.CertificateManager;
import org.apache.slider.server.services.security.SecurityStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.slider.api.proto.RestTypeMarshalling.marshall;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_COMPONENTS;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_CONTAINERS;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_NODES;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_RESOURCES;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_DESIRED;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_DESIRED_APPCONF;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_DESIRED_RESOURCES;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_RESOLVED;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_RESOLVED_APPCONF;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.MODEL_RESOLVED_RESOURCES;

/**
 * Implement the {@link SliderClusterProtocol}.
 */
@SuppressWarnings("unchecked")

public class SliderIPCService extends AbstractService
    implements SliderClusterProtocol {

  protected static final Logger log =
      LoggerFactory.getLogger(SliderIPCService.class);

  private final QueueAccess actionQueues;
  private final StateAccessForProviders state;
  private final MetricsAndMonitoring metricsAndMonitoring;
  private final AppMasterActionOperations amOperations;
  private final ContentCache cache;
  private final CertificateManager certificateManager;

  /**
   * This is the prefix used for metrics
   */
  public static final String METRICS_PREFIX =
      "org.apache.slider.api.SliderIPCService.";

  /**
   * Constructor
   * @param amOperations access to any AM operations
   * @param state state view
   * @param actionQueues queues for actions
   * @param metricsAndMonitoring metrics
   * @param cache
   */
  public SliderIPCService(AppMasterActionOperations amOperations,
      CertificateManager certificateManager,
      StateAccessForProviders state,
      QueueAccess actionQueues,
      MetricsAndMonitoring metricsAndMonitoring,
      ContentCache cache) {
    super("SliderIPCService");
    Preconditions.checkArgument(amOperations != null, "null amOperations");
    Preconditions.checkArgument(state != null, "null appState");
    Preconditions.checkArgument(actionQueues != null, "null actionQueues");
    Preconditions.checkArgument(metricsAndMonitoring != null,
        "null metricsAndMonitoring");
    Preconditions.checkArgument(cache != null, "null cache");
    this.state = state;
    this.actionQueues = actionQueues;
    this.metricsAndMonitoring = metricsAndMonitoring;
    this.amOperations = amOperations;
    this.cache = cache;
    this.certificateManager = certificateManager;
  }

  @Override   //SliderClusterProtocol
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion,
      int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }


  @Override   //SliderClusterProtocol
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return SliderClusterProtocol.versionID;
  }

  /**
   * General actions to perform on a slider RPC call coming in
   * @param operation operation to log
   * @throws IOException problems
   * @throws ServiceNotReadyException if the RPC service is constructed
   * but not fully initialized
   */
  protected void onRpcCall(String operation) throws IOException {
    log.debug("Received call to {}", operation);
    metricsAndMonitoring.markMeterAndCounter(METRICS_PREFIX + operation);
  }

  /**
   * Schedule an action
   * @param action for delayed execution
   */
  public void schedule(AsyncAction action) {
    actionQueues.schedule(action);
  }

  /**
   * Queue an action for immediate execution in the executor thread
   * @param action action to execute
   */
  public void queue(AsyncAction action) {
    actionQueues.put(action);
  }

  @Override //SliderClusterProtocol
  public Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request)
      throws IOException, YarnException {
    onRpcCall("stop");
    String message = request.getMessage();
    if (message == null) {
      message = "application stopped by client";
    }
    ActionStopSlider stopSlider =
        new ActionStopSlider(message,
            1000, TimeUnit.MILLISECONDS,
            LauncherExitCodes.EXIT_SUCCESS,
            FinalApplicationStatus.SUCCEEDED,
            message);
    log.info("SliderAppMasterApi.stopCluster: {}", stopSlider);
    schedule(stopSlider);
    return Messages.StopClusterResponseProto.getDefaultInstance();
  }

  @Override //SliderClusterProtocol
  public Messages.UpgradeContainersResponseProto upgradeContainers(
      Messages.UpgradeContainersRequestProto request) throws IOException,
      YarnException {
    onRpcCall("upgrade");
    String message = request.getMessage();
    if (message == null) {
      message = "application containers upgraded by client";
    }
    ActionUpgradeContainers upgradeContainers =
        new ActionUpgradeContainers(
            "Upgrade containers",
            1000, TimeUnit.MILLISECONDS,
            LauncherExitCodes.EXIT_SUCCESS,
            FinalApplicationStatus.SUCCEEDED,
            request.getContainerList(),
            request.getComponentList(),
            message);
    log.info("SliderAppMasterApi.upgradeContainers: {}", upgradeContainers);
    schedule(upgradeContainers);
    return Messages.UpgradeContainersResponseProto.getDefaultInstance();
  }

  @Override //SliderClusterProtocol
  public Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request)
      throws IOException {
    onRpcCall("flex");
    String payload = request.getClusterSpec();
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTree updatedResources = confTreeSerDeser.fromJson(payload);
    schedule(new ActionFlexCluster("flex", 1, TimeUnit.MILLISECONDS,
        updatedResources));
    return Messages.FlexClusterResponseProto.newBuilder().setResponse(
        true).build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
      Messages.GetJSONClusterStatusRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getstatus");
    String result;
    //quick update
    //query and json-ify
    ClusterDescription cd = state.refreshClusterStatus();
    result = cd.toJsonString();
    String stat = result;
    return Messages.GetJSONClusterStatusResponseProto.newBuilder()
                                                     .setClusterSpec(stat)
                                                     .build();
  }

  @Override
  public Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
      Messages.GetInstanceDefinitionRequestProto request)
      throws IOException, YarnException {

    onRpcCall("getinstancedefinition");
    String internal;
    String resources;
    String app;
    AggregateConf instanceDefinition =
        state.getInstanceDefinitionSnapshot();
    internal = instanceDefinition.getInternal().toJson();
    resources = instanceDefinition.getResources().toJson();
    app = instanceDefinition.getAppConf().toJson();
    assert internal != null;
    assert resources != null;
    assert app != null;
    log.debug("Generating getInstanceDefinition Response");
    Messages.GetInstanceDefinitionResponseProto.Builder builder =
        Messages.GetInstanceDefinitionResponseProto.newBuilder();
    builder.setInternal(internal);
    builder.setResources(resources);
    builder.setApplication(app);
    return builder.build();
  }

  @Override //SliderClusterProtocol
  public Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request)
      throws IOException, YarnException {
    onRpcCall("listnodes)");
    String role = request.getRole();
    Messages.ListNodeUUIDsByRoleResponseProto.Builder builder =
        Messages.ListNodeUUIDsByRoleResponseProto.newBuilder();
    List<RoleInstance> nodes = state.enumLiveInstancesInRole(role);
    for (RoleInstance node : nodes) {
      builder.addUuid(node.id);
    }
    return builder.build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getnode");
    RoleInstance instance = state.getLiveInstanceByContainerID(
        request.getUuid());
    return Messages.GetNodeResponseProto.newBuilder()
                                        .setClusterNode(instance.toProtobuf())
                                        .build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetClusterNodesResponseProto getClusterNodes(
      Messages.GetClusterNodesRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getclusternodes");
    List<RoleInstance>
        clusterNodes = state.getLiveInstancesByContainerIDs(
        request.getUuidList());

    Messages.GetClusterNodesResponseProto.Builder builder =
        Messages.GetClusterNodesResponseProto.newBuilder();
    for (RoleInstance node : clusterNodes) {
      builder.addClusterNode(node.toProtobuf());
    }
    //at this point: a possibly empty list of nodes
    return builder.build();
  }

  @Override
  public Messages.EchoResponseProto echo(Messages.EchoRequestProto request)
      throws IOException, YarnException {
    onRpcCall("echo");
    Messages.EchoResponseProto.Builder builder =
        Messages.EchoResponseProto.newBuilder();
    String text = request.getText();
    log.info("Echo request size ={}", text.length());
    log.info(text);
    //now return it
    builder.setText(text);
    return builder.build();
  }

  @Override
  public Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request)
      throws IOException, YarnException {
    onRpcCall("killcontainer");
    String containerID = request.getId();
    log.info("Kill Container {}", containerID);
    //throws NoSuchNodeException if it is missing
    RoleInstance instance =
        state.getLiveInstanceByContainerID(containerID);
    queue(new ActionKillContainer(instance.getId(), 0, TimeUnit.MILLISECONDS,
        amOperations));
    Messages.KillContainerResponseProto.Builder builder =
        Messages.KillContainerResponseProto.newBuilder();
    builder.setSuccess(true);
    return builder.build();
  }


  @Override
  public Messages.AMSuicideResponseProto amSuicide(
      Messages.AMSuicideRequestProto request)
      throws IOException {
    onRpcCall("amsuicide");
    int signal = request.getSignal();
    String text = request.getText();
    if (text == null) {
      text = "";
    }
    int delay = request.getDelay();
    log.info("AM Suicide with signal {}, message {} delay = {}", signal, text,
        delay);
    ActionHalt action = new ActionHalt(signal, text, delay,
        TimeUnit.MILLISECONDS);
    schedule(action);
    return Messages.AMSuicideResponseProto.getDefaultInstance();
  }

  @Override
  public Messages.ApplicationLivenessInformationProto getLivenessInformation(
      Messages.GetApplicationLivenessRequestProto request) throws IOException {
    ApplicationLivenessInformation info =
        state.getApplicationLivenessInformation();
    return marshall(info);
  }

  @Override
  public Messages.GetLiveContainersResponseProto getLiveContainers(
      Messages.GetLiveContainersRequestProto request)
      throws IOException {
    Map<String, ContainerInformation> infoMap =
        (Map<String, ContainerInformation>) cache.lookupWithIOE(LIVE_CONTAINERS);
    Messages.GetLiveContainersResponseProto.Builder builder =
        Messages.GetLiveContainersResponseProto.newBuilder();

    for (Map.Entry<String, ContainerInformation> entry : infoMap.entrySet()) {
      builder.addNames(entry.getKey());
      builder.addContainers(marshall(entry.getValue()));
    }
    return builder.build();
  }

  @Override
  public Messages.ContainerInformationProto getLiveContainer(Messages.GetLiveContainerRequestProto request)
      throws IOException {
    String containerId = request.getContainerId();
    RoleInstance id = state.getLiveInstanceByContainerID(containerId);
    ContainerInformation containerInformation = id.serialize();
    return marshall(containerInformation);
  }

  @Override
  public Messages.GetLiveComponentsResponseProto getLiveComponents(Messages.GetLiveComponentsRequestProto request)
      throws IOException {
    Map<String, ComponentInformation> infoMap =
        (Map<String, ComponentInformation>) cache.lookupWithIOE(LIVE_COMPONENTS);
    Messages.GetLiveComponentsResponseProto.Builder builder =
        Messages.GetLiveComponentsResponseProto.newBuilder();

    for (Map.Entry<String, ComponentInformation> entry : infoMap.entrySet()) {
      builder.addNames(entry.getKey());
      builder.addComponents(marshall(entry.getValue()));
    }
    return builder.build();
  }


  @Override
  public Messages.ComponentInformationProto getLiveComponent(Messages.GetLiveComponentRequestProto request)
      throws IOException {
    String name = request.getName();
    try {
      return marshall(state.getComponentInformation(name));
    } catch (YarnRuntimeException e) {
      throw new FileNotFoundException("Unknown component: " + name);
    }
  }

  @Override
  public Messages.GetLiveNodesResponseProto getLiveNodes(Messages.GetLiveNodesRequestProto request)
      throws IOException {
    NodeInformationList info = (NodeInformationList) cache.lookupWithIOE(LIVE_NODES);
    Messages.GetLiveNodesResponseProto.Builder builder =
        Messages.GetLiveNodesResponseProto.newBuilder();

    for (NodeInformation nodeInformation : info) {
      builder.addNodes(marshall(nodeInformation));
    }
    return builder.build();
  }


  @Override
  public Messages.NodeInformationProto getLiveNode(Messages.GetLiveNodeRequestProto request)
      throws IOException {
    String name = request.getName();
    NodeInformation nodeInformation = state.getNodeInformation(name);
    if (nodeInformation != null) {
      return marshall(nodeInformation);
    } else {
      throw new FileNotFoundException("Unknown host: " + name);
    }
  }

  @Override
  public Messages.WrappedJsonProto getModelDesired(Messages.EmptyPayloadProto request) throws IOException {
    return lookupAggregateConf(MODEL_DESIRED);
  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredAppconf(Messages.EmptyPayloadProto request) throws IOException {
    return lookupConfTree(MODEL_DESIRED_APPCONF);
  }

  @Override
  public Messages.WrappedJsonProto getModelDesiredResources(Messages.EmptyPayloadProto request) throws IOException {
    return lookupConfTree(MODEL_DESIRED_RESOURCES);
  }

  @Override
  public Messages.WrappedJsonProto getModelResolved(Messages.EmptyPayloadProto request) throws IOException {
    return lookupAggregateConf(MODEL_RESOLVED);
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedAppconf(Messages.EmptyPayloadProto request) throws IOException {
    return lookupConfTree(MODEL_RESOLVED_APPCONF);
  }

  @Override
  public Messages.WrappedJsonProto getModelResolvedResources(Messages.EmptyPayloadProto request) throws IOException {
    return lookupConfTree(MODEL_RESOLVED_RESOURCES);
  }

  @Override
  public Messages.WrappedJsonProto getLiveResources(Messages.EmptyPayloadProto request) throws IOException {
    return lookupConfTree(LIVE_RESOURCES);
  }

  /**
   * Helper method; look up an aggregate configuration in the cache from
   * a key, or raise an exception
   * @param key key to resolve
   * @return the configuration
   * @throws IOException on a failure
   */

  protected Messages.WrappedJsonProto lookupAggregateConf(String key) throws
      IOException {
    AggregateConf aggregateConf = (AggregateConf) cache.lookupWithIOE(key);
    String json = AggregateConfSerDeser.toString(aggregateConf);
    return wrap(json);
  }

  /**
   * Helper method; look up an conf tree in the cache from
   * a key, or raise an exception
   * @param key key to resolve
   * @return the configuration
   * @throws IOException on a failure
   */
  protected Messages.WrappedJsonProto lookupConfTree(String key) throws
      IOException {
    ConfTree conf = (ConfTree) cache.lookupWithIOE(key);
    String json = ConfTreeSerDeser.toString(conf);
    return wrap(json);
  }

  private Messages.WrappedJsonProto wrap(String json) {
    Messages.WrappedJsonProto.Builder builder =
        Messages.WrappedJsonProto.newBuilder();
    builder.setJson(json);
    return builder.build();
  }

  @Override
  public Messages.GetCertificateStoreResponseProto getClientCertificateStore(Messages.GetCertificateStoreRequestProto request) throws
      IOException {
    String hostname = request.getHostname();
    String clientId = request.getRequesterId();
    String password = request.getPassword();
    String type = request.getType();

    SecurityStore store = null;
    try {
      if ( SecurityStore.StoreType.keystore.equals(
          SecurityStore.StoreType.valueOf(type))) {
        store = certificateManager.generateContainerKeystore(hostname,
                                                             clientId,
                                                             null,
                                                             password);
      } else if (SecurityStore.StoreType.truststore.equals(
          SecurityStore.StoreType.valueOf(type))) {
        store = certificateManager.generateContainerTruststore(clientId,
                                                               null,
                                                               password);

      } else {
        throw new IOException("Illegal store type");
      }
    } catch (SliderException e) {
      throw new IOException(e);
    }
    return marshall(store);
  }
}
