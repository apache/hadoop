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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CancelUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.CompInstancesUpgradeResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.ComponentCountProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.DecommissionCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.DecommissionCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.RestartServiceResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.UpgradeServiceResponseProto;
import org.apache.hadoop.yarn.service.api.records.ComponentContainers;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.utils.FilterUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.hadoop.yarn.service.component.ComponentEventType.DECOMMISSION_INSTANCE;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.FLEX;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.YARN_SERVICE_AM_CLIENT_PORT_RANGE;

public class ClientAMService extends AbstractService
    implements ClientAMProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientAMService.class);

  private ServiceContext context;
  private Server server;

  private InetSocketAddress bindAddress;

  public ClientAMService(ServiceContext context) {
    super("Client AM Service");
    this.context = context;
  }

  @Override protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    String nodeHostString = getNMHostName();

    InetSocketAddress address = new InetSocketAddress(nodeHostString, 0);
    server = rpc.getServer(ClientAMProtocol.class, this, address, conf,
        context.secretManager, 1, YARN_SERVICE_AM_CLIENT_PORT_RANGE);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      this.server.refreshServiceAcl(getConfig(), new ClientAMPolicyProvider());
    }

    server.start();

    bindAddress = NetUtils.createSocketAddrForHost(nodeHostString,
        server.getListenerAddress().getPort());

    LOG.info("Instantiated ClientAMService at " + bindAddress);
    super.serviceStart();
  }

  @VisibleForTesting
  String getNMHostName() throws BadClusterStateException {
    return ServiceUtils.mandatoryEnvVariable(
        ApplicationConstants.Environment.NM_HOST.name());
  }

  @Override protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  @Override public FlexComponentsResponseProto flexComponents(
      FlexComponentsRequestProto request) throws IOException {
    if (!request.getComponentsList().isEmpty()) {
      for (ComponentCountProto component : request.getComponentsList()) {
        ComponentEvent event = new ComponentEvent(component.getName(), FLEX)
            .setDesired(component.getNumberOfContainers());
        context.scheduler.getDispatcher().getEventHandler().handle(event);
        LOG.info("Flexing component {} to {}", component.getName(),
            component.getNumberOfContainers());
      }
    }
    return FlexComponentsResponseProto.newBuilder().build();
  }

  @Override
  public GetStatusResponseProto getStatus(GetStatusRequestProto request)
      throws IOException, YarnException {
    String stat = ServiceApiUtil.jsonSerDeser.toJson(context.service);
    return GetStatusResponseProto.newBuilder().setStatus(stat).build();
  }

  @Override
  public StopResponseProto stop(StopRequestProto requestProto)
      throws IOException, YarnException {
    LOG.info("Stop the service by {}", UserGroupInformation.getCurrentUser());
    context.scheduler.getDiagnostics()
        .append("Stopped by user " + UserGroupInformation.getCurrentUser());
    context.scheduler.setGracefulStop(FinalApplicationStatus.ENDED);

    // Stop the service in 2 seconds delay to make sure this rpc call is completed.
    // shutdown hook will be executed which will stop AM gracefully.
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          ExitUtil.terminate(0);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while stopping", e);
        }
      }
    };
    thread.start();
    return StopResponseProto.newBuilder().build();
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  public UpgradeServiceResponseProto upgrade(
      UpgradeServiceRequestProto request) throws IOException {
    try {
      LOG.info("Upgrading service to version {} by {}", request.getVersion(),
          UserGroupInformation.getCurrentUser());
      context.getServiceManager().processUpgradeRequest(request.getVersion(),
          request.getAutoFinalize(), request.getExpressUpgrade());
      return UpgradeServiceResponseProto.newBuilder().build();
    } catch (Exception ex) {
      return UpgradeServiceResponseProto.newBuilder().setError(ex.getMessage())
          .build();
    }
  }

  @Override
  public RestartServiceResponseProto restart(RestartServiceRequestProto request)
      throws IOException, YarnException {
    ServiceEvent event = new ServiceEvent(ServiceEventType.START);
    context.scheduler.getDispatcher().getEventHandler().handle(event);
    LOG.info("Restart service by {}", UserGroupInformation.getCurrentUser());
    return RestartServiceResponseProto.newBuilder().build();
  }

  @Override
  public CompInstancesUpgradeResponseProto upgrade(
      CompInstancesUpgradeRequestProto request)
      throws IOException, YarnException {
    if (!request.getContainerIdsList().isEmpty()) {

      for (String containerId : request.getContainerIdsList()) {
        ComponentInstanceEvent event =
            new ComponentInstanceEvent(ContainerId.fromString(containerId),
                ComponentInstanceEventType.UPGRADE);
        LOG.info("Upgrade container {}", containerId);
        context.scheduler.getDispatcher().getEventHandler().handle(event);
      }
    }
    return CompInstancesUpgradeResponseProto.newBuilder().build();
  }

  @Override
  public GetCompInstancesResponseProto getCompInstances(
      GetCompInstancesRequestProto request) throws IOException {
    List<ComponentContainers> containers = FilterUtils.filterInstances(context,
        request);
    return GetCompInstancesResponseProto.newBuilder().setCompInstances(
        ServiceApiUtil.COMP_CONTAINERS_JSON_SERDE.toJson(containers.toArray(
            new ComponentContainers[containers.size()]))).build();
  }

  @Override
  public CancelUpgradeResponseProto cancelUpgrade(
      CancelUpgradeRequestProto request) throws IOException, YarnException {
    LOG.info("Cancel service upgrade by {}",
        UserGroupInformation.getCurrentUser());
    ServiceEvent event = new ServiceEvent(ServiceEventType.CANCEL_UPGRADE);
    context.scheduler.getDispatcher().getEventHandler().handle(event);
    return CancelUpgradeResponseProto.newBuilder().build();
  }

  @Override
  public DecommissionCompInstancesResponseProto decommissionCompInstances(
      DecommissionCompInstancesRequestProto request)
      throws IOException, YarnException {
    if (!request.getCompInstancesList().isEmpty()) {
      for (String instance : request.getCompInstancesList()) {
        String componentName = ServiceApiUtil.parseComponentName(instance);
        ComponentEvent event = new ComponentEvent(componentName,
            DECOMMISSION_INSTANCE).setInstanceName(instance);
        context.scheduler.getDispatcher().getEventHandler().handle(event);
        LOG.info("Decommissioning component {} instance {}", componentName,
            instance);
      }
    }
    return DecommissionCompInstancesResponseProto.newBuilder().build();
  }
}
