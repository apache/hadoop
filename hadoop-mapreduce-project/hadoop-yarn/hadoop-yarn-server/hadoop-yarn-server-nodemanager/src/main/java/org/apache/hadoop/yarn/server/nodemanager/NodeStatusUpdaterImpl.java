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

package org.apache.hadoop.yarn.server.nodemanager;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.AvroRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.AbstractService;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;
  private ContainerTokenSecretManager containerTokenSecretManager;
  private long heartBeatInterval;
  private ResourceTracker resourceTracker;
  private String rmAddress;
  private Resource totalResource;
  private int httpPort;
  private byte[] secretKeyBytes = new byte[0];
  private boolean isStopped;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics, 
      ContainerTokenSecretManager containerTokenSecretManager) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.rmAddress =
        conf.get(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS);
    this.heartBeatInterval =
        conf.getLong(YarnConfiguration.NM_TO_RM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_TO_RM_HEARTBEAT_INTERVAL_MS);
    int memory = conf.getInt(YarnConfiguration.NM_VMEM_GB, YarnConfiguration.DEFAULT_NM_VMEM_GB);
    this.totalResource = recordFactory.newRecordInstance(Resource.class);
    this.totalResource.setMemory(memory * 1024);
    metrics.addResource(totalResource);
    super.init(conf);
  }

  @Override
  public void start() {

    // NodeManager is the last service to start, so NodeId is available.
    this.nodeId = this.context.getNodeId();

    String httpBindAddressStr =
      getConfig().get(YarnConfiguration.NM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    InetSocketAddress httpBindAddress =
      NetUtils.createSocketAddr(httpBindAddressStr);
    try {
      //      this.hostName = InetAddress.getLocalHost().getCanonicalHostName();
      this.httpPort = httpBindAddress.getPort();
      // Registration has to be in start so that ContainerManager can get the
      // perNM tokens needed to authenticate ContainerTokens.
      registerWithRM();
      super.start();
      startStatusUpdater();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public synchronized void stop() {
    // Interrupt the updater.
    this.isStopped = true;
    super.stop();
  }

  protected ResourceTracker getRMClient() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.rmAddress);
    return (ResourceTracker) rpc.getProxy(ResourceTracker.class, rmAddress,
        conf);
  }

  private void registerWithRM() throws YarnRemoteException {
    this.resourceTracker = getRMClient();
    LOG.info("Connected to ResourceManager at " + this.rmAddress);
    
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(this.httpPort);
    request.setResource(this.totalResource);
    request.setNodeId(this.nodeId);
    RegistrationResponse regResponse =
        this.resourceTracker.registerNodeManager(request).getRegistrationResponse();
    if (UserGroupInformation.isSecurityEnabled()) {
      this.secretKeyBytes = regResponse.getSecretKey().array();
    }

    // do this now so that its set before we start heartbeating to RM
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Security enabled - updating secret keys now");
      // It is expected that status updater is started by this point and
      // RM gives the shared secret in registration during StatusUpdater#start().
      this.containerTokenSecretManager.setSecretKey(
          this.nodeId.toString(),
          this.getRMNMSharedSecret());
    }
    LOG.info("Registered with ResourceManager as " + this.nodeId
        + " with total resource of " + this.totalResource);

  }

  @Override
  public byte[] getRMNMSharedSecret() {
    return this.secretKeyBytes.clone();
  }

  private NodeStatus getNodeStatus() {

    NodeStatus nodeStatus = recordFactory.newRecordInstance(NodeStatus.class);
    nodeStatus.setNodeId(this.nodeId);

    int numActiveContainers = 0;
    List<ContainerStatus> containersStatuses = new ArrayList<ContainerStatus>();
    for (Iterator<Entry<ContainerId, Container>> i =
        this.context.getContainers().entrySet().iterator(); i.hasNext();) {
      Entry<ContainerId, Container> e = i.next();
      ContainerId containerId = e.getKey();
      Container container = e.getValue();

      // Clone the container to send it to the RM
      org.apache.hadoop.yarn.api.records.ContainerStatus containerStatus = 
          container.cloneAndGetContainerStatus();
      containersStatuses.add(containerStatus);
      ++numActiveContainers;
      LOG.info("Sending out status for container: " + containerStatus);

      if (containerStatus.getState() == ContainerState.COMPLETE) {
        // Remove
        i.remove();

        LOG.info("Removed completed container " + containerId);
      }
    }
    nodeStatus.setContainersStatuses(containersStatuses);

    LOG.debug(this.nodeId + " sending out status for "
        + numActiveContainers + " containers");

    NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
    if (this.healthChecker != null) {
      this.healthChecker.setHealthStatus(nodeHealthStatus);
    }
    LOG.debug("Node's health-status : " + nodeHealthStatus.getIsNodeHealthy()
        + ", " + nodeHealthStatus.getHealthReport());
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);

    return nodeStatus;
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  protected void startStatusUpdater() {

    new Thread() {
      @Override
      public void run() {
        int lastHeartBeatID = 0;
        while (!isStopped) {
          // Send heartbeat
          try {
            synchronized (heartbeatMonitor) {
              heartbeatMonitor.wait(heartBeatInterval);
            }
            NodeStatus nodeStatus = getNodeStatus();
            nodeStatus.setResponseId(lastHeartBeatID);
            
            NodeHeartbeatRequest request = recordFactory.newRecordInstance(NodeHeartbeatRequest.class);
            request.setNodeStatus(nodeStatus);            
            HeartbeatResponse response =
              resourceTracker.nodeHeartbeat(request).getHeartbeatResponse();
            lastHeartBeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanupList();
            if (containersToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup));
            }
            List<ApplicationId> appsToCleanup =
                response.getApplicationsToCleanupList();
            if (appsToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup));
            }
          } catch (Throwable e) {
            LOG.error("Caught exception in status-updater", e);
            break;
          }
        }
      }
    }.start();
  }
}
