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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.RMNMSecurityInfoClass;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.service.AbstractService;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private long heartBeatInterval;
  private ResourceTracker resourceTracker;
  private String rmAddress;
  private Resource totalResource;
  private String containerManagerBindAddress;
  private String nodeHttpAddress;
  private String hostName;
  private int containerManagerPort;
  private int httpPort;
  private NodeId nodeId;
  private byte[] secretKeyBytes = new byte[0];
  private boolean isStopped;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private final NodeHealthCheckerService healthChecker;
  private final NodeManagerMetrics metrics;

  public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher,
      NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
    super(NodeStatusUpdaterImpl.class.getName());
    this.healthChecker = healthChecker;
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.rmAddress =
        conf.get(YarnServerConfig.RESOURCETRACKER_ADDRESS,
            YarnServerConfig.DEFAULT_RESOURCETRACKER_BIND_ADDRESS);
    this.heartBeatInterval =
        conf.getLong(NMConfig.HEARTBEAT_INTERVAL,
            NMConfig.DEFAULT_HEARTBEAT_INTERVAL);
    int memory = conf.getInt(NMConfig.NM_VMEM_GB, NMConfig.DEFAULT_NM_VMEM_GB);
    this.totalResource = recordFactory.newRecordInstance(Resource.class);
    this.totalResource.setMemory(memory * 1024);
    metrics.addResource(totalResource);
    super.init(conf);
  }

  @Override
  public void start() {
    String cmBindAddressStr =
        getConfig().get(NMConfig.NM_BIND_ADDRESS,
            NMConfig.DEFAULT_NM_BIND_ADDRESS);
    InetSocketAddress cmBindAddress =
        NetUtils.createSocketAddr(cmBindAddressStr);
    String httpBindAddressStr =
      getConfig().get(NMConfig.NM_HTTP_BIND_ADDRESS,
          NMConfig.DEFAULT_NM_HTTP_BIND_ADDRESS);
    InetSocketAddress httpBindAddress =
      NetUtils.createSocketAddr(httpBindAddressStr);
    try {
      this.hostName = InetAddress.getLocalHost().getHostAddress();
      this.containerManagerPort = cmBindAddress.getPort();
      this.httpPort = httpBindAddress.getPort();
      this.containerManagerBindAddress =
          this.hostName + ":" + this.containerManagerPort;
      this.nodeHttpAddress = this.hostName + ":" + this.httpPort;
      LOG.info("Configured ContainerManager Address is "
          + this.containerManagerBindAddress);
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
    YarnRPC rpc = YarnRPC.create(getConfig());
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(this.rmAddress);
    Configuration rmClientConf = new Configuration(getConfig());
    rmClientConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        RMNMSecurityInfoClass.class, SecurityInfo.class);
    return (ResourceTracker) rpc.getProxy(ResourceTracker.class, rmAddress,
        rmClientConf);
  }

  private void registerWithRM() throws YarnRemoteException {
    this.resourceTracker = getRMClient();
    LOG.info("Connected to ResourceManager at " + this.rmAddress);
    
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHost(this.hostName);
    request.setContainerManagerPort(this.containerManagerPort);
    request.setHttpPort(this.httpPort);
    request.setResource(this.totalResource);
    RegistrationResponse regResponse =
        this.resourceTracker.registerNodeManager(request).getRegistrationResponse();
    this.nodeId = regResponse.getNodeId();
    if (UserGroupInformation.isSecurityEnabled()) {
      this.secretKeyBytes = regResponse.getSecretKey().array();
    }

    LOG.info("Registered with ResourceManager as " + this.containerManagerBindAddress
        + " with total resource of " + this.totalResource);
  }

  @Override
  public String getContainerManagerBindAddress() {
    return this.containerManagerBindAddress;
  }

  @Override
  public byte[] getRMNMSharedSecret() {
    return this.secretKeyBytes;
  }

  private NodeStatus getNodeStatus() {
    NodeStatus status = recordFactory.newRecordInstance(NodeStatus.class);
    status.setNodeId(this.nodeId);

    Map<String, List<org.apache.hadoop.yarn.api.records.Container>> activeContainers =
        status.getAllContainers();

    int numActiveContainers = 0;
    synchronized (this.context.getContainers()) {
      for (Iterator<Entry<ContainerId, Container>> i =
          this.context.getContainers().entrySet().iterator(); i.hasNext();) {
        Entry<ContainerId, Container> e = i.next();
        ContainerId containerId = e.getKey();
        Container container = e.getValue();
        String applicationId = String.valueOf(containerId.getAppId().getId()); // TODO: ID? Really?

        List<org.apache.hadoop.yarn.api.records.Container> applicationContainers = status.getContainers(applicationId);
            activeContainers.get(applicationId);
        if (applicationContainers == null) {
          applicationContainers = new ArrayList<org.apache.hadoop.yarn.api.records.Container>();
          status.setContainers(applicationId, applicationContainers);
//          activeContainers.put(applicationId, applicationContainers);
        }

        // Clone the container to send it to the RM
        org.apache.hadoop.yarn.api.records.Container c = container.cloneAndGetContainer();
        c.setContainerManagerAddress(this.containerManagerBindAddress);
        c.setNodeHttpAddress(this.nodeHttpAddress); // TODO: don't set everytime.
        applicationContainers.add(c);
        ++numActiveContainers;
        LOG.info("Sending out status for container: " + c);

        if (c.getState() == ContainerState.COMPLETE) {
          // Remove
          i.remove();

          LOG.info("Removed completed container " + containerId);
        }
      }
    }

    LOG.debug(this.containerManagerBindAddress + " sending out status for " + numActiveContainers
        + " containers");

    NodeHealthStatus nodeHealthStatus = this.context.getNodeHealthStatus();
    if (this.healthChecker != null) {
      this.healthChecker.setHealthStatus(nodeHealthStatus);
    }
    LOG.debug("Node's health-status : " + nodeHealthStatus.getIsNodeHealthy()
        + ", " + nodeHealthStatus.getHealthReport());
    status.setNodeHealthStatus(nodeHealthStatus);

    return status;
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  protected void startStatusUpdater() throws InterruptedException,
    YarnRemoteException {

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
            List<org.apache.hadoop.yarn.api.records.Container> containersToCleanup =
                response.getContainersToCleanupList();
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
          } catch (YarnRemoteException e) {
            LOG.error("Caught exception in status-updater", e);
            break;
          } catch (InterruptedException e) {
            LOG.error("Status-updater interrupted", e);
            break;
          }
        }
      }
    }.start();
  }
}
