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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.avro.AvroRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
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
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
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

  private NodeId nodeId;
  private long heartBeatInterval;
  private ResourceTracker resourceTracker;
  private InetSocketAddress rmAddress;
  private Resource totalResource;
  private int httpPort;
  private boolean isStopped;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private boolean tokenKeepAliveEnabled;
  private long tokenRemovalDelayMs;
  /** Keeps track of when the next keep alive request should be sent for an app*/
  private Map<ApplicationId, Long> appTokenKeepAliveMap =
      new HashMap<ApplicationId, Long>();
  private Random keepAliveDelayRandom = new Random();

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
    this.rmAddress = conf.getSocketAddr(
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);
    this.heartBeatInterval =
        conf.getLong(YarnConfiguration.NM_TO_RM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_TO_RM_HEARTBEAT_INTERVAL_MS);
    int memoryMb = 
        conf.getInt(
            YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB);
    float vMemToPMem =             
        conf.getFloat(
            YarnConfiguration.NM_VMEM_PMEM_RATIO, 
            YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO); 
    int virtualMemoryMb = (int)Math.ceil(memoryMb * vMemToPMem);
    
    int cpuCores =
        conf.getInt(
            YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);
    float vCoresToPCores =             
        conf.getFloat(
            YarnConfiguration.NM_VCORES_PCORES_RATIO, 
            YarnConfiguration.DEFAULT_NM_VCORES_PCORES_RATIO); 
    int virtualCores = (int)Math.ceil(cpuCores * vCoresToPCores); 

    this.totalResource = recordFactory.newRecordInstance(Resource.class);
    this.totalResource.setMemory(memoryMb);
    this.totalResource.setVirtualCores(virtualCores);
    metrics.addResource(totalResource);
    this.tokenKeepAliveEnabled = isTokenKeepAliveEnabled(conf);
    this.tokenRemovalDelayMs =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    
    LOG.info("Initialized nodemanager for " + nodeId + ":" +
    		" physical-memory=" + memoryMb + " virtual-memory=" + virtualMemoryMb +
    		" physical-cores=" + cpuCores + " virtual-cores=" + virtualCores);
    
    super.init(conf);
  }

  @Override
  public void start() {

    // NodeManager is the last service to start, so NodeId is available.
    this.nodeId = this.context.getNodeId();

    InetSocketAddress httpBindAddress = getConfig().getSocketAddr(
        YarnConfiguration.NM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_PORT);
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
  
  private boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Private
  protected boolean isTokenKeepAliveEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && isSecurityEnabled();
  }

  protected ResourceTracker getRMClient() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    return (ResourceTracker) rpc.getProxy(ResourceTracker.class, rmAddress,
        conf);
  }

  private void registerWithRM() throws YarnRemoteException {
    this.resourceTracker = getRMClient();
    LOG.info("Connecting to ResourceManager at " + this.rmAddress);
    
    RegisterNodeManagerRequest request = recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(this.httpPort);
    request.setResource(this.totalResource);
    request.setNodeId(this.nodeId);
    RegistrationResponse regResponse =
        this.resourceTracker.registerNodeManager(request).getRegistrationResponse();
    // if the Resourcemanager instructs NM to shutdown.
    if (NodeAction.SHUTDOWN.equals(regResponse.getNodeAction())) {
      throw new YarnException(
          "Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed");
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      MasterKey masterKey = regResponse.getMasterKey();
      // do this now so that its set before we start heartbeating to RM
      LOG.info("Security enabled - updating secret keys now");
      // It is expected that status updater is started by this point and
      // RM gives the shared secret in registration during
      // StatusUpdater#start().
      if (masterKey != null) {
        this.context.getContainerTokenSecretManager().setMasterKey(masterKey);
      }
    }

    LOG.info("Registered with ResourceManager as " + this.nodeId
        + " with total resource of " + this.totalResource);

  }

  private List<ApplicationId> createKeepAliveApplicationList() {
    if (!tokenKeepAliveEnabled) {
      return Collections.emptyList();
    }

    List<ApplicationId> appList = new ArrayList<ApplicationId>();
    for (Iterator<Entry<ApplicationId, Long>> i =
        this.appTokenKeepAliveMap.entrySet().iterator(); i.hasNext();) {
      Entry<ApplicationId, Long> e = i.next();
      ApplicationId appId = e.getKey();
      Long nextKeepAlive = e.getValue();
      if (!this.context.getApplications().containsKey(appId)) {
        // Remove if the application has finished.
        i.remove();
      } else if (System.currentTimeMillis() > nextKeepAlive) {
        // KeepAlive list for the next hearbeat.
        appList.add(appId);
        trackAppForKeepAlive(appId);
      }
    }
    return appList;
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
    nodeHealthStatus.setHealthReport(healthChecker.getHealthReport());
    nodeHealthStatus.setIsNodeHealthy(healthChecker.isHealthy());
    nodeHealthStatus.setLastHealthReportTime(
        healthChecker.getLastHealthReportTime());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node's health-status : " + nodeHealthStatus.getIsNodeHealthy()
                + ", " + nodeHealthStatus.getHealthReport());
    }
    nodeStatus.setNodeHealthStatus(nodeHealthStatus);

    List<ApplicationId> keepAliveAppIds = createKeepAliveApplicationList();
    nodeStatus.setKeepAliveApplications(keepAliveAppIds);
    
    return nodeStatus;
  }

  private void trackAppsForKeepAlive(List<ApplicationId> appIds) {
    if (tokenKeepAliveEnabled && appIds != null && appIds.size() > 0) {
      for (ApplicationId appId : appIds) {
        trackAppForKeepAlive(appId);
      }
    }
  }

  private void trackAppForKeepAlive(ApplicationId appId) {
    // Next keepAlive request for app between 0.7 & 0.9 of when the token will
    // likely expire.
    long nextTime = System.currentTimeMillis()
    + (long) (0.7 * tokenRemovalDelayMs + (0.2 * tokenRemovalDelayMs
        * keepAliveDelayRandom.nextInt(100))/100);
    appTokenKeepAliveMap.put(appId, nextTime);
  }

  @Override
  public void sendOutofBandHeartBeat() {
    synchronized (this.heartbeatMonitor) {
      this.heartbeatMonitor.notify();
    }
  }

  protected void startStatusUpdater() {

    new Thread("Node Status Updater") {
      @Override
      @SuppressWarnings("unchecked")
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
            
            NodeHeartbeatRequest request = recordFactory
                .newRecordInstance(NodeHeartbeatRequest.class);
            request.setNodeStatus(nodeStatus);
            if (isSecurityEnabled()) {
              request.setLastKnownMasterKey(NodeStatusUpdaterImpl.this.context
                .getContainerTokenSecretManager().getCurrentKey());
            }
            HeartbeatResponse response =
              resourceTracker.nodeHeartbeat(request).getHeartbeatResponse();

            // See if the master-key has rolled over
            if (isSecurityEnabled()) {
              MasterKey updatedMasterKey = response.getMasterKey();
              if (updatedMasterKey != null) {
                // Will be non-null only on roll-over on RM side
                context.getContainerTokenSecretManager().setMasterKey(
                  updatedMasterKey);
              }
            }

            if (response.getNodeAction() == NodeAction.SHUTDOWN) {
              LOG
                  .info("Recieved SHUTDOWN signal from Resourcemanager as part of heartbeat," +
                  		" hence shutting down.");
              dispatcher.getEventHandler().handle(
                  new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
              break;
            }
            if (response.getNodeAction() == NodeAction.REBOOT) {
              LOG.info("Node is out of sync with ResourceManager,"
                  + " hence rebooting.");
              dispatcher.getEventHandler().handle(
                  new NodeManagerEvent(NodeManagerEventType.REBOOT));
              break;
            }

            lastHeartBeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanupList();
            if (containersToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup, 
                      CMgrCompletedContainersEvent.Reason.BY_RESOURCEMANAGER));
            }
            List<ApplicationId> appsToCleanup =
                response.getApplicationsToCleanupList();
            //Only start tracking for keepAlive on FINISH_APP
            trackAppsForKeepAlive(appsToCleanup);
            if (appsToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup));
            }
          } catch (Throwable e) {
            // TODO Better error handling. Thread can die with the rest of the
            // NM still running.
            LOG.error("Caught exception in status-updater", e);
          }
        }
      }
    }.start();
  }
}
