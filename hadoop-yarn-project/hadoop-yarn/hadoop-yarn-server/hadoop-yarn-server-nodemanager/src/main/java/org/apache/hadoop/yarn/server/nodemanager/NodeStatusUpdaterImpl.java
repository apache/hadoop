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
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;

public class NodeStatusUpdaterImpl extends AbstractService implements
    NodeStatusUpdater {

  private static final Log LOG = LogFactory.getLog(NodeStatusUpdaterImpl.class);

  private final Object heartbeatMonitor = new Object();

  private final Context context;
  private final Dispatcher dispatcher;

  private NodeId nodeId;
  private long nextHeartBeatInterval;
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
  private long rmConnectWaitMS;
  private long rmConnectionRetryIntervalMS;
  private boolean waitForEver;

  private Runnable statusUpdaterRunnable;
  private Thread  statusUpdater;
  private long rmIdentifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;

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

  protected void rebootNodeStatusUpdater() {
    // Interrupt the updater.
    this.isStopped = true;

    try {
      statusUpdater.join();
      registerWithRM();
      statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
      this.isStopped = false;
      statusUpdater.start();
      LOG.info("NodeStatusUpdater thread is reRegistered and restarted");
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
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

  @VisibleForTesting
  protected void registerWithRM() throws YarnRemoteException {
    Configuration conf = getConfig();
    rmConnectWaitMS =
        conf.getInt(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_WAIT_SECS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_WAIT_SECS)
        * 1000;
    rmConnectionRetryIntervalMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS,
            YarnConfiguration
                .DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS)
        * 1000;

    if(rmConnectionRetryIntervalMS < 0) {
      throw new YarnException("Invalid Configuration. " +
          YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS +
          " should not be negative.");
    }

    waitForEver = (rmConnectWaitMS == -1000);

    if(! waitForEver) {
      if(rmConnectWaitMS < 0) {
          throw new YarnException("Invalid Configuration. " +
              YarnConfiguration.RESOURCEMANAGER_CONNECT_WAIT_SECS +
              " can be -1, but can not be other negative numbers");
      }

      //try connect once
      if(rmConnectWaitMS < rmConnectionRetryIntervalMS) {
        LOG.warn(YarnConfiguration.RESOURCEMANAGER_CONNECT_WAIT_SECS
            + " is smaller than "
            + YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS
            + ". Only try connect once.");
        rmConnectWaitMS = 0;
      }
    }

    int rmRetryCount = 0;
    long waitStartTime = System.currentTimeMillis();

    RegisterNodeManagerRequest request =
        recordFactory.newRecordInstance(RegisterNodeManagerRequest.class);
    request.setHttpPort(this.httpPort);
    request.setResource(this.totalResource);
    request.setNodeId(this.nodeId);
    RegisterNodeManagerResponse regNMResponse;

    while(true) {
      try {
        rmRetryCount++;
        LOG.info("Connecting to ResourceManager at " + this.rmAddress
            + ". current no. of attempts is " + rmRetryCount);
        this.resourceTracker = getRMClient();
        regNMResponse =
            this.resourceTracker.registerNodeManager(request);
        this.rmIdentifier = regNMResponse.getRMIdentifier();
        break;
      } catch(Throwable e) {
        LOG.warn("Trying to connect to ResourceManager, " +
            "current no. of failed attempts is "+rmRetryCount);
        if(System.currentTimeMillis() - waitStartTime < rmConnectWaitMS
            || waitForEver) {
          try {
            LOG.info("Sleeping for " + rmConnectionRetryIntervalMS/1000
                + " seconds before next connection retry to RM");
            Thread.sleep(rmConnectionRetryIntervalMS);
          } catch(InterruptedException ex) {
            //done nothing
          }
        } else {
          String errorMessage = "Failed to Connect to RM, " +
              "no. of failed attempts is "+rmRetryCount;
          LOG.error(errorMessage,e);
          throw new YarnException(errorMessage,e);
        }
      }
    }
    // if the Resourcemanager instructs NM to shutdown.
    if (NodeAction.SHUTDOWN.equals(regNMResponse.getNodeAction())) {
      throw new YarnException(
          "Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed");
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      MasterKey masterKey = regNMResponse.getMasterKey();
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
    LOG.info("Notifying ContainerManager to unblock new container-requests");
    ((ContainerManagerImpl) this.context.getContainerManager())
      .setBlockNewContainerRequests(false);
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

  @Override
  public NodeStatus getNodeStatusAndUpdateContainersInContext() {

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

  @Override
  public long getRMIdentifier() {
    return this.rmIdentifier;
  }

  protected void startStatusUpdater() {

    statusUpdaterRunnable = new Runnable() {
      @Override
      @SuppressWarnings("unchecked")
      public void run() {
        int lastHeartBeatID = 0;
        while (!isStopped) {
          // Send heartbeat
          try {
            NodeHeartbeatResponse response = null;
            int rmRetryCount = 0;
            long waitStartTime = System.currentTimeMillis();
            NodeStatus nodeStatus = getNodeStatusAndUpdateContainersInContext();
            nodeStatus.setResponseId(lastHeartBeatID);
            
            NodeHeartbeatRequest request = recordFactory
                .newRecordInstance(NodeHeartbeatRequest.class);
            request.setNodeStatus(nodeStatus);
            if (isSecurityEnabled()) {
              request.setLastKnownMasterKey(NodeStatusUpdaterImpl.this.context
                .getContainerTokenSecretManager().getCurrentKey());
            }
            while (!isStopped) {
              try {
                rmRetryCount++;
                response = resourceTracker.nodeHeartbeat(request);
                break;
              } catch (Throwable e) {
                LOG.warn("Trying to heartbeat to ResourceManager, "
                    + "current no. of failed attempts is " + rmRetryCount);
                if(System.currentTimeMillis() - waitStartTime < rmConnectWaitMS
                    || waitForEver) {
                  try {
                    LOG.info("Sleeping for " + rmConnectionRetryIntervalMS/1000
                        + " seconds before next heartbeat to RM");
                    Thread.sleep(rmConnectionRetryIntervalMS);
                  } catch(InterruptedException ex) {
                    //done nothing
                  }
                } else {
                  String errorMessage = "Failed to heartbeat to RM, " +
                      "no. of failed attempts is "+rmRetryCount;
                  LOG.error(errorMessage,e);
                  throw new YarnException(errorMessage,e);
                }
              }
            }
            //get next heartbeat interval from response
            nextHeartBeatInterval = response.getNextHeartBeatInterval();
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
            if (response.getNodeAction() == NodeAction.RESYNC) {
              LOG.info("Node is out of sync with ResourceManager,"
                  + " hence rebooting.");
              // Invalidate the RMIdentifier while resync
              NodeStatusUpdaterImpl.this.rmIdentifier =
                  ResourceManagerConstants.RM_INVALID_IDENTIFIER;
              dispatcher.getEventHandler().handle(
                  new NodeManagerEvent(NodeManagerEventType.RESYNC));
              break;
            }

            lastHeartBeatID = response.getResponseId();
            List<ContainerId> containersToCleanup = response
                .getContainersToCleanup();
            if (containersToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedContainersEvent(containersToCleanup, 
                      CMgrCompletedContainersEvent.Reason.BY_RESOURCEMANAGER));
            }
            List<ApplicationId> appsToCleanup =
                response.getApplicationsToCleanup();
            //Only start tracking for keepAlive on FINISH_APP
            trackAppsForKeepAlive(appsToCleanup);
            if (appsToCleanup.size() != 0) {
              dispatcher.getEventHandler().handle(
                  new CMgrCompletedAppsEvent(appsToCleanup));
            }
          } catch (YarnException e) {
            //catch and throw the exception if tried MAX wait time to connect RM
            dispatcher.getEventHandler().handle(
                new NodeManagerEvent(NodeManagerEventType.SHUTDOWN));
            throw e;
          } catch (Throwable e) {
            // TODO Better error handling. Thread can die with the rest of the
            // NM still running.
            LOG.error("Caught exception in status-updater", e);
          } finally {
            synchronized (heartbeatMonitor) {
              nextHeartBeatInterval = nextHeartBeatInterval <= 0 ?
                  YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS :
                    nextHeartBeatInterval;
              try {
                heartbeatMonitor.wait(nextHeartBeatInterval);
              } catch (InterruptedException e) {
                // Do Nothing
              }
            }
          }
        }
      }
    };
    statusUpdater =
        new Thread(statusUpdaterRunnable, "Node Status Updater");
    statusUpdater.start();
  }
}
