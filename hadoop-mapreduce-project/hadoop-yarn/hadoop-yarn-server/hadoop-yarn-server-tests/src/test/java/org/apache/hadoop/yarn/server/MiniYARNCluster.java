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

package org.apache.hadoop.yarn.server;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;

public class MiniYARNCluster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MiniYARNCluster.class);

  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  private NodeManager nodeManager;
  private ResourceManager resourceManager;

  private ResourceManagerWrapper resourceManagerWrapper;
  private NodeManagerWrapper nodeManagerWrapper;
  
  private File testWorkDir;

  public MiniYARNCluster(String testName) {
    super(testName);
    this.testWorkDir = new File("target", testName);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("COULD NOT CLEANUP", e);
      throw new YarnException("could not cleanup test dir", e);
    } 
    resourceManagerWrapper = new ResourceManagerWrapper();
    addService(resourceManagerWrapper);
    nodeManagerWrapper = new NodeManagerWrapper();
    addService(nodeManagerWrapper);
  }

  public File getTestWorkDir() {
    return testWorkDir;
  }

  public ResourceManager getResourceManager() {
    return this.resourceManager;
  }

  public NodeManager getNodeManager() {
    return this.nodeManager;
  }

  private class ResourceManagerWrapper extends AbstractService {
    public ResourceManagerWrapper() {
      super(ResourceManagerWrapper.class.getName());
    }

    @Override
    public synchronized void start() {
      try {
        Store store = StoreFactory.getStore(getConfig());
        resourceManager = new ResourceManager(store) {
          @Override
          protected void doSecureLogin() throws IOException {
            // Don't try to login using keytab in the testcase.
          };
        };
        resourceManager.init(getConfig());
        new Thread() {
          public void run() {
            resourceManager.start();
          };
        }.start();
        int waitCount = 0;
        while (resourceManager.getServiceState() == STATE.INITED
            && waitCount++ < 60) {
          LOG.info("Waiting for RM to start...");
          Thread.sleep(1500);
        }
        if (resourceManager.getServiceState() != STATE.STARTED) {
          // RM could have failed.
          throw new IOException(
              "ResourceManager failed to start. Final state is "
                  + resourceManager.getServiceState());
        }
        super.start();
      } catch (Throwable t) {
        throw new YarnException(t);
      }
    }

    @Override
    public synchronized void stop() {
      if (resourceManager != null) {
        resourceManager.stop();
      }
      super.stop();
    }
  }

  private class NodeManagerWrapper extends AbstractService {
    public NodeManagerWrapper() {
      super(NodeManagerWrapper.class.getName());
    }

    public synchronized void start() {
      try {
        File localDir =
            new File(testWorkDir, MiniYARNCluster.this.getName() + "-localDir");
        localDir.mkdir();
        LOG.info("Created localDir in " + localDir.getAbsolutePath());
        getConfig().set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
        File logDir =
            new File(testWorkDir, MiniYARNCluster.this.getName()
                + "-logDir");
        File remoteLogDir =
          new File(testWorkDir, MiniYARNCluster.this.getName()
              + "-remoteLogDir");
        logDir.mkdir();
        remoteLogDir.mkdir();
        LOG.info("Created logDir in " + logDir.getAbsolutePath());
        getConfig().set(YarnConfiguration.NM_LOG_DIRS, logDir.getAbsolutePath());
        getConfig().set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            remoteLogDir.getAbsolutePath());
        getConfig().setInt(YarnConfiguration.NM_VMEM_GB, 4); // By default AM + 2 containers
        nodeManager = new NodeManager() {

          @Override
          protected void doSecureLogin() throws IOException {
            // Don't try to login using keytab in the testcase.
          };

          @Override
          protected NodeStatusUpdater createNodeStatusUpdater(Context context,
              Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
            return new NodeStatusUpdaterImpl(context, dispatcher,
                healthChecker, metrics) {
              @Override
              protected ResourceTracker getRMClient() {
                final ResourceTrackerService rt = resourceManager
                    .getResourceTrackerService();
                final RecordFactory recordFactory =
                  RecordFactoryProvider.getRecordFactory(null);

                // For in-process communication without RPC
                return new ResourceTracker() {

                  @Override
                  public NodeHeartbeatResponse nodeHeartbeat(
                      NodeHeartbeatRequest request) throws YarnRemoteException {
                    NodeHeartbeatResponse response = recordFactory.newRecordInstance(
                        NodeHeartbeatResponse.class);
                    try {
                      response.setHeartbeatResponse(rt.nodeHeartbeat(request)
                          .getHeartbeatResponse());
                    } catch (IOException ioe) {
                      LOG.info("Exception in heartbeat from node " + 
                          request.getNodeStatus().getNodeId(), ioe);
                      throw RPCUtil.getRemoteException(ioe);
                    }
                    return response;
                  }

                  @Override
                  public RegisterNodeManagerResponse registerNodeManager(
                      RegisterNodeManagerRequest request)
                      throws YarnRemoteException {
                    RegisterNodeManagerResponse response = recordFactory.newRecordInstance(
                        RegisterNodeManagerResponse.class);
                    try {
                      response.setRegistrationResponse(rt
                          .registerNodeManager(request)
                          .getRegistrationResponse());
                    } catch (IOException ioe) {
                      LOG.info("Exception in node registration from "
                          + request.getNodeId().toString(), ioe);
                      throw RPCUtil.getRemoteException(ioe);
                    }
                    return response;
                  }
                };
              };
            };
          };
        };
        nodeManager.init(getConfig());
        new Thread() {
          public void run() {
            nodeManager.start();
          };
        }.start();
        int waitCount = 0;
        while (nodeManager.getServiceState() == STATE.INITED
            && waitCount++ < 60) {
          LOG.info("Waiting for NM to start...");
          Thread.sleep(1000);
        }
        if (nodeManager.getServiceState() != STATE.STARTED) {
          // RM could have failed.
          throw new IOException("NodeManager failed to start");
        }
        super.start();
      } catch (Throwable t) {
        throw new YarnException(t);
      }
    }

    @Override
    public synchronized void stop() {
      if (nodeManager != null) {
        nodeManager.stop();
      }
      super.stop();
    }
  }
}
