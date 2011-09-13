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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeStatusUpdater {

  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  static final Log LOG = LogFactory.getLog(TestNodeStatusUpdater.class);
  static final Path basedir =
      new Path("target", TestNodeStatusUpdater.class.getName());
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  int heartBeatID = 0;
  volatile Error nmStartError = null;
  private final List<NodeId> registeredNodes = new ArrayList<NodeId>();

  @After
  public void tearDown() {
    this.registeredNodes.clear();
    DefaultMetricsSystem.shutdown();
  }

  private class MyResourceTracker implements ResourceTracker {

    private final Context context;

    public MyResourceTracker(Context context) {
      this.context = context;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      NodeId nodeId = request.getNodeId();
      Resource resource = request.getResource();
      LOG.info("Registering " + nodeId.toString());
      try {
        Assert.assertEquals(InetAddress.getLocalHost().getHostAddress()
            + ":12345", nodeId.toString());
      } catch (UnknownHostException e) {
        Assert.fail(e.getMessage());
      }
      Assert.assertEquals(5 * 1024, resource.getMemory());
      registeredNodes.add(nodeId);
      RegistrationResponse regResponse = recordFactory
          .newRecordInstance(RegistrationResponse.class);

      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setRegistrationResponse(regResponse);
      return response;
    }

    ApplicationId applicationID = recordFactory
        .newRecordInstance(ApplicationId.class);
    ApplicationAttemptId appAttemptID = recordFactory
        .newRecordInstance(ApplicationAttemptId.class);
    ContainerId firstContainerID = recordFactory
        .newRecordInstance(ContainerId.class);
    ContainerId secondContainerID = recordFactory
        .newRecordInstance(ContainerId.class);

    private Map<ApplicationId, List<ContainerStatus>> getAppToContainerStatusMap(
        List<ContainerStatus> containers) {
      Map<ApplicationId, List<ContainerStatus>> map =
          new HashMap<ApplicationId, List<ContainerStatus>>();
      for (ContainerStatus cs : containers) {
        ApplicationId applicationId = 
            cs.getContainerId().getApplicationAttemptId().getApplicationId();
        List<ContainerStatus> appContainers = map.get(applicationId);
        if (appContainers == null) {
          appContainers = new ArrayList<ContainerStatus>();
          map.put(applicationId, appContainers);
        }
        appContainers.add(cs);
      }
      return map;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      NodeStatus nodeStatus = request.getNodeStatus();
      LOG.info("Got heartbeat number " + heartBeatID);
      nodeStatus.setResponseId(heartBeatID++);
      Map<ApplicationId, List<ContainerStatus>> appToContainers =
          getAppToContainerStatusMap(nodeStatus.getContainersStatuses());
      if (heartBeatID == 1) {
        Assert.assertEquals(0, nodeStatus.getContainersStatuses().size());

        // Give a container to the NM.
        applicationID.setId(heartBeatID);
        appAttemptID.setApplicationId(applicationID);
        firstContainerID.setApplicationAttemptId(appAttemptID);
        firstContainerID.setId(heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        launchContext.setContainerId(firstContainerID);
        launchContext.setResource(recordFactory.newRecordInstance(Resource.class));
        launchContext.getResource().setMemory(2);
        Container container = new ContainerImpl(null, launchContext, null, null);
        this.context.getContainers().put(firstContainerID, container);
      } else if (heartBeatID == 2) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.getContainersStatuses().size());
        Assert.assertEquals("Number of container for the app should be one!",
            1, appToContainers.get(applicationID).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(1, activeContainers.size());

        // Give another container to the NM.
        applicationID.setId(heartBeatID);
        appAttemptID.setApplicationId(applicationID);
        secondContainerID.setApplicationAttemptId(appAttemptID);
        secondContainerID.setId(heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        launchContext.setContainerId(secondContainerID);
        launchContext.setResource(recordFactory.newRecordInstance(Resource.class));
        launchContext.getResource().setMemory(3);
        Container container = new ContainerImpl(null, launchContext, null, null);
        this.context.getContainers().put(secondContainerID, container);
      } else if (heartBeatID == 3) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            appToContainers.size());
        Assert.assertEquals("Number of container for the app should be two!",
            2, appToContainers.get(applicationID).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(2, activeContainers.size());
      }
      HeartbeatResponse response = recordFactory
          .newRecordInstance(HeartbeatResponse.class);
      response.setResponseId(heartBeatID);
      
      NodeHeartbeatResponse nhResponse = recordFactory
          .newRecordInstance(NodeHeartbeatResponse.class);
      nhResponse.setHeartbeatResponse(response);
      return nhResponse;
    }
  }

  private class MyNodeStatusUpdater extends NodeStatusUpdaterImpl {
    private Context context;

    public MyNodeStatusUpdater(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        ContainerTokenSecretManager containerTokenSecretManager) {
      super(context, dispatcher, healthChecker, metrics,
          containerTokenSecretManager);
      this.context = context;
    }

    @Override
    protected ResourceTracker getRMClient() {
      return new MyResourceTracker(this.context);
    }
  }

  @Before
  public void clearError() {
    nmStartError = null;
  }

  @After
  public void deleteBaseDir() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    lfs.delete(basedir, true);
  }

  @Test
  public void testNMRegistration() throws InterruptedException {
    final NodeManager nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker,
          ContainerTokenSecretManager containerTokenSecretManager) {
        return new MyNodeStatusUpdater(context, dispatcher, healthChecker,
                                       metrics, containerTokenSecretManager);
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);

    // verify that the last service is the nodeStatusUpdater (ie registration
    // with RM)
    Object[] services  = nm.getServices().toArray();
    Object lastService = services[services.length-1];
    Assert.assertTrue("last service is NOT the node status updater",
        lastService instanceof NodeStatusUpdater);

    new Thread() {
      public void run() {
        try {
          nm.start();
        } catch (Error e) {
          TestNodeStatusUpdater.this.nmStartError = e;
        }
      }
    }.start();

    System.out.println(" ----- thread already started.."
        + nm.getServiceState());

    int waitCount = 0;
    while (nm.getServiceState() == STATE.INITED && waitCount++ != 20) {
      LOG.info("Waiting for NM to start..");
      Thread.sleep(1000);
    }
    if (nmStartError != null) {
      throw nmStartError;
    }
    if (nm.getServiceState() != STATE.STARTED) {
      // NM could have failed.
      Assert.fail("NodeManager failed to start");
    }

    while (heartBeatID <= 3) {
      Thread.sleep(500);
    }
    Assert.assertEquals("Number of registered NMs is wrong!!", 1,
        this.registeredNodes.size());

    nm.stop();
  }

  /**
   * Verifies that if for some reason NM fails to start ContainerManager RPC
   * server, RM is oblivious to NM's presence. The behaviour is like this
   * because otherwise, NM will report to RM even if all its servers are not
   * started properly, RM will think that the NM is alive and will retire the NM
   * only after NM_EXPIRY interval. See MAPREDUCE-2749.
   */
  @Test
  public void testNoRegistrationWhenNMServicesFail() {

    final NodeManager nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker,
          ContainerTokenSecretManager containerTokenSecretManager) {
        return new MyNodeStatusUpdater(context, dispatcher, healthChecker,
                                       metrics, containerTokenSecretManager);
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ContainerTokenSecretManager containerTokenSecretManager) {
        return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
            metrics, containerTokenSecretManager) {
          @Override
          public void start() {
            // Simulating failure of starting RPC server
            throw new YarnException("Starting of RPC Server failed");
          }
        };
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    try {
      nm.start();
      Assert.fail("NM should have failed to start. Didn't get exception!!");
    } catch (Exception e) {
      Assert.assertEquals("Starting of RPC Server failed", e.getCause()
          .getMessage());
    }

    Assert.assertEquals("NM state is wrong!", Service.STATE.INITED, nm
        .getServiceState());

    Assert.assertEquals("Number of registered nodes is wrong!", 0,
        this.registeredNodes.size());
  }

  private YarnConfiguration createNMConfig() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_VMEM_GB, 5); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:12345");
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:12346");
    conf.set(YarnConfiguration.NM_LOG_DIRS, new Path(basedir, "logs").toUri()
        .getPath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, new Path(basedir,
        "remotelogs").toUri().getPath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, new Path(basedir, "nm0")
        .toUri().getPath());
    return conf;
  }
}
