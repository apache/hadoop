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

import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.junit.Test;


public class TestEventFlow {

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private static File localDir = new File("target",
      TestEventFlow.class.getName() + "-localDir").getAbsoluteFile();
  private static File localLogDir = new File("target",
      TestEventFlow.class.getName() + "-localLogDir").getAbsoluteFile();
  private static File remoteLogDir = new File("target",
      TestEventFlow.class.getName() + "-remoteLogDir").getAbsoluteFile();
  private static final long SIMULATED_RM_IDENTIFIER = 1234;

  @Test
  public void testSuccessfulContainerLaunch() throws InterruptedException,
      IOException, YarnException {

    FileContext localFS = FileContext.getLocalFSFileContext();

    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(localLogDir.getAbsolutePath()), true);
    localFS.delete(new Path(remoteLogDir.getAbsolutePath()), true);
    localDir.mkdir();
    localLogDir.mkdir();
    remoteLogDir.mkdir();

    YarnConfiguration conf = new YarnConfiguration();
    
    Context context = new NMContext(new NMContainerTokenSecretManager(conf),
        new NMTokenSecretManagerInNM(), null, null,
        new NMNullStateStoreService(), false, conf) {
      @Override
      public int getHttpPort() {
        return 1234;
      }
    };

    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, 
        remoteLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "0.0.0.0:"
        + ServerSocketUtil.getPort(8040, 10));

    ContainerExecutor exec = new DefaultContainerExecutor();
    exec.setConf(conf);

    DeletionService del = new DeletionService(exec);
    Dispatcher dispatcher = new AsyncDispatcher();
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    NodeHealthCheckerService healthChecker =
        new NodeHealthCheckerService(dirsHandler);
    healthChecker.init(conf);
    NodeManagerMetrics metrics = NodeManagerMetrics.create();
    NodeStatusUpdater nodeStatusUpdater =
        new NodeStatusUpdaterImpl(context, dispatcher, healthChecker, metrics) {
      @Override
      protected ResourceTracker getRMClient() {
        return new LocalRMInterface();
      };

          @Override
          protected void stopRMProxy() {
            return;
          }

      @Override
      protected void startStatusUpdater() {
        return; // Don't start any updating thread.
      }

      @Override
      public long getRMIdentifier() {
        return SIMULATED_RM_IDENTIFIER;
      }
    };

    DummyContainerManager containerManager =
        new DummyContainerManager(context, exec, del, nodeStatusUpdater,
          metrics, dirsHandler);
    nodeStatusUpdater.init(conf);
    NodeResourceMonitorImpl nodeResourceMonitor = mock(
        NodeResourceMonitorImpl.class);
    ((NMContext) context).setNodeResourceMonitor(nodeResourceMonitor);
    ((NMContext)context).setContainerManager(containerManager);
    nodeStatusUpdater.start();
    ((NMContext)context).setNodeStatusUpdater(nodeStatusUpdater);
    containerManager.init(conf);
    containerManager.start();

    ContainerLaunchContext launchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ApplicationId applicationId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 0);
    ContainerId cID = ContainerId.newContainerId(applicationAttemptId, 0);

    String user = "testing";
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(launchContext,
          TestContainerManager.createContainerToken(cID,
            SIMULATED_RM_IDENTIFIER, context.getNodeId(), user,
            context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForContainerState(containerManager, cID,
        Arrays.asList(ContainerState.RUNNING), 20);

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cID);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    containerManager.stopContainers(stopRequest);
    BaseContainerManagerTest.waitForContainerState(containerManager, cID,
        ContainerState.COMPLETE);

    containerManager.stop();
  }
}
