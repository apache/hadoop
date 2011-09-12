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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.junit.Test;

public class TestEventFlow {

  private static final Log LOG = LogFactory.getLog(TestEventFlow.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private static File localDir = new File("target",
      TestEventFlow.class.getName() + "-localDir").getAbsoluteFile();
  private static File localLogDir = new File("target",
      TestEventFlow.class.getName() + "-localLogDir").getAbsoluteFile();
  private static File remoteLogDir = new File("target",
      TestEventFlow.class.getName() + "-remoteLogDir").getAbsoluteFile();

  @Test
  public void testSuccessfulContainerLaunch() throws InterruptedException,
      IOException {

    FileContext localFS = FileContext.getLocalFSFileContext();

    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(localLogDir.getAbsolutePath()), true);
    localFS.delete(new Path(remoteLogDir.getAbsolutePath()), true);
    localDir.mkdir();
    localLogDir.mkdir();
    remoteLogDir.mkdir();

    Context context = new NMContext();

    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogDir.getAbsolutePath());

    ContainerExecutor exec = new DefaultContainerExecutor();
    exec.setConf(conf);
    DeletionService del = new DeletionService(exec);
    Dispatcher dispatcher = new AsyncDispatcher();
    NodeHealthCheckerService healthChecker = null;
    NodeManagerMetrics metrics = NodeManagerMetrics.create();
    ContainerTokenSecretManager containerTokenSecretManager =  new ContainerTokenSecretManager();
    NodeStatusUpdater nodeStatusUpdater =
        new NodeStatusUpdaterImpl(context, dispatcher, healthChecker, metrics, containerTokenSecretManager) {
      @Override
      protected ResourceTracker getRMClient() {
        return new LocalRMInterface();
      };

      @Override
      protected void startStatusUpdater() {
        return; // Don't start any updating thread.
      }
    };

    DummyContainerManager containerManager =
        new DummyContainerManager(context, exec, del, nodeStatusUpdater, metrics, containerTokenSecretManager);
    containerManager.init(conf);
    containerManager.start();

    ContainerLaunchContext launchContext = recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ContainerId cID = recordFactory.newRecordInstance(ContainerId.class);
    cID.setAppId(recordFactory.newRecordInstance(ApplicationId.class));
    ApplicationAttemptId atId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    atId.setApplicationId(cID.getAppId());
    cID.setAppAttemptId(atId);
    launchContext.setContainerId(cID);
    launchContext.setUser("testing");
    launchContext.setResource(recordFactory.newRecordInstance(Resource.class));
    StartContainerRequest request = recordFactory.newRecordInstance(StartContainerRequest.class);
    request.setContainerLaunchContext(launchContext);
    containerManager.startContainer(request);

    BaseContainerManagerTest.waitForContainerState(containerManager, cID,
        ContainerState.RUNNING);

    StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
    stopRequest.setContainerId(cID);
    containerManager.stopContainer(stopRequest);
    BaseContainerManagerTest.waitForContainerState(containerManager, cID,
        ContainerState.COMPLETE);

    containerManager.stop();
  }
}
