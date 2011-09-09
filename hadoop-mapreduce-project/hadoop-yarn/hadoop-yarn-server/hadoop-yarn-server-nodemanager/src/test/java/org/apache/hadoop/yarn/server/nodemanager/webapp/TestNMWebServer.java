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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestNMWebServer {

  private static final File testRootDir = new File("target",
      TestNMWebServer.class.getSimpleName());

  @Before
  public void setup() {
    testRootDir.mkdirs();
  }

  @Test
  public void testNMWebApp() throws InterruptedException, IOException {
    Context nmContext = new NodeManager.NMContext();
    ResourceView resourceView = new ResourceView() {
      @Override
      public long getVmemAllocatedForContainers() {
        return 0;
      }
      @Override
      public long getPmemAllocatedForContainers() {
        return 0;
      }
    };
    WebServer server = new WebServer(nmContext, resourceView);
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, testRootDir.getAbsolutePath());
    server.init(conf);
    server.start();

    // Add an application and the corresponding containers
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(conf);
    Dispatcher dispatcher = new AsyncDispatcher();
    String user = "nobody";
    long clusterTimeStamp = 1234;
    ApplicationId appId =
        BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp, 1);
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    nmContext.getApplications().put(appId, app);
    ApplicationAttemptId appAttemptId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(1);
    ContainerId container1 =
        BuilderUtils.newContainerId(recordFactory, appId, appAttemptId, 0);
    ContainerId container2 =
        BuilderUtils.newContainerId(recordFactory, appId, appAttemptId, 1);
    NodeManagerMetrics metrics = mock(NodeManagerMetrics.class);
    for (ContainerId containerId : new ContainerId[] { container1,
        container2}) {
      // TODO: Use builder utils
      ContainerLaunchContext launchContext =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      launchContext.setContainerId(containerId);
      launchContext.setUser(user);
      Container container =
          new ContainerImpl(dispatcher, launchContext, null, metrics) {
        @Override
        public ContainerState getContainerState() {
          return ContainerState.RUNNING;
        };
      };
      nmContext.getContainers().put(containerId, container);
      //TODO: Gross hack. Fix in code.
      nmContext.getApplications().get(containerId.getAppId()).getContainers()
          .put(containerId, container);
      writeContainerLogs(conf, nmContext, containerId);

    }
    // TODO: Pull logs and test contents.
//    Thread.sleep(1000000);
  }

  private void writeContainerLogs(Configuration conf, Context nmContext,
      ContainerId containerId)
        throws IOException {
    // ContainerLogDir should be created
    File containerLogDir =
        ContainerLogsPage.ContainersLogsBlock.getContainerLogDirs(conf,
            containerId).get(0);
    containerLogDir.mkdirs();
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      Writer writer = new FileWriter(new File(containerLogDir, fileType));
      writer.write(ConverterUtils.toString(containerId) + "\n Hello "
          + fileType + "!");
      writer.close();
    }
  }
}
