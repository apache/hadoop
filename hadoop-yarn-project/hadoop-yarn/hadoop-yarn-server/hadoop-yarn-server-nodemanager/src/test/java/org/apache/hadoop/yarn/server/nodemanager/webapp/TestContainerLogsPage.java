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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestContainerLogsPage {

  @Test
  public void testContainerLogDirs() throws IOException {
    String absLogDir = new File("target",
        TestNMWebServer.class.getSimpleName() + "LogDir").getAbsolutePath();
    String logdirwithFile = "file://" + absLogDir;
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logdirwithFile);
    NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    // Add an application and the corresponding containers
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    String user = "nobody";
    long clusterTimeStamp = 1234;
    ApplicationId appId = BuilderUtils.newApplicationId(recordFactory,
        clusterTimeStamp, 1);
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId container1 = BuilderUtils.newContainerId(recordFactory, appId,
        appAttemptId, 0);
    List<File> files = null;
    files = ContainerLogsPage.ContainersLogsBlock.getContainerLogDirs(
        container1, dirsHandler);
    Assert.assertTrue(!(files.get(0).toString().contains("file:")));
  }
}
