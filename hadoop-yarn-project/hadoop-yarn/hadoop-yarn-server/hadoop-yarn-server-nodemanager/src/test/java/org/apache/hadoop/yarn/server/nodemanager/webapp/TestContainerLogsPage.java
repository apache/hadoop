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

import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsPage.ContainersLogsBlock;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Injector;
import com.google.inject.Module;

public class TestContainerLogsPage {

  @Test(timeout=30000)
  public void testContainerLogDirs() throws IOException, YarnException {
    File absLogDir = new File("target",
      TestNMWebServer.class.getSimpleName() + "LogDir").getAbsoluteFile();
    String logdirwithFile = absLogDir.toURI().toString();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logdirwithFile);
    NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler,
        new ApplicationACLsManager(conf), new NMNullStateStoreService());
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
    nmContext.getApplications().put(appId, app);

    MockContainer container =
        new MockContainer(appAttemptId, new AsyncDispatcher(), conf, user,
            appId, 1);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(container1, container);   
    List<File> files = null;
    files = ContainerLogsUtils.getContainerLogDirs(container1, user, nmContext);
    Assert.assertTrue(!(files.get(0).toString().contains("file:")));
    
    // After container is completed, it is removed from nmContext
    nmContext.getContainers().remove(container1);
    Assert.assertNull(nmContext.getContainers().get(container1));
    files = ContainerLogsUtils.getContainerLogDirs(container1, user, nmContext);
    Assert.assertTrue(!(files.get(0).toString().contains("file:")));
  }
  
  @Test(timeout = 10000)
  public void testContainerLogPageAccess() throws IOException {
    // SecureIOUtils require Native IO to be enabled. This test will run
    // only if it is enabled.
    assumeTrue(NativeIO.isAvailable());
    String user = "randomUser" + System.currentTimeMillis();
    File absLogDir = null, appDir = null, containerDir = null, syslog = null;
    try {
      // target log directory
      absLogDir =
          new File("target", TestContainerLogsPage.class.getSimpleName()
              + "LogDir").getAbsoluteFile();
      absLogDir.mkdir();

      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.NM_LOG_DIRS, absLogDir.toURI().toString());
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
      UserGroupInformation.setConfiguration(conf);

      NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
      healthChecker.init(conf);
      LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
      // Add an application and the corresponding containers
      RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(conf);
      long clusterTimeStamp = 1234;
      ApplicationId appId =
          BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp, 1);
      Application app = mock(Application.class);
      when(app.getAppId()).thenReturn(appId);

      // Making sure that application returns a random user. This is required
      // for SecureIOUtils' file owner check.
      when(app.getUser()).thenReturn(user);

      ApplicationAttemptId appAttemptId =
          BuilderUtils.newApplicationAttemptId(appId, 1);
      ContainerId container1 =
          BuilderUtils.newContainerId(recordFactory, appId, appAttemptId, 0);

      // Testing secure read access for log files

      // Creating application and container directory and syslog file.
      appDir = new File(absLogDir, appId.toString());
      appDir.mkdir();
      containerDir = new File(appDir, container1.toString());
      containerDir.mkdir();
      syslog = new File(containerDir, "syslog");
      syslog.createNewFile();
      BufferedOutputStream out =
          new BufferedOutputStream(new FileOutputStream(syslog));
      out.write("Log file Content".getBytes());
      out.close();

      Context context = mock(Context.class);
      ConcurrentMap<ApplicationId, Application> appMap =
          new ConcurrentHashMap<ApplicationId, Application>();
      appMap.put(appId, app);
      when(context.getApplications()).thenReturn(appMap);
      ConcurrentHashMap<ContainerId, Container> containers =
          new ConcurrentHashMap<ContainerId, Container>();
      when(context.getContainers()).thenReturn(containers);
      when(context.getLocalDirsHandler()).thenReturn(dirsHandler);

      MockContainer container = new MockContainer(appAttemptId,
        new AsyncDispatcher(), conf, user, appId, 1);
      container.setState(ContainerState.RUNNING);
      context.getContainers().put(container1, container);

      ContainersLogsBlock cLogsBlock =
          new ContainersLogsBlock(context);

      Map<String, String> params = new HashMap<String, String>();
      params.put(YarnWebParams.CONTAINER_ID, container1.toString());
      params.put(YarnWebParams.CONTAINER_LOG_TYPE, "syslog");

      Injector injector =
          WebAppTests.testPage(ContainerLogsPage.class,
            ContainersLogsBlock.class, cLogsBlock, params, (Module[])null);
      PrintWriter spyPw = WebAppTests.getPrintWriter(injector);
      verify(spyPw).write(
        "Exception reading log file. Application submitted by '" + user
            + "' doesn't own requested log file : syslog");
    } finally {
      if (syslog != null) {
        syslog.delete();
      }
      if (containerDir != null) {
        containerDir.delete();
      }
      if (appDir != null) {
        appDir.delete();
      }
      if (absLogDir != null) {
        absLogDir.delete();
      }
    }
  }
}
