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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
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

  private NodeHealthCheckerService createNodeHealthCheckerService(Configuration conf) {
    NodeHealthScriptRunner scriptRunner = NodeManager.getNodeHealthScriptRunner(conf);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    return new NodeHealthCheckerService(scriptRunner, dirsHandler);
  }

  @Test(timeout=30000)
  public void testContainerLogDirs() throws IOException, YarnException {
    File absLogDir = new File("target",
      TestNMWebServer.class.getSimpleName() + "LogDir").getAbsoluteFile();
    String logdirwithFile = absLogDir.toURI().toString();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logdirwithFile);
    NodeHealthCheckerService healthChecker = createNodeHealthCheckerService(conf);
    healthChecker.init(conf);
    LocalDirsHandlerService dirsHandler = healthChecker.getDiskHandler();
    NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler,
        new ApplicationACLsManager(conf), new NMNullStateStoreService(), false,
            conf);
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

    // Create a new context to check if correct container log dirs are fetched
    // on full disk.
    LocalDirsHandlerService dirsHandlerForFullDisk = spy(dirsHandler);
    // good log dirs are empty and nm log dir is in the full log dir list.
    when(dirsHandlerForFullDisk.getLogDirs()).
        thenReturn(new ArrayList<String>());
    when(dirsHandlerForFullDisk.getLogDirsForRead()).
        thenReturn(Arrays.asList(new String[] {absLogDir.getAbsolutePath()}));
    nmContext = new NodeManager.NMContext(null, null, dirsHandlerForFullDisk,
        new ApplicationACLsManager(conf), new NMNullStateStoreService(), false,
        conf);
    nmContext.getApplications().put(appId, app);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(container1, container);
    List<File> dirs =
        ContainerLogsUtils.getContainerLogDirs(container1, user, nmContext);
    File containerLogDir = new File(absLogDir, appId + "/" + container1);
    Assert.assertTrue(dirs.contains(containerLogDir));
  }

  @Test(timeout=30000)
  public void testContainerLogFile() throws IOException, YarnException {
    File absLogDir = new File("target",
        TestNMWebServer.class.getSimpleName() + "LogDir").getAbsoluteFile();
    String logdirwithFile = absLogDir.toURI().toString();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LOG_DIRS, logdirwithFile);
    conf.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
        0.0f);
    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    NMContext nmContext = new NodeManager.NMContext(null, null, dirsHandler,
        new ApplicationACLsManager(conf), new NMNullStateStoreService(), false,
        conf);
    // Add an application and the corresponding containers
    String user = "nobody";
    long clusterTimeStamp = 1234;
    ApplicationId appId = BuilderUtils.newApplicationId(
        clusterTimeStamp, 1);
    Application app = mock(Application.class);
    when(app.getUser()).thenReturn(user);
    when(app.getAppId()).thenReturn(appId);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(
        appAttemptId, 1);
    nmContext.getApplications().put(appId, app);

    MockContainer container =
        new MockContainer(appAttemptId, new AsyncDispatcher(), conf, user,
            appId, 1);
    container.setState(ContainerState.RUNNING);
    nmContext.getContainers().put(containerId, container);
    File containerLogDir = new File(absLogDir,
        ContainerLaunch.getRelativeContainerLogDir(appId.toString(),
            containerId.toString()));
    containerLogDir.mkdirs();
    String fileName = "fileName";
    File containerLogFile = new File(containerLogDir, fileName);
    containerLogFile.createNewFile();
    File file = ContainerLogsUtils.getContainerLogFile(containerId,
        fileName, user, nmContext);
    Assert.assertEquals(containerLogFile.toURI().toString(),
        file.toURI().toString());
    FileUtil.fullyDelete(absLogDir);
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

      NodeHealthCheckerService healthChecker = createNodeHealthCheckerService(conf);
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
      when(context.getConf()).thenReturn(conf);

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
  
  @Test
  public void testLogDirWithDriveLetter() throws Exception {
    //To verify that logs paths which include drive letters (Windows)
    //do not lose their drive letter specification
    LocalDirsHandlerService localDirs = mock(LocalDirsHandlerService.class);
    List<String> logDirs = new ArrayList<String>();
    logDirs.add("F:/nmlogs");
    when(localDirs.getLogDirsForRead()).thenReturn(logDirs);
    
    ApplicationIdPBImpl appId = mock(ApplicationIdPBImpl.class);
    when(appId.toString()).thenReturn("app_id_1");
    
    ApplicationAttemptIdPBImpl appAttemptId =
               mock(ApplicationAttemptIdPBImpl.class);
    when(appAttemptId.getApplicationId()).thenReturn(appId);
    
    ContainerId containerId = mock(ContainerIdPBImpl.class);
    when(containerId.getApplicationAttemptId()).thenReturn(appAttemptId);
    
    List<File> logDirFiles = ContainerLogsUtils.getContainerLogDirs(
      containerId, localDirs);
    
    Assert.assertTrue("logDir lost drive letter " +
      logDirFiles.get(0),
      logDirFiles.get(0).toString().indexOf("F:" + File.separator +
        "nmlogs") > -1);
  }
  
  @Test
  public void testLogFileWithDriveLetter() throws Exception {
    
    ContainerImpl container = mock(ContainerImpl.class);
    
    ApplicationIdPBImpl appId = mock(ApplicationIdPBImpl.class);
    when(appId.toString()).thenReturn("appId");
    
    Application app = mock(Application.class);
    when(app.getAppId()).thenReturn(appId);
    
    ApplicationAttemptIdPBImpl appAttemptId =
               mock(ApplicationAttemptIdPBImpl.class);
    when(appAttemptId.getApplicationId()).thenReturn(appId); 
    
    ConcurrentMap<ApplicationId, Application> applications = 
      new ConcurrentHashMap<ApplicationId, Application>();
    applications.put(appId, app);
    
    ContainerId containerId = mock(ContainerIdPBImpl.class);
    when(containerId.toString()).thenReturn("containerId");
    when(containerId.getApplicationAttemptId()).thenReturn(appAttemptId);
    
    ConcurrentMap<ContainerId, Container> containers = 
      new ConcurrentHashMap<ContainerId, Container>();
    
    containers.put(containerId, container);
    
    LocalDirsHandlerService localDirs = mock(LocalDirsHandlerService.class);
    when(localDirs.getLogPathToRead("appId" + Path.SEPARATOR + "containerId" +
      Path.SEPARATOR + "fileName"))
      .thenReturn(new Path("F:/nmlogs/appId/containerId/fileName"));
    
    NMContext context = mock(NMContext.class);
    when(context.getLocalDirsHandler()).thenReturn(localDirs);
    when(context.getApplications()).thenReturn(applications);
    when(context.getContainers()).thenReturn(containers);
    
    File logFile = ContainerLogsUtils.getContainerLogFile(containerId,
      "fileName", null, context);
      
    Assert.assertTrue("logFile lost drive letter " +
      logFile,
      logFile.toString().indexOf("F:" + File.separator + "nmlogs") > -1);
    
  }
  
}
