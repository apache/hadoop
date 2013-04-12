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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import static org.mockito.Mockito.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mortbay.util.MultiException;


//@Ignore
public class TestLogAggregationService extends BaseContainerManagerTest {

  private Map<ApplicationAccessType, String> acls = createAppAcls();
  
  static {
    LOG = LogFactory.getLog(TestLogAggregationService.class);
  }

  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private File remoteRootLogDir = new File("target", this.getClass()
      .getName() + "-remoteLogDir");

  public TestLogAggregationService() throws UnsupportedFileSystemException {
    super();
    this.remoteRootLogDir.mkdir();
  }

  @Override
  public void tearDown() throws IOException, InterruptedException {
    super.tearDown();
    createContainerExecutor().deleteAsUser(user,
        new Path(remoteRootLogDir.getAbsolutePath()), new Path[] {});
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLocalFileDeletionAfterUpload() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    logAggregationService.start();

    
    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
        new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogHandlerAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));

    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(application1, 1);
    ContainerId container11 = BuilderUtils.newContainerId(appAttemptId, 1);
    // Simulate log-file creation
    writeContainerLogs(app1LogDir, container11);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container11, 0));

    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application1));

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
    // ensure filesystems were closed
    verify(logAggregationService).closeFileSystems(
        any(UserGroupInformation.class));
    
    delSrvc.stop();
    
    String containerIdStr = ConverterUtils.toString(container11);
    File containerLogDir = new File(app1LogDir, containerIdStr);
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      File f = new File(containerLogDir, fileType);
      Assert.assertFalse("check "+f, f.exists());
    }

    Assert.assertFalse(app1LogDir.exists());

    Path logFilePath =
        logAggregationService.getRemoteNodeLogFileForApp(application1,
            this.user);
    Assert.assertTrue("Log file [" + logFilePath + "] not found", new File(
        logFilePath.toUri().getPath()).exists());
    
    dispatcher.await();
    
    ApplicationEvent expectedEvents[] = new ApplicationEvent[]{
        new ApplicationEvent(
            appAttemptId.getApplicationId(),
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(
            appAttemptId.getApplicationId(),
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED)
    };

    checkEvents(appEventHandler, expectedEvents, true, "getType", "getApplicationID");
    dispatcher.stop();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoContainerOnNode() throws Exception {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    
    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
      new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogHandlerAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));

    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application1));

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());

    Assert.assertFalse(new File(logAggregationService
        .getRemoteNodeLogFileForApp(application1, this.user).toUri().getPath())
        .exists());
    
    dispatcher.await();
    
    ApplicationEvent expectedEvents[] = new ApplicationEvent[]{
        new ApplicationEvent(
            application1,
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(
            application1,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED)
    };
    checkEvents(appEventHandler, expectedEvents, true, "getType", "getApplicationID");
    dispatcher.stop();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleAppsLogAggregation() throws Exception {

    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    
    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
      new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogHandlerAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));

    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(application1, 1);
    ContainerId container11 = BuilderUtils.newContainerId(appAttemptId1, 1);
    
    // Simulate log-file creation
    writeContainerLogs(app1LogDir, container11);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container11, 0));

    ApplicationId application2 = BuilderUtils.newApplicationId(1234, 2);
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(application2, 1);

    File app2LogDir =
      new File(localLogDir, ConverterUtils.toString(application2));
    app2LogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(
        application2, this.user, null,
        ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY, this.acls));

    
    ContainerId container21 = BuilderUtils.newContainerId(appAttemptId2, 1);
    
    writeContainerLogs(app2LogDir, container21);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container21, 0));

    ContainerId container12 = BuilderUtils.newContainerId(appAttemptId1, 2);

    writeContainerLogs(app1LogDir, container12);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container12, 0));

    ApplicationId application3 = BuilderUtils.newApplicationId(1234, 3);
    ApplicationAttemptId appAttemptId3 =
        BuilderUtils.newApplicationAttemptId(application3, 1);

    File app3LogDir =
      new File(localLogDir, ConverterUtils.toString(application3));
    app3LogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application3,
        this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY, this.acls));        

    dispatcher.await();
    ApplicationEvent expectedInitEvents[] = new ApplicationEvent[]{
        new ApplicationEvent(
            application1,
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(
            application2,
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(
            application3,
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED)
    };
    checkEvents(appEventHandler, expectedInitEvents, false, "getType", "getApplicationID");
    reset(appEventHandler);
    
    ContainerId container31 = BuilderUtils.newContainerId(appAttemptId3, 1);
    writeContainerLogs(app3LogDir, container31);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container31, 0));

    ContainerId container32 = BuilderUtils.newContainerId(appAttemptId3, 2);
    writeContainerLogs(app3LogDir, container32);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container32, 1)); // Failed 

    ContainerId container22 = BuilderUtils.newContainerId(appAttemptId2, 2);
    writeContainerLogs(app2LogDir, container22);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container22, 0));

    ContainerId container33 = BuilderUtils.newContainerId(appAttemptId3, 3);
    writeContainerLogs(app3LogDir, container33);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container33, 0));

    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application2));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application3));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application1));

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());

    verifyContainerLogs(logAggregationService, application1,
        new ContainerId[] { container11, container12 });
    verifyContainerLogs(logAggregationService, application2,
        new ContainerId[] { container21 });
    verifyContainerLogs(logAggregationService, application3,
        new ContainerId[] { container31, container32 });
    
    dispatcher.await();
    
    ApplicationEvent[] expectedFinishedEvents = new ApplicationEvent[]{
        new ApplicationEvent(
            application1,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED),
        new ApplicationEvent(
            application2,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED),
        new ApplicationEvent(
            application3,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED)
    };
    checkEvents(appEventHandler, expectedFinishedEvents, false, "getType", "getApplicationID");
    dispatcher.stop();
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testLogAggregationInitFailsWithoutKillingNM() throws Exception {

    this.conf.set(YarnConfiguration.NM_LOG_DIRS,
        localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);

    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId appId = BuilderUtils.newApplicationId(
        System.currentTimeMillis(), (int)Math.random());
    doThrow(new YarnException("KABOOM!"))
      .when(logAggregationService).initAppAggregator(
          eq(appId), eq(user), any(Credentials.class),
          any(ContainerLogsRetentionPolicy.class), anyMap());

    logAggregationService.handle(new LogHandlerAppStartedEvent(appId,
        this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
        this.acls));

    dispatcher.await();
    ApplicationEvent expectedEvents[] = new ApplicationEvent[]{
        new ApplicationEvent(appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED)
    };
    checkEvents(appEventHandler, expectedEvents, false,
        "getType", "getApplicationID", "getDiagnostic");
    // no filesystems instantiated yet
    verify(logAggregationService, never()).closeFileSystems(
        any(UserGroupInformation.class));

    // verify trying to collect logs for containers/apps we don't know about
    // doesn't blow up and tear down the NM
    logAggregationService.handle(new LogHandlerContainerFinishedEvent(
        BuilderUtils.newContainerId(4, 1, 1, 1), 0));
    dispatcher.await();
    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        BuilderUtils.newApplicationId(1, 5)));
    dispatcher.await();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLogAggregationCreateDirsFailsWithoutKillingNM()
      throws Exception {
    
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    logAggregationService.start();
    
    ApplicationId appId = BuilderUtils.newApplicationId(
        System.currentTimeMillis(), (int)Math.random());
    Exception e = new RuntimeException("KABOOM!");
    doThrow(e)
      .when(logAggregationService).createAppDir(any(String.class),
          any(ApplicationId.class), any(UserGroupInformation.class));
    logAggregationService.handle(new LogHandlerAppStartedEvent(appId,
        this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY, this.acls));        
    
    dispatcher.await();
    ApplicationEvent expectedEvents[] = new ApplicationEvent[]{
        new ApplicationEvent(appId, 
        		ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED)
    };
    checkEvents(appEventHandler, expectedEvents, false,
        "getType", "getApplicationID", "getDiagnostic");
    // filesystems may have been instantiated
    verify(logAggregationService).closeFileSystems(
        any(UserGroupInformation.class));

    // verify trying to collect logs for containers/apps we don't know about
    // doesn't blow up and tear down the NM
    logAggregationService.handle(new LogHandlerContainerFinishedEvent(
        BuilderUtils.newContainerId(4, 1, 1, 1), 0));
    dispatcher.await();
    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        BuilderUtils.newApplicationId(1, 5)));
    dispatcher.await();

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
  }

  private void writeContainerLogs(File appLogDir, ContainerId containerId)
      throws IOException {
    // ContainerLogDir should be created
    String containerStr = ConverterUtils.toString(containerId);
    File containerLogDir = new File(appLogDir, containerStr);
    containerLogDir.mkdir();
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      Writer writer11 = new FileWriter(new File(containerLogDir, fileType));
      writer11.write(containerStr + " Hello " + fileType + "!");
      writer11.close();
    }
  }

  private void verifyContainerLogs(
      LogAggregationService logAggregationService, ApplicationId appId,
      ContainerId[] expectedContainerIds) throws IOException {
    AggregatedLogFormat.LogReader reader =
        new AggregatedLogFormat.LogReader(this.conf,
            logAggregationService.getRemoteNodeLogFileForApp(appId, this.user));
    
    Assert.assertEquals(this.user, reader.getApplicationOwner());
    verifyAcls(reader.getApplicationAcls());
    
    try {
      Map<String, Map<String, String>> logMap =
          new HashMap<String, Map<String, String>>();
      DataInputStream valueStream;

      LogKey key = new LogKey();
      valueStream = reader.next(key);

      while (valueStream != null) {
        LOG.info("Found container " + key.toString());
        Map<String, String> perContainerMap = new HashMap<String, String>();
        logMap.put(key.toString(), perContainerMap);

        while (true) {
          try {
            DataOutputBuffer dob = new DataOutputBuffer();
            LogReader.readAContainerLogsForALogType(valueStream, dob);

            DataInputBuffer dib = new DataInputBuffer();
            dib.reset(dob.getData(), dob.getLength());

            Assert.assertEquals("\nLogType:", dib.readUTF());
            String fileType = dib.readUTF();

            Assert.assertEquals("\nLogLength:", dib.readUTF());
            String fileLengthStr = dib.readUTF();
            long fileLength = Long.parseLong(fileLengthStr);

            Assert.assertEquals("\nLog Contents:\n", dib.readUTF());
            byte[] buf = new byte[(int) fileLength]; // cast is okay in this
                                                     // test.
            dib.read(buf, 0, (int) fileLength);
            perContainerMap.put(fileType, new String(buf));

            LOG.info("LogType:" + fileType);
            LOG.info("LogType:" + fileLength);
            LOG.info("Log Contents:\n" + perContainerMap.get(fileType));
          } catch (EOFException eof) {
            break;
          }
        }

        // Next container
        key = new LogKey();
        valueStream = reader.next(key);
      }

      // 1 for each container
      Assert.assertEquals(expectedContainerIds.length, logMap.size());
      for (ContainerId cId : expectedContainerIds) {
        String containerStr = ConverterUtils.toString(cId);
        Map<String, String> thisContainerMap = logMap.remove(containerStr);
        Assert.assertEquals(3, thisContainerMap.size());
        for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
          String expectedValue = containerStr + " Hello " + fileType + "!";
          LOG.info("Expected log-content : " + new String(expectedValue));
          String foundValue = thisContainerMap.remove(fileType);
          Assert.assertNotNull(cId + " " + fileType
              + " not present in aggregated log-file!", foundValue);
          Assert.assertEquals(expectedValue, foundValue);
        }
        Assert.assertEquals(0, thisContainerMap.size());
      }
      Assert.assertEquals(0, logMap.size());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLogAggregationForRealContainerLaunch() throws IOException,
      InterruptedException {

    this.containerManager.start();


    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    fileWriter.write("\necho Hello World! Stdout! > "
        + new File(localLogDir, "stdout"));
    fileWriter.write("\necho Hello World! Stderr! > "
        + new File(localLogDir, "stderr"));
    fileWriter.write("\necho Hello World! Syslog! > "
        + new File(localLogDir, "syslog"));
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // ////// Construct the Container-id
    ApplicationId appId =
        recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(0);
    appId.setId(0);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, 0);

    containerLaunchContext.setContainerId(cId);

    containerLaunchContext.setUser(this.user);

    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/bash");
    commands.add(scriptFile.getAbsolutePath());
    containerLaunchContext.setCommands(commands);
    containerLaunchContext.setResource(recordFactory
        .newRecordInstance(Resource.class));
    containerLaunchContext.getResource().setMemory(100 * 1024 * 1024);
    StartContainerRequest startRequest =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    this.containerManager.startContainer(startRequest);

    BaseContainerManagerTest.waitForContainerState(this.containerManager,
        cId, ContainerState.COMPLETE);

    this.containerManager.handle(new CMgrCompletedAppsEvent(Arrays
        .asList(appId)));
    this.containerManager.stop();
  }

  private void verifyAcls(Map<ApplicationAccessType, String> logAcls) {
    Assert.assertEquals(this.acls.size(), logAcls.size());
    for (ApplicationAccessType appAccessType : this.acls.keySet()) {
      Assert.assertEquals(this.acls.get(appAccessType),
          logAcls.get(appAccessType));
    }
  }

  private DrainDispatcher createDispatcher() {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(this.conf);
    dispatcher.start();
    return dispatcher;
  }
  
  private Map<ApplicationAccessType, String> createAppAcls() {
    Map<ApplicationAccessType, String> appAcls =
        new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.MODIFY_APP, "user group");
    appAcls.put(ApplicationAccessType.VIEW_APP, "*");
    return appAcls;
  }

  @Test(timeout=20000)
  @SuppressWarnings("unchecked")
  public void testStopAfterError() throws Exception {
    DeletionService delSrvc = mock(DeletionService.class);

    // get the AppLogAggregationImpl thread to crash
    LocalDirsHandlerService mockedDirSvc = mock(LocalDirsHandlerService.class);
    when(mockedDirSvc.getLogDirs()).thenThrow(new RuntimeException());

    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);

    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, delSrvc,
                                  mockedDirSvc);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);
    logAggregationService.handle(new LogHandlerAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLogAggregatorCleanup() throws Exception {
    DeletionService delSrvc = mock(DeletionService.class);

    // get the AppLogAggregationImpl thread to crash
    LocalDirsHandlerService mockedDirSvc = mock(LocalDirsHandlerService.class);

    DrainDispatcher dispatcher = createDispatcher();
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);

    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, delSrvc,
                                  mockedDirSvc);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);
    logAggregationService.handle(new LogHandlerAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));

    logAggregationService.handle(new LogHandlerAppFinishedEvent(application1));
    dispatcher.await();
    int timeToWait = 20 * 1000;
    while (timeToWait > 0 && logAggregationService.getNumAggregators() > 0) {
      Thread.sleep(100);
      timeToWait -= 100;
    }
    Assert.assertEquals("Log aggregator failed to cleanup!", 0,
        logAggregationService.getNumAggregators());
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends Event<?>>
  void checkEvents(EventHandler<T> eventHandler,
                   T expectedEvents[], boolean inOrder,
                   String... methods) throws Exception {
    Class<T> genericClass = (Class<T>)expectedEvents.getClass().getComponentType();
    ArgumentCaptor<T> eventCaptor = ArgumentCaptor.forClass(genericClass);
    // captor work work unless used via a verify
    verify(eventHandler, atLeast(0)).handle(eventCaptor.capture());
    List<T> actualEvents = eventCaptor.getAllValues();

    // batch up exceptions so junit presents them as one
    MultiException failures = new MultiException();
    try {
      assertEquals("expected events", expectedEvents.length, actualEvents.size());
    } catch (Throwable e) {
      failures.add(e);
    }
    if (inOrder) {
    	// sequentially verify the events
      int len = Math.max(expectedEvents.length, actualEvents.size());
      for (int n=0; n < len; n++) {
        try {
          String expect = (n < expectedEvents.length)
              ? eventToString(expectedEvents[n], methods) : null;
          String actual = (n < actualEvents.size())
              ? eventToString(actualEvents.get(n), methods) : null;
          assertEquals("event#"+n, expect, actual);
        } catch (Throwable e) {
          failures.add(e);
        }
      }
    } else {
    	// verify the actual events were expected
    	// verify no expected events were not seen
      Set<String> expectedSet = new HashSet<String>();
      for (T expectedEvent : expectedEvents) {
        expectedSet.add(eventToString(expectedEvent, methods));
      }
      for (T actualEvent : actualEvents) {
        try {
          String actual = eventToString(actualEvent, methods);
          assertTrue("unexpected event: "+actual, expectedSet.remove(actual));
        } catch (Throwable e) {
          failures.add(e);
        }
      }
      for (String expected : expectedSet) {
        try {
          Assert.fail("missing event: "+expected);
        } catch (Throwable e) {
          failures.add(e);
        }
      }
    }
    failures.ifExceptionThrow();
  }
  
  private static String eventToString(Event<?> event, String[] methods) throws Exception {
    StringBuilder sb = new StringBuilder("[ ");
    for (String m : methods) {
      try {
      	Method method = event.getClass().getMethod(m);
        String value = method.invoke(event).toString();
        sb.append(method.getName()).append("=").append(value).append(" ");
      } catch (Exception e) {
        // ignore, actual event may not implement the method...
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
