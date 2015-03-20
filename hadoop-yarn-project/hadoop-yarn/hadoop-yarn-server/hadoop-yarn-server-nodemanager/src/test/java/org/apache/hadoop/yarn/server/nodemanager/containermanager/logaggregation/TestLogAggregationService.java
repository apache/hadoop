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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.TestNonAggregatingLogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mortbay.util.MultiException;

import com.google.common.base.Supplier;

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
  
  DrainDispatcher dispatcher;
  EventHandler<ApplicationEvent> appEventHandler;

  @Override
  @SuppressWarnings("unchecked")
  public void setup() throws IOException {
    super.setup();
    NodeId nodeId = NodeId.newInstance("0.0.0.0", 5555);
    ((NMContext)context).setNodeId(nodeId);
    dispatcher = createDispatcher();
    appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    UserGroupInformation.setConfiguration(conf);
  }

  @Override
  public void tearDown() throws IOException, InterruptedException {
    super.tearDown();
    createContainerExecutor().deleteAsUser(user,
        new Path(remoteRootLogDir.getAbsolutePath()), new Path[] {});
    dispatcher.await();
    dispatcher.stop();
    dispatcher.close();
  }

  private void verifyLocalFileDeletion(
      LogAggregationService logAggregationService) throws Exception {
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
    writeContainerLogs(app1LogDir, container11, new String[] { "stdout",
        "stderr", "syslog" });
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container11, 0));

    logAggregationService.handle(new LogHandlerAppFinishedEvent(
        application1));

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
    // ensure filesystems were closed
    verify(logAggregationService).closeFileSystems(
        any(UserGroupInformation.class));
    verify(delSrvc).delete(eq(user), eq((Path) null),
      eq(new Path(app1LogDir.getAbsolutePath())));
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

    checkEvents(appEventHandler, expectedEvents, true, "getType",
        "getApplicationID");
  }

  @Test
  public void testLocalFileDeletionAfterUpload() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    verifyLocalFileDeletion(logAggregationService);
  }

  @Test
  public void testLocalFileDeletionOnDiskFull() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    List<String> logDirs = super.dirsHandler.getLogDirs();
    LocalDirsHandlerService dirsHandler = spy(super.dirsHandler);
    // Simulate disk being full by returning no good log dirs but having a
    // directory in full log dirs.
    when(dirsHandler.getLogDirs()).thenReturn(new ArrayList<String>());
    when(dirsHandler.getLogDirsForRead()).thenReturn(logDirs);
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
            dirsHandler));
    verifyLocalFileDeletion(logAggregationService);
  }

  /* Test to verify fix for YARN-3793 */
  @Test
  public void testNoLogsUploadedOnAppFinish() throws Exception {
    this.delSrvc = new DeletionService(createContainerExecutor());
    delSrvc = spy(delSrvc);
    this.delSrvc.init(conf);
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    LogAggregationService logAggregationService = new LogAggregationService(
        dispatcher, this.context, this.delSrvc, super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId app = BuilderUtils.newApplicationId(1234, 1);
    File appLogDir = new File(localLogDir, ConverterUtils.toString(app));
    appLogDir.mkdir();
    LogAggregationContext context =
        LogAggregationContext.newInstance("HOST*", "sys*");
    logAggregationService.handle(new LogHandlerAppStartedEvent(app, this.user,
        null, ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls, context));

    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(app, 1);
    ContainerId cont = BuilderUtils.newContainerId(appAttemptId, 1);
    writeContainerLogs(appLogDir, cont, new String[] { "stdout",
        "stderr", "syslog" });
    logAggregationService.handle(new LogHandlerContainerFinishedEvent(cont, 0));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(app));
    logAggregationService.stop();
    delSrvc.stop();
    // Aggregated logs should not be deleted if not uploaded.
    verify(delSrvc, times(0)).delete(user, null);
  }

  @Test
  public void testNoContainerOnNode() throws Exception {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
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
    logAggregationService.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleAppsLogAggregation() throws Exception {

    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    String[] fileNames = new String[] { "stdout", "stderr", "syslog" };

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
    writeContainerLogs(app1LogDir, container11, fileNames);
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
    
    writeContainerLogs(app2LogDir, container21, fileNames);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container21, 0));

    ContainerId container12 = BuilderUtils.newContainerId(appAttemptId1, 2);

    writeContainerLogs(app1LogDir, container12, fileNames);
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
    writeContainerLogs(app3LogDir, container31, fileNames);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container31, 0));

    ContainerId container32 = BuilderUtils.newContainerId(appAttemptId3, 2);
    writeContainerLogs(app3LogDir, container32, fileNames);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container32, 1)); // Failed 

    ContainerId container22 = BuilderUtils.newContainerId(appAttemptId2, 2);
    writeContainerLogs(app2LogDir, container22, fileNames);
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container22, 0));

    ContainerId container33 = BuilderUtils.newContainerId(appAttemptId3, 3);
    writeContainerLogs(app3LogDir, container33, fileNames);
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
        new ContainerId[] { container11, container12 }, fileNames, 3, false);

    verifyContainerLogs(logAggregationService, application2,
        new ContainerId[] { container21 }, fileNames, 3, false);

    verifyContainerLogs(logAggregationService, application3,
        new ContainerId[] { container31, container32 }, fileNames, 3, false);
    
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
  }
  
  @Test
  public void testVerifyAndCreateRemoteDirsFailure()
      throws Exception {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    
    YarnRuntimeException e = new YarnRuntimeException("KABOOM!");
    doThrow(e)
      .when(logAggregationService).verifyAndCreateRemoteLogDir(
          any(Configuration.class));
        
    logAggregationService.start();
    
    // Now try to start an application
    ApplicationId appId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(),
          (int) (Math.random() * 1000));
    logAggregationService.handle(new LogHandlerAppStartedEvent(appId,
        this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
        this.acls));
    dispatcher.await();
    
    // Verify that it failed
    ApplicationEvent[] expectedEvents = new ApplicationEvent[] {
        new ApplicationEvent(appId, 
            ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED)
    };
    checkEvents(appEventHandler, expectedEvents, false,
        "getType", "getApplicationID", "getDiagnostic");

    Mockito.reset(logAggregationService);
    
    // Now try to start another one
    ApplicationId appId2 =
        BuilderUtils.newApplicationId(System.currentTimeMillis(),
          (int) (Math.random() * 1000));
    File appLogDir =
        new File(localLogDir, ConverterUtils.toString(appId2));
    appLogDir.mkdir();
    
    logAggregationService.handle(new LogHandlerAppStartedEvent(appId2,
        this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
        this.acls));
    dispatcher.await();
    
    // Verify that it worked
    expectedEvents = new ApplicationEvent[] {
        new ApplicationEvent(appId, // original failure
            ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED), 
        new ApplicationEvent(appId2, // success
            ApplicationEventType.APPLICATION_LOG_HANDLING_INITED)
    };
    checkEvents(appEventHandler, expectedEvents, false,
        "getType", "getApplicationID", "getDiagnostic");
    
    logAggregationService.stop();
  }
  
  
  @Test
  public void testVerifyAndCreateRemoteDirNonExistence()
      throws Exception {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    File aNewFile = new File(String.valueOf("tmp"+System.currentTimeMillis()));
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        aNewFile.getAbsolutePath());

    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    boolean existsBefore = aNewFile.exists();
    assertTrue("The new file already exists!", !existsBefore);

    logAggregationService.verifyAndCreateRemoteLogDir(this.conf);
    
    boolean existsAfter = aNewFile.exists();
    assertTrue("The new aggregate file is not successfully created", existsAfter);
    aNewFile.delete(); //housekeeping
  }

  @Test
  public void testRemoteRootLogDirIsCreatedWithCorrectGroupOwner()
      throws IOException {
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    Path aNewFile = new Path(String.valueOf("tmp"+System.currentTimeMillis()));
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, aNewFile.getName());

    LogAggregationService logAggregationService = new LogAggregationService(
        dispatcher, this.context, this.delSrvc, super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.verifyAndCreateRemoteLogDir(this.conf);

    String targetGroup =
        UserGroupInformation.getLoginUser().getPrimaryGroupName();
    FileSystem fs = FileSystem.get(this.conf);
    FileStatus fileStatus = fs.getFileStatus(aNewFile);
    Assert.assertEquals("The new aggregate file is not successfully created",
        fileStatus.getGroup(), targetGroup);

    fs.delete(aNewFile, true);
  }

  @Test
  public void testAppLogDirCreation() throws Exception {
    final String logSuffix = "logs";
    this.conf.set(YarnConfiguration.NM_LOG_DIRS,
        localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, logSuffix);

    InlineDispatcher dispatcher = new InlineDispatcher();
    dispatcher.init(this.conf);
    dispatcher.start();

    FileSystem fs = FileSystem.get(this.conf);
    final FileSystem spyFs = spy(FileSystem.get(this.conf));

    LogAggregationService aggSvc = new LogAggregationService(dispatcher,
        this.context, this.delSrvc, super.dirsHandler) {
      @Override
      protected FileSystem getFileSystem(Configuration conf) {
        return spyFs;
      }
    };

    aggSvc.init(this.conf);
    aggSvc.start();

    // start an application and verify user, suffix, and app dirs created
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    Path userDir = fs.makeQualified(new Path(
        remoteRootLogDir.getAbsolutePath(), this.user));
    Path suffixDir = new Path(userDir, logSuffix);
    Path appDir = new Path(suffixDir, appId.toString());
    aggSvc.handle(new LogHandlerAppStartedEvent(appId, this.user, null,
        ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));
    verify(spyFs).mkdirs(eq(userDir), isA(FsPermission.class));
    verify(spyFs).mkdirs(eq(suffixDir), isA(FsPermission.class));
    verify(spyFs).mkdirs(eq(appDir), isA(FsPermission.class));

    // start another application and verify only app dir created
    ApplicationId appId2 = BuilderUtils.newApplicationId(1, 2);
    Path appDir2 = new Path(suffixDir, appId2.toString());
    aggSvc.handle(new LogHandlerAppStartedEvent(appId2, this.user, null,
        ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));
    verify(spyFs).mkdirs(eq(appDir2), isA(FsPermission.class));

    // start another application with the app dir already created and verify
    // we do not try to create it again
    ApplicationId appId3 = BuilderUtils.newApplicationId(1, 3);
    Path appDir3 = new Path(suffixDir, appId3.toString());
    new File(appDir3.toUri().getPath()).mkdir();
    aggSvc.handle(new LogHandlerAppStartedEvent(appId3, this.user, null,
        ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls));
    verify(spyFs, never()).mkdirs(eq(appDir3), isA(FsPermission.class));
    aggSvc.stop();
    aggSvc.close();
    dispatcher.stop();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLogAggregationInitAppFailsWithoutKillingNM() throws Exception {

    this.conf.set(YarnConfiguration.NM_LOG_DIRS,
        localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId appId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(),
          (int) (Math.random() * 1000));
    doThrow(new YarnRuntimeException("KABOOM!"))
      .when(logAggregationService).initAppAggregator(
          eq(appId), eq(user), any(Credentials.class),
          any(ContainerLogsRetentionPolicy.class), anyMap(),
          any(LogAggregationContext.class));

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
  public void testLogAggregationCreateDirsFailsWithoutKillingNM()
      throws Exception {
    
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());

    DeletionService spyDelSrvc = spy(this.delSrvc);
    LogAggregationService logAggregationService = spy(
        new LogAggregationService(dispatcher, this.context, spyDelSrvc,
                                  super.dirsHandler));
    logAggregationService.init(this.conf);
    logAggregationService.start();
    
    ApplicationId appId =
        BuilderUtils.newApplicationId(System.currentTimeMillis(),
          (int) (Math.random() * 1000));

    File appLogDir =
        new File(localLogDir, ConverterUtils.toString(appId));
    appLogDir.mkdir();

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
    verify(spyDelSrvc).delete(eq(user), any(Path.class),
        Mockito.<Path>anyVararg());
    verify(logAggregationService).closeFileSystems(
        any(UserGroupInformation.class));
  }

  private void writeContainerLogs(File appLogDir, ContainerId containerId,
      String[] fileName) throws IOException {
    // ContainerLogDir should be created
    String containerStr = ConverterUtils.toString(containerId);
    File containerLogDir = new File(appLogDir, containerStr);
    containerLogDir.mkdir();
    for (String fileType : fileName) {
      Writer writer11 = new FileWriter(new File(containerLogDir, fileType));
      writer11.write(containerStr + " Hello " + fileType + "!");
      writer11.close();
    }
  }

  private LogFileStatusInLastCycle verifyContainerLogs(LogAggregationService logAggregationService,
      ApplicationId appId, ContainerId[] expectedContainerIds,
      String[] logFiles, int numOfContainerLogs, boolean multiLogs)
      throws IOException {
    Path appLogDir = logAggregationService.getRemoteAppLogDir(appId, this.user);
    RemoteIterator<FileStatus> nodeFiles = null;
    try {
      Path qualifiedLogDir =
          FileContext.getFileContext(this.conf).makeQualified(appLogDir);
      nodeFiles =
          FileContext.getFileContext(qualifiedLogDir.toUri(), this.conf)
            .listStatus(appLogDir);
    } catch (FileNotFoundException fnf) {
      Assert.fail("Should have log files");
    }

    Assert.assertTrue(nodeFiles.hasNext());
    FileStatus targetNodeFile = null;
    if (! multiLogs) {
      targetNodeFile = nodeFiles.next();
      Assert.assertTrue(targetNodeFile.getPath().getName().equals(
        LogAggregationUtils.getNodeString(logAggregationService.getNodeId())));
    } else {
      long fileCreateTime = 0;
      while (nodeFiles.hasNext()) {
        FileStatus nodeFile = nodeFiles.next();
        if (!nodeFile.getPath().getName()
          .contains(LogAggregationUtils.TMP_FILE_SUFFIX)) {
          long time =
              Long.parseLong(nodeFile.getPath().getName().split("_")[2]);
          if (time > fileCreateTime) {
            targetNodeFile = nodeFile;
            fileCreateTime = time;
          }
        }
      }
      String[] fileName = targetNodeFile.getPath().getName().split("_");
      Assert.assertTrue(fileName.length == 3);
      Assert.assertEquals(fileName[0] + ":" + fileName[1],
        logAggregationService.getNodeId().toString());
    }
    AggregatedLogFormat.LogReader reader =
        new AggregatedLogFormat.LogReader(this.conf, targetNodeFile.getPath());
    Assert.assertEquals(this.user, reader.getApplicationOwner());
    verifyAcls(reader.getApplicationAcls());

    List<String> fileTypes = new ArrayList<String>();

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
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            LogReader.readAContainerLogsForALogType(valueStream, ps);

            String writtenLines[] = baos.toString().split(
              System.getProperty("line.separator"));

            Assert.assertEquals("LogType:", writtenLines[0].substring(0, 8));
            String fileType = writtenLines[0].substring(8);
            fileTypes.add(fileType);

            Assert.assertEquals("LogLength:", writtenLines[1].substring(0, 10));
            String fileLengthStr = writtenLines[1].substring(10);
            long fileLength = Long.parseLong(fileLengthStr);

            Assert.assertEquals("Log Contents:",
              writtenLines[2].substring(0, 13));

            String logContents = StringUtils.join(
              Arrays.copyOfRange(writtenLines, 3, writtenLines.length), "\n");
            perContainerMap.put(fileType, logContents);

            LOG.info("LogType:" + fileType);
            LOG.info("LogLength:" + fileLength);
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
        Assert.assertEquals(numOfContainerLogs, thisContainerMap.size());
        for (String fileType : logFiles) {
          String expectedValue =
              containerStr + " Hello " + fileType + "!End of LogType:"
                  + fileType;
          LOG.info("Expected log-content : " + new String(expectedValue));
          String foundValue = thisContainerMap.remove(fileType);
          Assert.assertNotNull(cId + " " + fileType
              + " not present in aggregated log-file!", foundValue);
          Assert.assertEquals(expectedValue, foundValue);
        }
        Assert.assertEquals(0, thisContainerMap.size());
      }
      Assert.assertEquals(0, logMap.size());
      return new LogFileStatusInLastCycle(targetNodeFile.getPath().getName(), fileTypes);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLogAggregationForRealContainerLaunch() throws IOException,
      InterruptedException, YarnException {

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
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId cId = BuilderUtils.newContainerId(appAttemptId, 0);

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
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/bash");
    commands.add(scriptFile.getAbsolutePath());
    containerLaunchContext.setCommands(commands);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          TestContainerManager.createContainerToken(
            cId, DUMMY_RM_IDENTIFIER, context.getNodeId(), user,
            context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    this.containerManager.startContainers(allRequests);
    
    BaseContainerManagerTest.waitForContainerState(this.containerManager,
        cId, ContainerState.COMPLETE);

    this.containerManager.handle(new CMgrCompletedAppsEvent(Arrays
        .asList(appId), CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));
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
  public void testStopAfterError() throws Exception {
    DeletionService delSrvc = mock(DeletionService.class);

    // get the AppLogAggregationImpl thread to crash
    LocalDirsHandlerService mockedDirSvc = mock(LocalDirsHandlerService.class);
    when(mockedDirSvc.getLogDirs()).thenThrow(new RuntimeException());
    
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
    logAggregationService.close();
  }

  @Test
  public void testLogAggregatorCleanup() throws Exception {
    DeletionService delSrvc = mock(DeletionService.class);

    // get the AppLogAggregationImpl thread to crash
    LocalDirsHandlerService mockedDirSvc = mock(LocalDirsHandlerService.class);

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
    logAggregationService.stop();
    logAggregationService.close();
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

  /*
   * Test to make sure we handle cases where the directories we get back from
   * the LocalDirsHandler may have issues including the log dir not being
   * present as well as other issues. The test uses helper functions from
   * TestNonAggregatingLogHandler.
   */
  @Test
  public void testFailedDirsLocalFileDeletionAfterUpload() throws Exception {

    // setup conf and services
    DeletionService mockDelService = mock(DeletionService.class);
    File[] localLogDirs =
        TestNonAggregatingLogHandler.getLocalLogDirFiles(this.getClass()
          .getName(), 7);
    final List<String> localLogDirPaths =
        new ArrayList<String>(localLogDirs.length);
    for (int i = 0; i < localLogDirs.length; i++) {
      localLogDirPaths.add(localLogDirs[i].getAbsolutePath());
    }

    String localLogDirsString = StringUtils.join(localLogDirPaths, ",");

    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      this.remoteRootLogDir.getAbsolutePath());
    this.conf.setLong(YarnConfiguration.NM_DISK_HEALTH_CHECK_INTERVAL_MS, 500);

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(application1, 1);

    this.dirsHandler = new LocalDirsHandlerService();
    LocalDirsHandlerService mockDirsHandler = mock(LocalDirsHandlerService.class);

    LogAggregationService logAggregationService =
        spy(new LogAggregationService(dispatcher, this.context, mockDelService,
          mockDirsHandler));
    AbstractFileSystem spylfs =
        spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doReturn(lfs).when(logAggregationService).getLocalFileContext(
      isA(Configuration.class));

    logAggregationService.init(this.conf);
    logAggregationService.start();

    TestNonAggregatingLogHandler.runMockedFailedDirs(logAggregationService,
      application1, user, mockDelService, mockDirsHandler, conf, spylfs, lfs,
      localLogDirs);

    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
    verify(logAggregationService).closeFileSystems(
      any(UserGroupInformation.class));

    ApplicationEvent expectedEvents[] =
        new ApplicationEvent[] {
            new ApplicationEvent(appAttemptId.getApplicationId(),
              ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
            new ApplicationEvent(appAttemptId.getApplicationId(),
              ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED) };

    checkEvents(appEventHandler, expectedEvents, true, "getType",
      "getApplicationID");
  }

  @Test (timeout = 50000)
  @SuppressWarnings("unchecked")
  public void testLogAggregationServiceWithPatterns() throws Exception {

    LogAggregationContext logAggregationContextWithIncludePatterns =
        Records.newRecord(LogAggregationContext.class);
    String includePattern = "stdout|syslog";
    logAggregationContextWithIncludePatterns.setIncludePattern(includePattern);

    LogAggregationContext LogAggregationContextWithExcludePatterns =
        Records.newRecord(LogAggregationContext.class);
    String excludePattern = "stdout|syslog";
    LogAggregationContextWithExcludePatterns.setExcludePattern(excludePattern);

    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      this.remoteRootLogDir.getAbsolutePath());

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);
    ApplicationId application2 = BuilderUtils.newApplicationId(1234, 2);
    ApplicationId application3 = BuilderUtils.newApplicationId(1234, 3);
    ApplicationId application4 = BuilderUtils.newApplicationId(1234, 4);
    Application mockApp = mock(Application.class);
    when(mockApp.getContainers()).thenReturn(
        new HashMap<ContainerId, Container>());

    this.context.getApplications().put(application1, mockApp);
    this.context.getApplications().put(application2, mockApp);
    this.context.getApplications().put(application3, mockApp);
    this.context.getApplications().put(application4, mockApp);

    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
          super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    // LogContext for application1 has includePatten which includes
    // stdout and syslog.
    // After logAggregation is finished, we expect the logs for application1
    // has only logs from stdout and syslog
    // AppLogDir should be created
    File appLogDir1 =
        new File(localLogDir, ConverterUtils.toString(application1));
    appLogDir1.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application1,
      this.user, null, ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls,
      logAggregationContextWithIncludePatterns));

    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(application1, 1);
    ContainerId container1 = BuilderUtils.newContainerId(appAttemptId1, 1);

    // Simulate log-file creation
    writeContainerLogs(appLogDir1, container1, new String[] { "stdout",
        "stderr", "syslog" });
    logAggregationService.handle(new LogHandlerContainerFinishedEvent(
      container1, 0));

    // LogContext for application2 has excludePatten which includes
    // stdout and syslog.
    // After logAggregation is finished, we expect the logs for application2
    // has only logs from stderr
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(application2, 1);

    File app2LogDir =
        new File(localLogDir, ConverterUtils.toString(application2));
    app2LogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application2,
      this.user, null, ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY,
      this.acls, LogAggregationContextWithExcludePatterns));
    ContainerId container2 = BuilderUtils.newContainerId(appAttemptId2, 1);

    writeContainerLogs(app2LogDir, container2, new String[] { "stdout",
        "stderr", "syslog" });
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container2, 0));

    // LogContext for application3 has includePattern which is *.log and
    // excludePatten which includes std.log and sys.log.
    // After logAggregation is finished, we expect the logs for application3
    // has all logs whose suffix is .log but excluding sys.log and std.log
    LogAggregationContext context1 =
        Records.newRecord(LogAggregationContext.class);
    context1.setIncludePattern(".*.log");
    context1.setExcludePattern("sys.log|std.log");
    ApplicationAttemptId appAttemptId3 =
        BuilderUtils.newApplicationAttemptId(application3, 1);
    File app3LogDir =
        new File(localLogDir, ConverterUtils.toString(application3));
    app3LogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application3,
      this.user, null, ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY,
      this.acls, context1));
    ContainerId container3 = BuilderUtils.newContainerId(appAttemptId3, 1);
    writeContainerLogs(app3LogDir, container3, new String[] { "stdout",
        "sys.log", "std.log", "out.log", "err.log", "log" });
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container3, 0));

    // LogContext for application4 has includePattern
    // which includes std.log and sys.log and
    // excludePatten which includes std.log.
    // After logAggregation is finished, we expect the logs for application4
    // only has sys.log
    LogAggregationContext context2 =
        Records.newRecord(LogAggregationContext.class);
    context2.setIncludePattern("sys.log|std.log");
    context2.setExcludePattern("std.log");
    ApplicationAttemptId appAttemptId4 =
        BuilderUtils.newApplicationAttemptId(application4, 1);
    File app4LogDir =
        new File(localLogDir, ConverterUtils.toString(application4));
    app4LogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application4,
      this.user, null, ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY,
      this.acls, context2));
    ContainerId container4 = BuilderUtils.newContainerId(appAttemptId4, 1);
    writeContainerLogs(app4LogDir, container4, new String[] { "stdout",
        "sys.log", "std.log", "out.log", "err.log", "log" });
    logAggregationService.handle(
        new LogHandlerContainerFinishedEvent(container4, 0));

    dispatcher.await();
    ApplicationEvent expectedInitEvents[] =
        new ApplicationEvent[] { new ApplicationEvent(application1,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(application2,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(application3,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED),
        new ApplicationEvent(application4,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED)};
    checkEvents(appEventHandler, expectedInitEvents, false, "getType",
      "getApplicationID");
    reset(appEventHandler);

    logAggregationService.handle(new LogHandlerAppFinishedEvent(application1));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(application2));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(application3));
    logAggregationService.handle(new LogHandlerAppFinishedEvent(application4));
    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());

    String[] logFiles = new String[] { "stdout", "syslog" };
    verifyContainerLogs(logAggregationService, application1,
      new ContainerId[] { container1 }, logFiles, 2, false);

    logFiles = new String[] { "stderr" };
    verifyContainerLogs(logAggregationService, application2,
      new ContainerId[] { container2 }, logFiles, 1, false);

    logFiles = new String[] { "out.log", "err.log" };
    verifyContainerLogs(logAggregationService, application3,
      new ContainerId[] { container3 }, logFiles, 2, false);

    logFiles = new String[] { "sys.log" };
    verifyContainerLogs(logAggregationService, application4,
      new ContainerId[] { container4 }, logFiles, 1, false);

    dispatcher.await();

    ApplicationEvent[] expectedFinishedEvents =
        new ApplicationEvent[] { new ApplicationEvent(application1,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED),
        new ApplicationEvent(application2,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED),
        new ApplicationEvent(application3,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED),
        new ApplicationEvent(application4,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED) };
    checkEvents(appEventHandler, expectedFinishedEvents, false, "getType",
      "getApplicationID");
  }

  @Test (timeout = 50000)
  public void testLogAggregationServiceWithInterval() throws Exception {
    testLogAggregationService(false);
  }

  @Test (timeout = 50000)
  public void testLogAggregationServiceWithRetention() throws Exception {
    testLogAggregationService(true);
  }

  @SuppressWarnings("unchecked")
  private void testLogAggregationService(boolean retentionSizeLimitation)
      throws Exception {
    LogAggregationContext logAggregationContextWithInterval =
        Records.newRecord(LogAggregationContext.class);
    // set IncludePattern/excludePattern in rolling fashion
    // we expect all the logs except std_final will be uploaded
    // when app is running. The std_final will be uploaded when
    // the app finishes.
    logAggregationContextWithInterval.setRolledLogsIncludePattern(".*");
    logAggregationContextWithInterval.setRolledLogsExcludePattern("std_final");
    this.conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    //configure YarnConfiguration.NM_REMOTE_APP_LOG_DIR to
    //have fully qualified path
    this.conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      "file://" + this.remoteRootLogDir.getAbsolutePath());
    this.conf.setLong(
      YarnConfiguration.NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS,
      3600);
    if (retentionSizeLimitation) {
      // set the retention size as 1. The number of logs for one application
      // in one NM should be 1.
      this.conf.setInt(YarnConfiguration.NM_PREFIX
          + "log-aggregation.num-log-files-per-app", 1);
    }

    // by setting this configuration, the log files will not be deleted immediately after
    // they are aggregated to remote directory.
    // We could use it to test whether the previous aggregated log files will be aggregated
    // again in next cycle.
    this.conf.setLong(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);

    ApplicationId application = BuilderUtils.newApplicationId(123456, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(application, 1);
    ContainerId container = BuilderUtils.newContainerId(appAttemptId, 1);

    Context context = spy(this.context);
    ConcurrentMap<ApplicationId, Application> maps =
        new ConcurrentHashMap<ApplicationId, Application>();
    Application app = mock(Application.class);
    Map<ContainerId, Container> containers = new HashMap<ContainerId, Container>();
    containers.put(container, mock(Container.class));
    maps.put(application, app);
    when(app.getContainers()).thenReturn(containers);
    when(context.getApplications()).thenReturn(maps);

    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, context, this.delSrvc,
          super.dirsHandler);

    logAggregationService.init(this.conf);
    logAggregationService.start();

    // AppLogDir should be created
    File appLogDir =
        new File(localLogDir, ConverterUtils.toString(application));
    appLogDir.mkdir();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application,
      this.user, null, ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls,
      logAggregationContextWithInterval));

    LogFileStatusInLastCycle logFileStatusInLastCycle = null;
    // Simulate log-file creation
    // create std_final in log directory which will not be aggregated
    // until the app finishes.
    String[] logFiles1WithFinalLog =
        new String[] { "stdout", "stderr", "syslog", "std_final" };
    String[] logFiles1 = new String[] { "stdout", "stderr", "syslog"};
    writeContainerLogs(appLogDir, container, logFiles1WithFinalLog);

    // Do log aggregation
    AppLogAggregatorImpl aggregator =
        (AppLogAggregatorImpl) logAggregationService.getAppLogAggregators()
          .get(application);
    aggregator.doLogAggregationOutOfBand();

    if (retentionSizeLimitation) {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 1, true, null));
    } else {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 1, false, null));
    }
    // Container logs should be uploaded
    logFileStatusInLastCycle = verifyContainerLogs(logAggregationService, application,
        new ContainerId[] { container }, logFiles1, 3, true);
    for(String logFile : logFiles1) {
      Assert.assertTrue(logFileStatusInLastCycle.getLogFileTypesInLastCycle()
        .contains(logFile));
    }
    // Make sure the std_final is not uploaded.
    Assert.assertFalse(logFileStatusInLastCycle.getLogFileTypesInLastCycle()
      .contains("std_final"));

    Thread.sleep(2000);

    // There is no log generated at this time. Do the log aggregation again.
    aggregator.doLogAggregationOutOfBand();

    // Same logs will not be aggregated again.
    // Only one aggregated log file in Remote file directory.
    Assert.assertEquals(numOfLogsAvailable(logAggregationService,
        application, true, null), 1);

    Thread.sleep(2000);

    // Do log aggregation
    String[] logFiles2 = new String[] { "stdout_1", "stderr_1", "syslog_1" };
    writeContainerLogs(appLogDir, container, logFiles2);

    aggregator.doLogAggregationOutOfBand();

    if (retentionSizeLimitation) {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 1, true, logFileStatusInLastCycle.getLogFilePathInLastCycle()));
    } else {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 2, false, null));
    }
    // Container logs should be uploaded
    logFileStatusInLastCycle = verifyContainerLogs(logAggregationService, application,
        new ContainerId[] { container }, logFiles2, 3, true);

    for(String logFile : logFiles2) {
      Assert.assertTrue(logFileStatusInLastCycle.getLogFileTypesInLastCycle()
        .contains(logFile));
    }
    // Make sure the std_final is not uploaded.
    Assert.assertFalse(logFileStatusInLastCycle.getLogFileTypesInLastCycle()
      .contains("std_final"));

    Thread.sleep(2000);

    // create another logs
    String[] logFiles3 = new String[] { "stdout_2", "stderr_2", "syslog_2" };
    writeContainerLogs(appLogDir, container, logFiles3);

    logAggregationService.handle(
      new LogHandlerContainerFinishedEvent(container, 0));

    dispatcher.await();
    logAggregationService.handle(new LogHandlerAppFinishedEvent(application));
    if (retentionSizeLimitation) {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 1, true, logFileStatusInLastCycle.getLogFilePathInLastCycle()));
    } else {
      Assert.assertTrue(waitAndCheckLogNum(logAggregationService, application,
        50, 3, false, null));
    }

    // the app is finished. The log "std_final" should be aggregated this time.
    String[] logFiles3WithFinalLog =
        new String[] { "stdout_2", "stderr_2", "syslog_2", "std_final" };
    verifyContainerLogs(logAggregationService, application,
      new ContainerId[] { container }, logFiles3WithFinalLog, 4, true);
    logAggregationService.stop();
    assertEquals(0, logAggregationService.getNumAggregators());
  }


  @Test (timeout = 20000)
  public void testAddNewTokenSentFromRMForLogAggregation() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);
    Application mockApp = mock(Application.class);
    when(mockApp.getContainers()).thenReturn(
      new HashMap<ContainerId, Container>());
    this.context.getApplications().put(application1, mockApp);
    @SuppressWarnings("resource")
    LogAggregationService logAggregationService =
        new LogAggregationService(dispatcher, this.context, this.delSrvc,
          super.dirsHandler);
    logAggregationService.init(this.conf);
    logAggregationService.start();
    logAggregationService.handle(new LogHandlerAppStartedEvent(application1,
      this.user, null, ContainerLogsRetentionPolicy.ALL_CONTAINERS, this.acls,
      Records.newRecord(LogAggregationContext.class)));

    // Inject new token for log-aggregation after app log-aggregator init
    Text userText1 = new Text("user1");
    RMDelegationTokenIdentifier dtId1 =
        new RMDelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    final Token<RMDelegationTokenIdentifier> token1 =
        new Token<RMDelegationTokenIdentifier>(dtId1.getBytes(),
          "password1".getBytes(), dtId1.getKind(), new Text("service1"));
    Credentials credentials = new Credentials();
    credentials.addToken(userText1, token1);
    this.context.getSystemCredentialsForApps().put(application1, credentials);

    logAggregationService.handle(new LogHandlerAppFinishedEvent(application1));

    final UserGroupInformation ugi =
        ((AppLogAggregatorImpl) logAggregationService.getAppLogAggregators()
          .get(application1)).getUgi();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        boolean hasNewToken = false;
        for (Token<?> token : ugi.getCredentials().getAllTokens()) {
          if (token.equals(token1)) {
            hasNewToken = true;
          }
        }
        return hasNewToken;
      }
    }, 1000, 20000);
    logAggregationService.stop();
  }

  private int numOfLogsAvailable(LogAggregationService logAggregationService,
      ApplicationId appId, boolean sizeLimited, String lastLogFile)
      throws IOException {
    Path appLogDir = logAggregationService.getRemoteAppLogDir(appId, this.user);
    RemoteIterator<FileStatus> nodeFiles = null;
    try {
      Path qualifiedLogDir =
          FileContext.getFileContext(this.conf).makeQualified(appLogDir);
      nodeFiles =
          FileContext.getFileContext(qualifiedLogDir.toUri(), this.conf)
            .listStatus(appLogDir);
    } catch (FileNotFoundException fnf) {
      return -1;
    }
    int count = 0;
    while (nodeFiles.hasNext()) {
      FileStatus status = nodeFiles.next();
      String filename = status.getPath().getName();
      if (filename.contains(LogAggregationUtils.TMP_FILE_SUFFIX)
          || (lastLogFile != null && filename.contains(lastLogFile)
              && sizeLimited)) {
        return -1;
      }
      if (filename.contains(LogAggregationUtils
        .getNodeString(logAggregationService.getNodeId()))) {
        count++;
      }
    }
    return count;
  }

  private boolean waitAndCheckLogNum(
      LogAggregationService logAggregationService, ApplicationId application,
      int maxAttempts, int expectNum, boolean sizeLimited, String lastLogFile)
      throws IOException, InterruptedException {
    int count = 0;
    while (numOfLogsAvailable(logAggregationService, application, sizeLimited,
      lastLogFile) != expectNum && count <= maxAttempts) {
      Thread.sleep(500);
      count++;
    }
    return numOfLogsAvailable(logAggregationService, application, sizeLimited,
      lastLogFile) == expectNum;
  }

  private static class LogFileStatusInLastCycle {
    private String logFilePathInLastCycle;
    private List<String> logFileTypesInLastCycle;

    public LogFileStatusInLastCycle(String logFilePathInLastCycle,
        List<String> logFileTypesInLastCycle) {
      this.logFilePathInLastCycle = logFilePathInLastCycle;
      this.logFileTypesInLastCycle = logFileTypesInLastCycle;
    }

    public String getLogFilePathInLastCycle() {
      return this.logFilePathInLastCycle;
    }

    public List<String> getLogFileTypesInLastCycle() {
      return this.logFileTypesInLastCycle;
    }
  }
}
