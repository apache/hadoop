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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionMatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.mockito.internal.matchers.VarargMatcher;

public class TestNonAggregatingLogHandler {
  
  DeletionService mockDelService;
  Configuration conf;
  DrainDispatcher dispatcher;
  private ApplicationEventHandler appEventHandler;
  String user = "testuser";
  ApplicationId appId;
  ApplicationAttemptId appAttemptId;
  ContainerId container11;
  LocalDirsHandlerService dirsHandler;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    mockDelService = mock(DeletionService.class);
    conf = new YarnConfiguration();
    dispatcher = createDispatcher(conf);
    appEventHandler = new ApplicationEventHandler();
    dispatcher.register(ApplicationEventType.class, appEventHandler);
    appId = BuilderUtils.newApplicationId(1234, 1);
    appAttemptId = BuilderUtils.newApplicationAttemptId(appId, 1);
    container11 = BuilderUtils.newContainerId(appAttemptId, 1);
    dirsHandler = new LocalDirsHandlerService();
  }

  @After
  public void tearDown() throws IOException {
    dirsHandler.stop();
    dirsHandler.close();
    dispatcher.await();
    dispatcher.stop();
    dispatcher.close();
  }  

  @Test
  public void testLogDeletion() throws IOException {
    File[] localLogDirs = getLocalLogDirFiles(this.getClass().getName(), 2);
    String localLogDirsString =
        localLogDirs[0].getAbsolutePath() + ","
            + localLogDirs[1].getAbsolutePath();

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 0l);

    dirsHandler.init(conf);

    NonAggregatingLogHandler rawLogHandler =
        new NonAggregatingLogHandler(dispatcher, mockDelService, dirsHandler,
            new NMNullStateStoreService());
    NonAggregatingLogHandler logHandler = spy(rawLogHandler);
    AbstractFileSystem spylfs =
        spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doReturn(lfs).when(logHandler)
      .getLocalFileContext(isA(Configuration.class));
    FsPermission defaultPermission =
        FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    final FileStatus fs =
        new FileStatus(0, true, 1, 0, System.currentTimeMillis(), 0,
          defaultPermission, "", "",
          new Path(localLogDirs[0].getAbsolutePath()));
    doReturn(fs).when(spylfs).getFileStatus(isA(Path.class));

    logHandler.init(conf);
    logHandler.start();

    logHandler.handle(new LogHandlerAppStartedEvent(appId, user, null, null));

    logHandler.handle(new LogHandlerContainerFinishedEvent(container11,
        ContainerType.APPLICATION_MASTER, 0));

    logHandler.handle(new LogHandlerAppFinishedEvent(appId));

    Path[] localAppLogDirs = new Path[2];
    localAppLogDirs[0] =
        new Path(localLogDirs[0].getAbsolutePath(), appId.toString());
    localAppLogDirs[1] =
        new Path(localLogDirs[1].getAbsolutePath(), appId.toString());

    testDeletionServiceCall(mockDelService, user, 5000, localAppLogDirs);
    logHandler.close();
    for (int i = 0; i < localLogDirs.length; i++) {
      FileUtils.deleteDirectory(localLogDirs[i]);
    }
  }

  @Test
  public void testDelayedDelete() throws IOException {
    File[] localLogDirs = getLocalLogDirFiles(this.getClass().getName(), 2);
    String localLogDirsString =
        localLogDirs[0].getAbsolutePath() + ","
            + localLogDirs[1].getAbsolutePath();

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);

    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
            YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);

    dirsHandler.init(conf);

    NonAggregatingLogHandler logHandler =
        new NonAggregatingLogHandlerWithMockExecutor(dispatcher, mockDelService,
                                                     dirsHandler);
    logHandler.init(conf);
    logHandler.start();

    logHandler.handle(new LogHandlerAppStartedEvent(appId, user, null, null));

    logHandler.handle(new LogHandlerContainerFinishedEvent(container11,
        ContainerType.APPLICATION_MASTER, 0));

    logHandler.handle(new LogHandlerAppFinishedEvent(appId));

    Path[] localAppLogDirs = new Path[2];
    localAppLogDirs[0] =
        new Path(localLogDirs[0].getAbsolutePath(), appId.toString());
    localAppLogDirs[1] =
        new Path(localLogDirs[1].getAbsolutePath(), appId.toString());

    ScheduledThreadPoolExecutor mockSched =
        ((NonAggregatingLogHandlerWithMockExecutor) logHandler).mockSched;

    verify(mockSched).schedule(any(Runnable.class), eq(10800l),
        eq(TimeUnit.SECONDS));
    logHandler.close();
    for (int i = 0; i < localLogDirs.length; i++) {
      FileUtils.deleteDirectory(localLogDirs[i]);
    }
  }
  
  @Test
  public void testStop() throws Exception {
    NonAggregatingLogHandler aggregatingLogHandler = 
        new NonAggregatingLogHandler(null, null, null,
            new NMNullStateStoreService());

    // It should not throw NullPointerException
    aggregatingLogHandler.stop();

    NonAggregatingLogHandlerWithMockExecutor logHandler = 
        new NonAggregatingLogHandlerWithMockExecutor(null, null, null);
    logHandler.init(new Configuration());
    logHandler.stop();
    verify(logHandler.mockSched).shutdown();
    verify(logHandler.mockSched)
        .awaitTermination(eq(10l), eq(TimeUnit.SECONDS));
    verify(logHandler.mockSched).shutdownNow();
    logHandler.close();
    aggregatingLogHandler.close();
  }

  @Test
  public void testHandlingApplicationFinishedEvent() throws IOException {
    DeletionService delService = new DeletionService(null);
    NonAggregatingLogHandler aggregatingLogHandler =
        new NonAggregatingLogHandler(new InlineDispatcher(),
            delService,
            dirsHandler,
            new NMNullStateStoreService());

    dirsHandler.init(conf);
    dirsHandler.start();
    delService.init(conf);
    delService.start();
    aggregatingLogHandler.init(conf);
    aggregatingLogHandler.start();
    
    // It should NOT throw RejectedExecutionException
    aggregatingLogHandler.handle(new LogHandlerAppFinishedEvent(appId));
    aggregatingLogHandler.stop();

    // It should NOT throw RejectedExecutionException after stopping
    // handler service.
    aggregatingLogHandler.handle(new LogHandlerAppFinishedEvent(appId));
    aggregatingLogHandler.close();
  }

  private class NonAggregatingLogHandlerWithMockExecutor extends
      NonAggregatingLogHandler {

    private ScheduledThreadPoolExecutor mockSched;

    public NonAggregatingLogHandlerWithMockExecutor(Dispatcher dispatcher,
        DeletionService delService, LocalDirsHandlerService dirsHandler) {
      this(dispatcher, delService, dirsHandler, new NMNullStateStoreService());
    }

    public NonAggregatingLogHandlerWithMockExecutor(Dispatcher dispatcher,
        DeletionService delService, LocalDirsHandlerService dirsHandler,
        NMStateStoreService stateStore) {
      super(dispatcher, delService, dirsHandler, stateStore);
    }

    @Override
    ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(
        Configuration conf) {
      mockSched = mock(ScheduledThreadPoolExecutor.class);
      return mockSched;
    }

  }

  private DrainDispatcher createDispatcher(Configuration conf) {
    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    return dispatcher;
  }
  
  /*
   * Test to ensure that we handle the cleanup of directories that may not have
   * the application log dirs we're trying to delete or may have other problems.
   * Test creates 7 log dirs, and fails the directory check for 4 of them and
   * then checks to ensure we tried to delete only the ones that passed the
   * check.
   */
  @Test
  public void testFailedDirLogDeletion() throws Exception {

    File[] localLogDirs = getLocalLogDirFiles(this.getClass().getName(), 7);
    final List<String> localLogDirPaths =
        new ArrayList<String>(localLogDirs.length);
    for (int i = 0; i < localLogDirs.length; i++) {
      localLogDirPaths.add(localLogDirs[i].getAbsolutePath());
    }

    String localLogDirsString = StringUtils.join(localLogDirPaths, ",");

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 0l);

    LocalDirsHandlerService mockDirsHandler = mock(LocalDirsHandlerService.class);

    NonAggregatingLogHandler rawLogHandler =
        new NonAggregatingLogHandler(dispatcher, mockDelService,
            mockDirsHandler, new NMNullStateStoreService());
    NonAggregatingLogHandler logHandler = spy(rawLogHandler);
    AbstractFileSystem spylfs =
        spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doReturn(lfs).when(logHandler)
      .getLocalFileContext(isA(Configuration.class));
    logHandler.init(conf);
    logHandler.start();
    runMockedFailedDirs(logHandler, appId, user, mockDelService,
      mockDirsHandler, conf, spylfs, lfs, localLogDirs);
    logHandler.close();
  }

  @Test
  public void testRecovery() throws Exception {
    File[] localLogDirs = getLocalLogDirFiles(this.getClass().getName(), 2);
    String localLogDirsString =
        localLogDirs[0].getAbsolutePath() + ","
            + localLogDirs[1].getAbsolutePath();

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);

    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
            YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);

    dirsHandler.init(conf);

    appEventHandler.resetLogHandlingEvent();
    assertFalse(appEventHandler.receiveLogHandlingFinishEvent());

    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    NonAggregatingLogHandlerWithMockExecutor logHandler =
        new NonAggregatingLogHandlerWithMockExecutor(dispatcher, mockDelService,
                                                     dirsHandler, stateStore);
    logHandler.init(conf);
    logHandler.start();

    logHandler.handle(new LogHandlerAppStartedEvent(appId, user, null, null));
    logHandler.handle(new LogHandlerContainerFinishedEvent(container11,
        ContainerType.APPLICATION_MASTER, 0));
    logHandler.handle(new LogHandlerAppFinishedEvent(appId));

    // simulate a restart and verify deletion is rescheduled
    logHandler.close();
    logHandler = new NonAggregatingLogHandlerWithMockExecutor(dispatcher,
        mockDelService, dirsHandler, stateStore);
    logHandler.init(conf);
    logHandler.start();
    ArgumentCaptor<Runnable> schedArg = ArgumentCaptor.forClass(Runnable.class);
    verify(logHandler.mockSched).schedule(schedArg.capture(),
        anyLong(), eq(TimeUnit.MILLISECONDS));

    // execute the runnable and verify another restart has nothing scheduled
    schedArg.getValue().run();
    logHandler.close();
    logHandler = new NonAggregatingLogHandlerWithMockExecutor(dispatcher,
        mockDelService, dirsHandler, stateStore);
    logHandler.init(conf);
    logHandler.start();
    verify(logHandler.mockSched, never()).schedule(any(Runnable.class),
        anyLong(), any(TimeUnit.class));

    // wait events get drained.
    this.dispatcher.await();
    assertTrue(appEventHandler.receiveLogHandlingFinishEvent());

    appEventHandler.resetLogHandlingEvent();
    assertFalse(appEventHandler.receiveLogHandlingFailedEvent());
    // send an app finish event against a removed app
    logHandler.handle(new LogHandlerAppFinishedEvent(appId));
    this.dispatcher.await();
    // verify to receive a log failed event.
    assertTrue(appEventHandler.receiveLogHandlingFailedEvent());
    assertFalse(appEventHandler.receiveLogHandlingFinishEvent());
    logHandler.close();
  }

  /**
   * Function to run a log handler with directories failing the getFileStatus
   * call. The function accepts the log handler, setup the mocks to fail with
   * specific exceptions and ensures the deletion service has the correct calls.
   * 
   * @param logHandler the logHandler implementation to test
   * 
   * @param appId the application id that we wish when sending events to the log
   * handler
   * 
   * @param user the user name to use
   * 
   * @param mockDelService a mock of the DeletionService which we will verify
   * the delete calls against
   * 
   * @param dirsHandler a spy or mock on the LocalDirsHandler service used to
   * when creating the logHandler. It needs to be a spy so that we can intercept
   * the getAllLogDirs() call.
   * 
   * @param conf the configuration used
   * 
   * @param spylfs a spy on the AbstractFileSystem object used when creating lfs
   * 
   * @param lfs the FileContext object to be used to mock the getFileStatus()
   * calls
   * 
   * @param localLogDirs list of the log dirs to run the test against, must have
   * at least 7 entries
   */
  public static void runMockedFailedDirs(LogHandler logHandler,
      ApplicationId appId, String user, DeletionService mockDelService,
      LocalDirsHandlerService dirsHandler, Configuration conf,
      AbstractFileSystem spylfs, FileContext lfs, File[] localLogDirs)
      throws Exception {
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    if (localLogDirs.length < 7) {
      throw new IllegalArgumentException(
        "Argument localLogDirs must be at least of length 7");
    }
    Path[] localAppLogDirPaths = new Path[localLogDirs.length];
    for (int i = 0; i < localAppLogDirPaths.length; i++) {
      localAppLogDirPaths[i] =
          new Path(localLogDirs[i].getAbsolutePath(), appId.toString());
    }
    final List<String> localLogDirPaths =
        new ArrayList<String>(localLogDirs.length);
    for (int i = 0; i < localLogDirs.length; i++) {
      localLogDirPaths.add(localLogDirs[i].getAbsolutePath());
    }

    // setup mocks
    FsPermission defaultPermission =
        FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    final FileStatus fs =
        new FileStatus(0, true, 1, 0, System.currentTimeMillis(), 0,
          defaultPermission, "", "",
          new Path(localLogDirs[0].getAbsolutePath()));
    doReturn(fs).when(spylfs).getFileStatus(isA(Path.class));
    doReturn(localLogDirPaths).when(dirsHandler).getLogDirsForCleanup();

    logHandler.handle(new LogHandlerAppStartedEvent(appId, user, null,
        appAcls));

    // test case where some dirs have the log dir to delete
    // mock some dirs throwing various exceptions
    // verify deletion happens only on the others
    Mockito.doThrow(new FileNotFoundException()).when(spylfs)
      .getFileStatus(eq(localAppLogDirPaths[0]));
    doReturn(fs).when(spylfs).getFileStatus(eq(localAppLogDirPaths[1]));
    Mockito.doThrow(new AccessControlException()).when(spylfs)
      .getFileStatus(eq(localAppLogDirPaths[2]));
    doReturn(fs).when(spylfs).getFileStatus(eq(localAppLogDirPaths[3]));
    Mockito.doThrow(new IOException()).when(spylfs)
      .getFileStatus(eq(localAppLogDirPaths[4]));
    Mockito.doThrow(new UnsupportedFileSystemException("test")).when(spylfs)
      .getFileStatus(eq(localAppLogDirPaths[5]));
    doReturn(fs).when(spylfs).getFileStatus(eq(localAppLogDirPaths[6]));

    logHandler.handle(new LogHandlerAppFinishedEvent(appId));

    testDeletionServiceCall(mockDelService, user, 5000, localAppLogDirPaths[1],
      localAppLogDirPaths[3], localAppLogDirPaths[6]);

    return;
  }

  static class DeletePathsMatcher implements
      ArgumentMatcher<Path[]>, VarargMatcher {
    
    // to get rid of serialization warning
    static final long serialVersionUID = 0;

    private transient Path[] matchPaths;

    DeletePathsMatcher(Path... matchPaths) {
      this.matchPaths = matchPaths;
    }

    @Override
    public boolean matches(Path[] varargs) {
      return new EqualsBuilder().append(matchPaths, varargs).isEquals();
    }

    // function to get rid of FindBugs warning
    private void readObject(ObjectInputStream os) throws NotSerializableException {
      throw new NotSerializableException(this.getClass().getName());
    }
  }

  /**
   * Function to verify that the DeletionService object received the right
   * requests.
   * 
   * @param delService the DeletionService mock which we verify against
   * 
   * @param user the user name to use when verifying the deletion
   * 
   * @param timeout amount in milliseconds to wait before we decide the calls
   * didn't come through
   * 
   * @param matchPaths the paths to match in the delete calls
   * 
   * @throws WantedButNotInvoked if the calls could not be verified
   */
  static void testDeletionServiceCall(DeletionService delService, String user,
      long timeout, Path... matchPaths) {

    long verifyStartTime = System.currentTimeMillis();
    WantedButNotInvoked notInvokedException = null;
    boolean matched = false;
    while (!matched && System.currentTimeMillis() < verifyStartTime + timeout) {
      try {
        verify(delService, times(1)).delete(argThat(new FileDeletionMatcher(
            delService, user, null, Arrays.asList(matchPaths))));
        matched = true;
      } catch (WantedButNotInvoked e) {
        notInvokedException = e;
        try {
          Thread.sleep(50l);
        } catch (InterruptedException i) {
        }
      }
    }
    if (!matched) {
      throw notInvokedException;
    }
    return;
  }

  public static File[] getLocalLogDirFiles(String name, int number) {
    File[] dirs = new File[number];
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new File("target", name + "-localLogDir" + i).getAbsoluteFile();
    }
    return dirs;
  }

  class ApplicationEventHandler implements EventHandler<ApplicationEvent> {

    private boolean logHandlingFinished = false;
    private boolean logHandlingFailed = false;

    @Override
    public void handle(ApplicationEvent event) {
      switch (event.getType()) {
      case APPLICATION_LOG_HANDLING_FINISHED:
        logHandlingFinished = true;
        break;
      case APPLICATION_LOG_HANDLING_FAILED:
        logHandlingFailed = true;
      default:
        // do nothing.
      }
    }

    public boolean receiveLogHandlingFinishEvent() {
      return logHandlingFinished;
    }

    public boolean receiveLogHandlingFailedEvent() {
      return logHandlingFailed;
    }

    public void resetLogHandlingEvent() {
      logHandlingFinished = false;
      logHandlingFailed = false;
    }
  }

}
