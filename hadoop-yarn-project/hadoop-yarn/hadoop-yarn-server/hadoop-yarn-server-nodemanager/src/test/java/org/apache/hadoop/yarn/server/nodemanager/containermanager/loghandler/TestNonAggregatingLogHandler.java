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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Test;
import org.mockito.exceptions.verification.WantedButNotInvoked;

public class TestNonAggregatingLogHandler {

  @Test
  @SuppressWarnings("unchecked")
  public void testLogDeletion() {
    DeletionService delService = mock(DeletionService.class);
    Configuration conf = new YarnConfiguration();
    String user = "testuser";

    File[] localLogDirs = new File[2];
    localLogDirs[0] =
        new File("target", this.getClass().getName() + "-localLogDir0")
            .getAbsoluteFile();
    localLogDirs[1] =
        new File("target", this.getClass().getName() + "-localLogDir1")
            .getAbsoluteFile();
    String localLogDirsString =
        localLogDirs[0].getAbsolutePath() + ","
            + localLogDirs[1].getAbsolutePath();

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 0l);

    DrainDispatcher dispatcher = createDispatcher(conf);
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);

    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);

    ApplicationId appId1 = BuilderUtils.newApplicationId(1234, 1);
    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(appId1, 1);
    ContainerId container11 = BuilderUtils.newContainerId(appAttemptId1, 1);

    NonAggregatingLogHandler logHandler =
        new NonAggregatingLogHandler(dispatcher, delService, dirsHandler);
    logHandler.init(conf);
    logHandler.start();

    logHandler.handle(new LogHandlerAppStartedEvent(appId1, user, null,
        ContainerLogsRetentionPolicy.ALL_CONTAINERS, null));

    logHandler.handle(new LogHandlerContainerFinishedEvent(container11, 0));

    logHandler.handle(new LogHandlerAppFinishedEvent(appId1));

    Path[] localAppLogDirs = new Path[2];
    localAppLogDirs[0] =
        new Path(localLogDirs[0].getAbsolutePath(), appId1.toString());
    localAppLogDirs[1] =
        new Path(localLogDirs[1].getAbsolutePath(), appId1.toString());

    // 5 seconds for the delete which is a separate thread.
    long verifyStartTime = System.currentTimeMillis();
    WantedButNotInvoked notInvokedException = null;
    boolean matched = false;
    while (!matched && System.currentTimeMillis() < verifyStartTime + 5000l) {
      try {
        verify(delService).delete(eq(user), (Path) eq(null),
            eq(localAppLogDirs[0]), eq(localAppLogDirs[1]));
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
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDelayedDelete() {
    DeletionService delService = mock(DeletionService.class);
    Configuration conf = new YarnConfiguration();
    String user = "testuser";

    File[] localLogDirs = new File[2];
    localLogDirs[0] =
        new File("target", this.getClass().getName() + "-localLogDir0")
            .getAbsoluteFile();
    localLogDirs[1] =
        new File("target", this.getClass().getName() + "-localLogDir1")
            .getAbsoluteFile();
    String localLogDirsString =
        localLogDirs[0].getAbsolutePath() + ","
            + localLogDirs[1].getAbsolutePath();

    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDirsString);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);

    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
            YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);

    DrainDispatcher dispatcher = createDispatcher(conf);
    EventHandler<ApplicationEvent> appEventHandler = mock(EventHandler.class);
    dispatcher.register(ApplicationEventType.class, appEventHandler);

    LocalDirsHandlerService dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);

    ApplicationId appId1 = BuilderUtils.newApplicationId(1234, 1);
    ApplicationAttemptId appAttemptId1 =
        BuilderUtils.newApplicationAttemptId(appId1, 1);
    ContainerId container11 = BuilderUtils.newContainerId(appAttemptId1, 1);

    NonAggregatingLogHandler logHandler =
        new NonAggregatingLogHandlerWithMockExecutor(dispatcher, delService,
                                                     dirsHandler);
    logHandler.init(conf);
    logHandler.start();

    logHandler.handle(new LogHandlerAppStartedEvent(appId1, user, null,
        ContainerLogsRetentionPolicy.ALL_CONTAINERS, null));

    logHandler.handle(new LogHandlerContainerFinishedEvent(container11, 0));

    logHandler.handle(new LogHandlerAppFinishedEvent(appId1));

    Path[] localAppLogDirs = new Path[2];
    localAppLogDirs[0] =
        new Path(localLogDirs[0].getAbsolutePath(), appId1.toString());
    localAppLogDirs[1] =
        new Path(localLogDirs[1].getAbsolutePath(), appId1.toString());

    ScheduledThreadPoolExecutor mockSched =
        ((NonAggregatingLogHandlerWithMockExecutor) logHandler).mockSched;

    verify(mockSched).schedule(any(Runnable.class), eq(10800l),
        eq(TimeUnit.SECONDS));
  }
  
  @Test
  public void testStop() throws Exception {
    NonAggregatingLogHandler aggregatingLogHandler = 
        new NonAggregatingLogHandler(null, null, null);

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
  }

  @Test
  public void testHandlingApplicationFinishedEvent() {
    Configuration conf = new Configuration();
    LocalDirsHandlerService dirsService  = new LocalDirsHandlerService();
    DeletionService delService = new DeletionService(null);
    NonAggregatingLogHandler aggregatingLogHandler =
        new NonAggregatingLogHandler(new InlineDispatcher(),
            delService,
            dirsService);

    dirsService.init(conf);
    dirsService.start();
    delService.init(conf);
    delService.start();
    aggregatingLogHandler.init(conf);
    aggregatingLogHandler.start();
    ApplicationId appId = BuilderUtils.newApplicationId(1234, 1);
    // It should NOT throw RejectedExecutionException
    aggregatingLogHandler.handle(new LogHandlerAppFinishedEvent(appId));
    aggregatingLogHandler.stop();

    // It should NOT throw RejectedExecutionException after stopping
    // handler service.
    aggregatingLogHandler.handle(new LogHandlerAppFinishedEvent(appId));
  }

  private class NonAggregatingLogHandlerWithMockExecutor extends
      NonAggregatingLogHandler {

    private ScheduledThreadPoolExecutor mockSched;

    public NonAggregatingLogHandlerWithMockExecutor(Dispatcher dispatcher,
        DeletionService delService, LocalDirsHandlerService dirsHandler) {
      super(dispatcher, delService, dirsHandler);
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
}
