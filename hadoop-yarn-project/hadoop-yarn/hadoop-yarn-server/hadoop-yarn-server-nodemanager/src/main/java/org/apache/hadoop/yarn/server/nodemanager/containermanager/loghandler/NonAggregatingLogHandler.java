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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLogDeleterState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Log Handler which schedules deletion of log files based on the configured log
 * retention time.
 */
public class NonAggregatingLogHandler extends AbstractService implements
    LogHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(NonAggregatingLogHandler.class);
  private final Dispatcher dispatcher;
  private final DeletionService delService;
  private final Map<ApplicationId, String> appOwners;

  private final LocalDirsHandlerService dirsHandler;
  private final NMStateStoreService stateStore;
  private long deleteDelaySeconds;
  private ScheduledThreadPoolExecutor sched;

  public NonAggregatingLogHandler(Dispatcher dispatcher,
      DeletionService delService, LocalDirsHandlerService dirsHandler,
      NMStateStoreService stateStore) {
    super(NonAggregatingLogHandler.class.getName());
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;
    this.stateStore = stateStore;
    this.appOwners = new ConcurrentHashMap<ApplicationId, String>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Default 3 hours.
    this.deleteDelaySeconds =
        conf.getLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS,
                YarnConfiguration.DEFAULT_NM_LOG_RETAIN_SECONDS);
    sched = createScheduledThreadPoolExecutor(conf);
    super.serviceInit(conf);
    recover();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (sched != null) {
      sched.shutdown();
      boolean isShutdown = false;
      try {
        isShutdown = sched.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        sched.shutdownNow();
        isShutdown = true;
      }
      if (!isShutdown) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }
  
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  private void recover() throws IOException {
    if (stateStore.canRecover()) {
      RecoveredLogDeleterState state = stateStore.loadLogDeleterState();
      long now = System.currentTimeMillis();
      for (Map.Entry<ApplicationId, LogDeleterProto> entry :
        state.getLogDeleterMap().entrySet()) {
        ApplicationId appId = entry.getKey();
        LogDeleterProto proto = entry.getValue();
        long deleteDelayMsec = proto.getDeletionTime() - now;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scheduling deletion of " + appId + " logs in "
              + deleteDelayMsec + " msec");
        }
        LogDeleterRunnable logDeleter =
            new LogDeleterRunnable(proto.getUser(), appId);
        try {
          sched.schedule(logDeleter, deleteDelayMsec, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
          // Handling this event in local thread before starting threads
          // or after calling sched.shutdownNow().
          logDeleter.run();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartedEvent =
            (LogHandlerAppStartedEvent) event;
        this.appOwners.put(appStartedEvent.getApplicationId(),
            appStartedEvent.getUser());
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appStartedEvent.getApplicationId(),
                ApplicationEventType.APPLICATION_LOG_HANDLING_INITED));
        break;
      case CONTAINER_FINISHED:
        // Ignore
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        ApplicationId appId = appFinishedEvent.getApplicationId();
        // Schedule - so that logs are available on the UI till they're deleted.
        LOG.info("Scheduling Log Deletion for application: "
            + appId + ", with delay of "
            + this.deleteDelaySeconds + " seconds");
        String user = appOwners.remove(appId);
        if (user == null) {
          LOG.error("Unable to locate user for " + appId);
          // send LOG_HANDLING_FAILED out
          NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
              new ApplicationEvent(appId,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
          break;
        }
        LogDeleterRunnable logDeleter = new LogDeleterRunnable(user, appId);
        long deletionTimestamp = System.currentTimeMillis()
            + this.deleteDelaySeconds * 1000;
        LogDeleterProto deleterProto = LogDeleterProto.newBuilder()
            .setUser(user)
            .setDeletionTime(deletionTimestamp)
            .build();
        try {
          stateStore.storeLogDeleter(appId, deleterProto);
        } catch (IOException e) {
          LOG.error("Unable to record log deleter state", e);
        }
        try {
          sched.schedule(logDeleter, this.deleteDelaySeconds,
              TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
          // Handling this event in local thread before starting threads
          // or after calling sched.shutdownNow().
          logDeleter.run();
        }
        break;
      default:
        ; // Ignore
    }
  }

  ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor(
      Configuration conf) {
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("LogDeleter #%d").build();
    sched =
        new HadoopScheduledThreadPoolExecutor(conf.getInt(
            YarnConfiguration.NM_LOG_DELETION_THREADS_COUNT,
            YarnConfiguration.DEFAULT_NM_LOG_DELETE_THREAD_COUNT), tf);
    return sched;
  }

  class LogDeleterRunnable implements Runnable {
    private String user;
    private ApplicationId applicationId;

    public LogDeleterRunnable(String user, ApplicationId applicationId) {
      this.user = user;
      this.applicationId = applicationId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      List<Path> localAppLogDirs = new ArrayList<Path>();
      FileContext lfs = getLocalFileContext(getConfig());
      for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
        Path logDir = new Path(rootLogDir, applicationId.toString());
        try {
          lfs.getFileStatus(logDir);
          localAppLogDirs.add(logDir);
        } catch (UnsupportedFileSystemException ue) {
          LOG.warn("Unsupported file system used for log dir " + logDir, ue);
          continue;
        } catch (IOException ie) {
          continue;
        }
      }

      // Inform the application before the actual delete itself, so that links
      // to logs will no longer be there on NM web-UI.
      NonAggregatingLogHandler.this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.applicationId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
      if (localAppLogDirs.size() > 0) {
        FileDeletionTask deletionTask = new FileDeletionTask(
            NonAggregatingLogHandler.this.delService, user, null,
            localAppLogDirs);
        NonAggregatingLogHandler.this.delService.delete(deletionTask);
      }
      try {
        NonAggregatingLogHandler.this.stateStore.removeLogDeleter(
            this.applicationId);
      } catch (IOException e) {
        LOG.error("Error removing log deletion state", e);
      }
    }

    @Override
    public String toString() {
      return "LogDeleter for AppId " + this.applicationId.toString()
          + ", owned by " + user;
    }
  }
}