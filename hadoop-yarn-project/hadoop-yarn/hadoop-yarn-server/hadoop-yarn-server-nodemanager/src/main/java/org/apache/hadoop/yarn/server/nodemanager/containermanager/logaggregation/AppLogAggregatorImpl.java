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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Log LOG = LogFactory
      .getLog(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final Path remoteNodeLogFileForApp;
  private final Path remoteNodeTmpLogFileForApp;
  private final ContainerLogsRetentionPolicy retentionPolicy;

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final LogAggregationContext logAggregationContext;
  private final Context context;

  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      ContainerLogsRetentionPolicy retentionPolicy,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      Context context) {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = ConverterUtils.toString(appId);
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    this.retentionPolicy = retentionPolicy;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
  }

  private void uploadLogsForContainers() {
    if (this.logAggregationDisabled) {
      return;
    }

    // Create a set of Containers whose logs will be uploaded in this cycle.
    // It includes:
    // a) all containers in pendingContainers: those containers are finished
    //    and satisfy the retentionPolicy.
    // b) some set of running containers: For all the Running containers,
    // we have ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY,
    // so simply set wasContainerSuccessful as true to
    // bypass FAILED_CONTAINERS check and find the running containers 
    // which satisfy the retentionPolicy.
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);
    Set<ContainerId> finishedContainers =
        new HashSet<ContainerId>(pendingContainerInThisCycle);
    if (this.context.getApplications().get(this.appId) != null) {
      for (ContainerId container : this.context.getApplications()
        .get(this.appId).getContainers().keySet()) {
        if (shouldUploadLogs(container, true)) {
          pendingContainerInThisCycle.add(container);
        }
      }
    }

    LogWriter writer = null;
    try {
      try {
        writer =
            new LogWriter(this.conf, this.remoteNodeTmpLogFileForApp,
              this.userUgi);
        // Write ACLs once when the writer is created.
        writer.writeApplicationACLs(appAcls);
        writer.writeApplicationOwner(this.userUgi.getShortUserName());

      } catch (IOException e1) {
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ");
        return;
      }

      boolean uploadedLogsInThisCycle = false;
      for (ContainerId container : pendingContainerInThisCycle) {
        ContainerLogAggregator aggregator = null;
        if (containerLogAggregators.containsKey(container)) {
          aggregator = containerLogAggregators.get(container);
        } else {
          aggregator = new ContainerLogAggregator(container);
          containerLogAggregators.put(container, aggregator);
        }
        Set<Path> uploadedFilePathsInThisCycle =
            aggregator.doContainerLogAggregation(writer);
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
        }
        this.delService.delete(this.userUgi.getShortUserName(), null,
          uploadedFilePathsInThisCycle
            .toArray(new Path[uploadedFilePathsInThisCycle.size()]));

        // This container is finished, and all its logs have been uploaded,
        // remove it from containerLogAggregators.
        if (finishedContainers.contains(container)) {
          containerLogAggregators.remove(container);
        }
      }

      if (writer != null) {
        writer.close();
      }

      final Path renamedPath = logAggregationContext == null ||
          logAggregationContext.getRollingIntervalSeconds() <= 0
              ? remoteNodeLogFileForApp : new Path(
                remoteNodeLogFileForApp.getParent(),
                remoteNodeLogFileForApp.getName() + "_"
                    + System.currentTimeMillis());

      final boolean rename = uploadedLogsInThisCycle;
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            FileSystem remoteFS = FileSystem.get(conf);
            if (remoteFS.exists(remoteNodeTmpLogFileForApp)
                && rename) {
              remoteFS.rename(remoteNodeTmpLogFileForApp, renamedPath);
            }
            return null;
          }
        });
      } catch (Exception e) {
        LOG.error(
          "Failed to move temporary log file to final location: ["
              + remoteNodeTmpLogFileForApp + "] to ["
              + renamedPath + "]", e);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } finally {
      if (!this.appAggregationFinished.get()) {
        LOG.warn("Aggregation did not complete for application " + appId);
      }
      this.appAggregationFinished.set(true);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppLogAggregation() {
    while (!this.appFinishing.get() && !this.aborted.get()) {
      synchronized(this) {
        try {
          if (this.logAggregationContext != null && this.logAggregationContext
              .getRollingIntervalSeconds() > 0) {
            wait(this.logAggregationContext.getRollingIntervalSeconds() * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            uploadLogsForContainers();
          } else {
            wait(THREAD_SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.warn("PendingContainers queue is interrupted");
          this.appFinishing.set(true);
        }
      }
    }

    if (this.aborted.get()) {
      return;
    }

    // App is finished, upload the container logs.
    uploadLogsForContainers();

    // Remove the local app-log-dirs
    List<String> rootLogDirs = dirsHandler.getLogDirs();
    Path[] localAppLogDirs = new Path[rootLogDirs.size()];
    int index = 0;
    for (String rootLogDir : rootLogDirs) {
      localAppLogDirs[index] = new Path(rootLogDir, this.applicationId);
      index++;
    }
    this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs);
    
    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);    
  }

  private Path getRemoteNodeTmpLogFileForApp() {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  // TODO: The condition: containerId.getId() == 1 to determine an AM container
  // is not always true.
  private boolean shouldUploadLogs(ContainerId containerId,
      boolean wasContainerSuccessful) {

    // All containers
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.ALL_CONTAINERS)) {
      return true;
    }

    // AM Container only
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY)) {
      if (containerId.getId() == 1) {
        return true;
      }
      return false;
    }

    // AM + Failing containers
    if (this.retentionPolicy
        .equals(ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY)) {
      if (containerId.getId() == 1) {
        return true;
      } else if(!wasContainerSuccessful) {
        return true;
      }
      return false;
    }
    return false;
  }

  @Override
  public void startContainerLogAggregation(ContainerId containerId,
      boolean wasContainerSuccessful) {
    if (shouldUploadLogs(containerId, wasContainerSuccessful)) {
      LOG.info("Considering container " + containerId
          + " for log-aggregation");
      this.pendingContainers.add(containerId);
    }
  }

  @Override
  public synchronized void finishLogAggregation() {
    LOG.info("Application just finished : " + this.applicationId);
    this.appFinishing.set(true);
    this.notifyAll();
  }

  @Override
  public synchronized void abortLogAggregation() {
    LOG.info("Aborting log aggregation for " + this.applicationId);
    this.aborted.set(true);
    this.notifyAll();
  }

  @Private
  @VisibleForTesting
  public synchronized void doLogAggregationOutOfBand() {
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }

  private class ContainerLogAggregator {
    private final ContainerId containerId;
    private Set<String> uploadedFileMeta =
        new HashSet<String>();
    
    public ContainerLogAggregator(ContainerId containerId) {
      this.containerId = containerId;
    }

    public Set<Path> doContainerLogAggregation(LogWriter writer) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirs()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirs(), containerId,
            userUgi.getShortUserName(), logAggregationContext,
            this.uploadedFileMeta);
      try {
        writer.append(logKey, logValue);
      } catch (Exception e) {
        LOG.error("Couldn't upload logs for " + containerId
            + ". Skipping this container.");
        return new HashSet<Path>();
      }
      this.uploadedFileMeta.addAll(logValue
        .getCurrentUpLoadedFileMeta());
      // if any of the previous uploaded logs have been deleted,
      // we need to remove them from alreadyUploadedLogs
      Iterable<String> mask =
          Iterables.filter(uploadedFileMeta, new Predicate<String>() {
            @Override
            public boolean apply(String next) {
              return logValue.getAllExistingFilesMeta().contains(next);
            }
          });

      this.uploadedFileMeta = Sets.newHashSet(mask);
      return logValue.getCurrentUpLoadedFilesPath();
    }
  }
}
