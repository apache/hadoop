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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Log LOG = LogFactory
      .getLog(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;

  private final String applicationId;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final String[] rootLogDirs;
  private final Path remoteNodeLogFileForApp;
  private final ContainerLogsRetentionPolicy retentionPolicy;

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();

  private LogWriter writer = null;

  public AppLogAggregatorImpl(DeletionService deletionService,
      Configuration conf, ApplicationId appId, UserGroupInformation userUgi,
      String[] localRootLogDirs, Path remoteNodeLogFileForApp,
      ContainerLogsRetentionPolicy retentionPolicy) {
    this.conf = conf;
    this.delService = deletionService;
    this.applicationId = ConverterUtils.toString(appId);
    this.userUgi = userUgi;
    this.rootLogDirs = localRootLogDirs;
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.retentionPolicy = retentionPolicy;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
  }

  private void uploadLogsForContainer(ContainerId containerId) {

    if (this.logAggregationDisabled) {
      return;
    }

    // Lazy creation of the writer
    if (this.writer == null) {
      LOG.info("Starting aggregate log-file for app " + this.applicationId);
      try {
        this.writer =
            new LogWriter(this.conf, this.remoteNodeLogFileForApp,
                this.userUgi);
      } catch (IOException e) {
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Disabling log-aggregation for this app.", e);
        this.logAggregationDisabled = true;
        return;
      }
    }

    LOG.info("Uploading logs for container " + containerId);
    LogKey logKey = new LogKey(containerId);
    LogValue logValue = new LogValue(this.rootLogDirs, containerId);
    try {
      this.writer.append(logKey, logValue);
    } catch (IOException e) {
      LOG.error("Couldn't upload logs for " + containerId
          + ". Skipping this container.");
    }
  }

  @Override
  public void run() {

    ContainerId containerId;

    while (!this.appFinishing.get()) {
      try {
        containerId = this.pendingContainers.poll();
        if (containerId == null) {
          Thread.sleep(THREAD_SLEEP_TIME);
        } else {
          uploadLogsForContainer(containerId);
        }
      } catch (InterruptedException e) {
        LOG.warn("PendingContainers queue is interrupted");
      }
    }

    // Application is finished. Finish pending-containers
    while ((containerId = this.pendingContainers.poll()) != null) {
      uploadLogsForContainer(containerId);
    }

    // Remove the local app-log-dirs
    Path[] localAppLogDirs = new Path[this.rootLogDirs.length];
    int index = 0;
    for (String rootLogDir : this.rootLogDirs) {
      localAppLogDirs[index] = new Path(rootLogDir, this.applicationId);
      index++;
    }
    this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs);

    if (this.writer != null) {
      this.writer.closeWriter();
      LOG.info("Finished aggregate log-file for app " + this.applicationId);
    }

    this.appAggregationFinished.set(true);
  }

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
  public void finishLogAggregation() {
    LOG.info("Application just finished : " + this.applicationId);
    this.appFinishing.set(true);
  }

  @Override
  public void join() {
    // Aggregation service is finishing
    this.finishLogAggregation();

    while (!this.appAggregationFinished.get()) {
      LOG.info("Waiting for aggregation to complete for "
          + this.applicationId);
      try {
        Thread.sleep(THREAD_SLEEP_TIME);
      } catch (InterruptedException e) {
        LOG.warn("Join interrupted. Some logs may not have been aggregated!!");
        break;
      }
    }
  }
}
