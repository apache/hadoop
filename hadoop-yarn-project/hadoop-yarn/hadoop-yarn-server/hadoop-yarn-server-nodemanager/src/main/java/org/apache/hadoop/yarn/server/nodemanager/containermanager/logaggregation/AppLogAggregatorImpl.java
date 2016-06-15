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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogWriter;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Log LOG = LogFactory
      .getLog(AppLogAggregatorImpl.class);
  private static final int THREAD_SLEEP_TIME = 1000;
  // This is temporary solution. The configuration will be deleted once
  // we find a more scalable method to only write a single log file per LRS.
  private static final String NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP
      = YarnConfiguration.NM_PREFIX + "log-aggregation.num-log-files-per-app";
  private static final int
      DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP = 30;
  
  // This configuration is for debug and test purpose. By setting
  // this configuration as true. We can break the lower bound of
  // NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS.
  private static final String NM_LOG_AGGREGATION_DEBUG_ENABLED
      = YarnConfiguration.NM_PREFIX + "log-aggregation.debug-enabled";

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

  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final FileContext lfs;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final int retentionSize;
  private final long rollingMonitorInterval;
  private final boolean logAggregationInRolling;
  private final NodeId nodeId;

  // These variables are only for testing
  private final AtomicBoolean waiting = new AtomicBoolean(false);
  private int logAggregationTimes = 0;
  private int cleanupOldLogTimes = 0;

  private boolean renameTemporaryLogFileFailed = false;

  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();
  private final ContainerLogAggregationPolicy logAggPolicy;


  /**
   * The value recovered from state store to determine the age of application
   * log files if log retention is enabled. Files older than retention policy
   * will not be uploaded but scheduled for cleaning up. -1 if not recovered.
   */
  private final long recoveredLogInitedTime;

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval) {
    this(dispatcher, deletionService, conf, appId, userUgi, nodeId,
        dirsHandler, remoteNodeLogFileForApp, appAcls,
        logAggregationContext, context, lfs, rollingMonitorInterval, -1);
  }

  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      long recoveredLogInitedTime) {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = appId.toString();
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
    this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.lfs = lfs;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.nodeId = nodeId;
    int configuredRentionSize =
        conf.getInt(NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP,
            DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP);
    if (configuredRentionSize <= 0) {
      this.retentionSize =
          DEFAULT_NM_LOG_AGGREGATION_NUM_LOG_FILES_SIZE_PER_APP;
    } else {
      this.retentionSize = configuredRentionSize;
    }
    this.rollingMonitorInterval = rollingMonitorInterval;
    this.logAggregationInRolling =
        this.rollingMonitorInterval <= 0 || this.logAggregationContext == null
            || this.logAggregationContext.getRolledLogsIncludePattern() == null
            || this.logAggregationContext.getRolledLogsIncludePattern()
              .isEmpty() ? false : true;
    this.logAggPolicy = getLogAggPolicy(conf);
    this.recoveredLogInitedTime = recoveredLogInitedTime;
  }

  private ContainerLogAggregationPolicy getLogAggPolicy(Configuration conf) {
    ContainerLogAggregationPolicy policy = getLogAggPolicyInstance(conf);
    String params = getLogAggPolicyParameters(conf);
    if (params != null) {
      policy.parseParameters(params);
    }
    return policy;
  }

  // Use the policy class specified in LogAggregationContext if available.
  // Otherwise use the cluster-wide default policy class.
  private ContainerLogAggregationPolicy getLogAggPolicyInstance(
      Configuration conf) {
    Class<? extends ContainerLogAggregationPolicy> policyClass = null;
    if (this.logAggregationContext != null) {
      String className =
          this.logAggregationContext.getLogAggregationPolicyClassName();
      if (className != null) {
        try {
          Class<?> policyFromContext = conf.getClassByName(className);
          if (ContainerLogAggregationPolicy.class.isAssignableFrom(
              policyFromContext)) {
            policyClass = policyFromContext.asSubclass(
                ContainerLogAggregationPolicy.class);
          } else {
            LOG.warn(this.appId + " specified invalid log aggregation policy " +
                className);
          }
        } catch (ClassNotFoundException cnfe) {
          // We don't fail the app if the policy class isn't valid.
          LOG.warn(this.appId + " specified invalid log aggregation policy " +
              className);
        }
      }
    }
    if (policyClass == null) {
      policyClass = conf.getClass(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS,
          AllContainerLogAggregationPolicy.class,
              ContainerLogAggregationPolicy.class);
    } else {
      LOG.info(this.appId + " specifies ContainerLogAggregationPolicy of "
          + policyClass);
    }
    return ReflectionUtils.newInstance(policyClass, conf);
  }

  // Use the policy parameters specified in LogAggregationContext if available.
  // Otherwise use the cluster-wide default policy parameters.
  private String getLogAggPolicyParameters(Configuration conf) {
    String params = null;
    if (this.logAggregationContext != null) {
      params = this.logAggregationContext.getLogAggregationPolicyParameters();
    }
    if (params == null) {
      params = conf.get(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS_PARAMETERS);
    }
    return params;
  }

  private void uploadLogsForContainers(boolean appFinished) {
    if (this.logAggregationDisabled) {
      return;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials systemCredentials =
          context.getSystemCredentialsForApps().get(appId);
      if (systemCredentials != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding new framework-token for " + appId
              + " for log-aggregation: " + systemCredentials.getAllTokens()
              + "; userUgi=" + userUgi);
        }
        // this will replace old token
        userUgi.addCredentials(systemCredentials);
      }
    }

    // Create a set of Containers whose logs will be uploaded in this cycle.
    // It includes:
    // a) all containers in pendingContainers: those containers are finished
    //    and satisfy the ContainerLogAggregationPolicy.
    // b) some set of running containers: For all the Running containers,
    //    we use exitCode of 0 to find those which satisfy the
    //    ContainerLogAggregationPolicy.
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);
    Set<ContainerId> finishedContainers =
        new HashSet<ContainerId>(pendingContainerInThisCycle);
    if (this.context.getApplications().get(this.appId) != null) {
      for (Container container : this.context.getApplications()
        .get(this.appId).getContainers().values()) {
        ContainerType containerType =
            container.getContainerTokenIdentifier().getContainerType();
        if (shouldUploadLogs(new ContainerLogContext(
            container.getContainerId(), containerType, 0))) {
          pendingContainerInThisCycle.add(container.getContainerId());
        }
      }
    }

    LogWriter writer = null;
    String diagnosticMessage = "";
    boolean logAggregationSucceedInThisCycle = true;
    try {
      if (pendingContainerInThisCycle.isEmpty()) {
        return;
      }

      logAggregationTimes++;

      try {
        writer = createLogWriter();
        // Write ACLs once when the writer is created.
        writer.writeApplicationACLs(appAcls);
        writer.writeApplicationOwner(this.userUgi.getShortUserName());

      } catch (IOException e1) {
        logAggregationSucceedInThisCycle = false;
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ", e1);
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
            aggregator.doContainerLogAggregation(writer, appFinished,
            finishedContainers.contains(container));
        if (uploadedFilePathsInThisCycle.size() > 0) {
          uploadedLogsInThisCycle = true;
          this.delService.delete(this.userUgi.getShortUserName(), null,
              uploadedFilePathsInThisCycle
                  .toArray(new Path[uploadedFilePathsInThisCycle.size()]));
        }

        // This container is finished, and all its logs have been uploaded,
        // remove it from containerLogAggregators.
        if (finishedContainers.contains(container)) {
          containerLogAggregators.remove(container);
        }
      }

      // Before upload logs, make sure the number of existing logs
      // is smaller than the configured NM log aggregation retention size.
      if (uploadedLogsInThisCycle && logAggregationInRolling) {
        cleanOldLogs();
        cleanupOldLogTimes++;
      }

      if (writer != null) {
        writer.close();
        writer = null;
      }

      long currentTime = System.currentTimeMillis();
      final Path renamedPath = this.rollingMonitorInterval <= 0
              ? remoteNodeLogFileForApp : new Path(
                remoteNodeLogFileForApp.getParent(),
                remoteNodeLogFileForApp.getName() + "_"
                    + currentTime);

      final boolean rename = uploadedLogsInThisCycle;
      try {
        userUgi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            FileSystem remoteFS = remoteNodeLogFileForApp.getFileSystem(conf);
            if (rename) {
              remoteFS.rename(remoteNodeTmpLogFileForApp, renamedPath);
            } else {
              remoteFS.delete(remoteNodeTmpLogFileForApp, false);
            }
            return null;
          }
        });
        diagnosticMessage =
            "Log uploaded successfully for Application: " + appId
                + " in NodeManager: "
                + LogAggregationUtils.getNodeString(nodeId) + " at "
                + Times.format(currentTime) + "\n";
      } catch (Exception e) {
        LOG.error(
          "Failed to move temporary log file to final location: ["
              + remoteNodeTmpLogFileForApp + "] to ["
              + renamedPath + "]", e);
        diagnosticMessage =
            "Log uploaded failed for Application: " + appId
                + " in NodeManager: "
                + LogAggregationUtils.getNodeString(nodeId) + " at "
                + Times.format(currentTime) + "\n";
        renameTemporaryLogFileFailed = true;
        logAggregationSucceedInThisCycle = false;
      }
    } finally {
      LogAggregationStatus logAggregationStatus =
          logAggregationSucceedInThisCycle
              ? LogAggregationStatus.RUNNING
              : LogAggregationStatus.RUNNING_WITH_FAILURE;
      sendLogAggregationReport(logAggregationStatus, diagnosticMessage);
      if (appFinished) {
        // If the app is finished, one extra final report with log aggregation
        // status SUCCEEDED/FAILED will be sent to RM to inform the RM
        // that the log aggregation in this NM is completed.
        LogAggregationStatus finalLogAggregationStatus =
            renameTemporaryLogFileFailed || !logAggregationSucceedInThisCycle
                ? LogAggregationStatus.FAILED
                : LogAggregationStatus.SUCCEEDED;
        sendLogAggregationReport(finalLogAggregationStatus, "");
      }

      if (writer != null) {
        writer.close();
      }
    }
  }

  protected LogWriter createLogWriter() throws IOException {
    return new LogWriter(this.conf, this.remoteNodeTmpLogFileForApp,
        this.userUgi);
  }

  private void sendLogAggregationReport(
      LogAggregationStatus logAggregationStatus, String diagnosticMessage) {
    LogAggregationReport report =
        Records.newRecord(LogAggregationReport.class);
    report.setApplicationId(appId);
    report.setDiagnosticMessage(diagnosticMessage);
    report.setLogAggregationStatus(logAggregationStatus);
    this.context.getLogAggregationStatusForApps().add(report);
  }

  private void cleanOldLogs() {
    try {
      final FileSystem remoteFS =
          this.remoteNodeLogFileForApp.getFileSystem(conf);
      Path appDir =
          this.remoteNodeLogFileForApp.getParent().makeQualified(
            remoteFS.getUri(), remoteFS.getWorkingDirectory());
      Set<FileStatus> status =
          new HashSet<FileStatus>(Arrays.asList(remoteFS.listStatus(appDir)));

      Iterable<FileStatus> mask =
          Iterables.filter(status, new Predicate<FileStatus>() {
            @Override
            public boolean apply(FileStatus next) {
              return next.getPath().getName()
                .contains(LogAggregationUtils.getNodeString(nodeId))
                && !next.getPath().getName().endsWith(
                    LogAggregationUtils.TMP_FILE_SUFFIX);
            }
          });
      status = Sets.newHashSet(mask);
      // Normally, we just need to delete one oldest log
      // before we upload a new log.
      // If we can not delete the older logs in this cycle,
      // we will delete them in next cycle.
      if (status.size() >= this.retentionSize) {
        // sort by the lastModificationTime ascending
        List<FileStatus> statusList = new ArrayList<FileStatus>(status);
        Collections.sort(statusList, new Comparator<FileStatus>() {
          public int compare(FileStatus s1, FileStatus s2) {
            return s1.getModificationTime() < s2.getModificationTime() ? -1
                : s1.getModificationTime() > s2.getModificationTime() ? 1 : 0;
          }
        });
        for (int i = 0 ; i <= statusList.size() - this.retentionSize; i++) {
          final FileStatus remove = statusList.get(i);
          try {
            userUgi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                remoteFS.delete(remove.getPath(), false);
                return null;
              }
            });
          } catch (Exception e) {
            LOG.error("Failed to delete " + remove.getPath(), e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to clean old logs", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    try {
      doAppLogAggregation();
    } catch (Exception e) {
      // do post clean up of log directories on any exception
      LOG.error("Error occured while aggregating the log for the application "
          + appId, e);
      doAppLogAggregationPostCleanUp();
    } finally {
      if (!this.appAggregationFinished.get()) {
        LOG.warn("Aggregation did not complete for application " + appId);
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(this.appId,
                ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
      }
      this.appAggregationFinished.set(true);
    }
  }

  @SuppressWarnings("unchecked")
  private void doAppLogAggregation() {
    while (!this.appFinishing.get() && !this.aborted.get()) {
      synchronized(this) {
        try {
          waiting.set(true);
          if (logAggregationInRolling) {
            wait(this.rollingMonitorInterval * 1000);
            if (this.appFinishing.get() || this.aborted.get()) {
              break;
            }
            uploadLogsForContainers(false);
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
    uploadLogsForContainers(true);

    doAppLogAggregationPostCleanUp();

    this.dispatcher.getEventHandler().handle(
        new ApplicationEvent(this.appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));
    this.appAggregationFinished.set(true);
  }

  private void doAppLogAggregationPostCleanUp() {
    // Remove the local app-log-dirs
    List<Path> localAppLogDirs = new ArrayList<Path>();
    for (String rootLogDir : dirsHandler.getLogDirsForCleanup()) {
      Path logPath = new Path(rootLogDir, applicationId);
      try {
        // check if log dir exists
        lfs.getFileStatus(logPath);
        localAppLogDirs.add(logPath);
      } catch (UnsupportedFileSystemException ue) {
        LOG.warn("Log dir " + rootLogDir + "is an unsupported file system", ue);
        continue;
      } catch (IOException fe) {
        continue;
      }
    }

    if (localAppLogDirs.size() > 0) {
      this.delService.delete(this.userUgi.getShortUserName(), null,
        localAppLogDirs.toArray(new Path[localAppLogDirs.size()]));
    }
  }

  private Path getRemoteNodeTmpLogFileForApp() {
    return new Path(remoteNodeLogFileForApp.getParent(),
      (remoteNodeLogFileForApp.getName() + LogAggregationUtils.TMP_FILE_SUFFIX));
  }

  // TODO: The condition: containerId.getId() == 1 to determine an AM container
  // is not always true.
  private boolean shouldUploadLogs(ContainerLogContext logContext) {
    return logAggPolicy.shouldDoLogAggregation(logContext);
  }

  @Override
  public void startContainerLogAggregation(ContainerLogContext logContext) {
    if (shouldUploadLogs(logContext)) {
      LOG.info("Considering container " + logContext.getContainerId()
          + " for log-aggregation");
      this.pendingContainers.add(logContext.getContainerId());
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

  @Override
  public void disableLogAggregation() {
    this.logAggregationDisabled = true;
  }

  @Private
  @VisibleForTesting
  // This is only used for testing.
  // This will wake the log aggregation thread that is waiting for
  // rollingMonitorInterval.
  // To use this method, make sure the log aggregation thread is running
  // and waiting for rollingMonitorInterval.
  public synchronized void doLogAggregationOutOfBand() {
    while(!waiting.get()) {
      try {
        wait(200);
      } catch (InterruptedException e) {
        // Do Nothing
      }
    }
    LOG.info("Do OutOfBand log aggregation");
    this.notifyAll();
  }

  class ContainerLogAggregator {
    private final AggregatedLogFormat.LogRetentionContext retentionContext;
    private final ContainerId containerId;
    private Set<String> uploadedFileMeta = new HashSet<String>();
    public ContainerLogAggregator(ContainerId containerId) {
      this.containerId = containerId;
      this.retentionContext = getRetentionContext();
    }

    private AggregatedLogFormat.LogRetentionContext getRetentionContext() {
      final long logRetentionSecs =
          conf.getLong(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS,
              YarnConfiguration.DEFAULT_LOG_AGGREGATION_RETAIN_SECONDS);
      return new AggregatedLogFormat.LogRetentionContext(
          recoveredLogInitedTime, logRetentionSecs * 1000);
    }

    public Set<Path> doContainerLogAggregation(LogWriter writer,
        boolean appFinished, boolean containerFinished) {
      LOG.info("Uploading logs for container " + containerId
          + ". Current good log dirs are "
          + StringUtils.join(",", dirsHandler.getLogDirsForRead()));
      final LogKey logKey = new LogKey(containerId);
      final LogValue logValue =
          new LogValue(dirsHandler.getLogDirsForRead(), containerId,
            userUgi.getShortUserName(), logAggregationContext,
            this.uploadedFileMeta,  retentionContext, appFinished,
            containerFinished);
      try {
        writer.append(logKey, logValue);
      } catch (Exception e) {
        LOG.error("Couldn't upload logs for " + containerId
            + ". Skipping this container.", e);
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

      // need to return files uploaded or older-than-retention clean up.
      return Sets.union(logValue.getCurrentUpLoadedFilesPath(),
          logValue.getObseleteRetentionLogFiles());

    }
  }

  // only for test
  @VisibleForTesting
  public UserGroupInformation getUgi() {
    return this.userUgi;
  }

  @Private
  @VisibleForTesting
  public int getLogAggregationTimes() {
    return this.logAggregationTimes;
  }

  @VisibleForTesting
  int getCleanupOldLogTimes() {
    return this.cleanupOldLogTimes;
  }
}
