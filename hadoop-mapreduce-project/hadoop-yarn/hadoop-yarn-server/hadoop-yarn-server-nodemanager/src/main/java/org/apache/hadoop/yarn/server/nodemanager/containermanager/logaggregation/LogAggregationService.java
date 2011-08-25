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

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_BIND_ADDRESS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_BIND_ADDRESS;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorEvent;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LogAggregationService extends AbstractService implements
    EventHandler<LogAggregatorEvent> {

  private static final Log LOG = LogFactory
      .getLog(LogAggregationService.class);

  private final DeletionService deletionService;

  private String[] localRootLogDirs;
  Path remoteRootLogDir;
  private String nodeFile;

  static final String LOG_COMPRESSION_TYPE = NMConfig.NM_PREFIX
      + "logaggregation.log_compression_type";
  static final String DEFAULT_COMPRESSION_TYPE = "none";

  private static final String LOG_RENTENTION_POLICY_CONFIG_KEY =
      NMConfig.NM_PREFIX + "logaggregation.retain-policy";

  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  private final ExecutorService threadPool;

  public LogAggregationService(DeletionService deletionService) {
    super(LogAggregationService.class.getName());
    this.deletionService = deletionService;
    this.appLogAggregators =
        new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.threadPool = Executors.newCachedThreadPool();
  }

  public synchronized void init(Configuration conf) {
    this.localRootLogDirs =
        conf.getStrings(NMConfig.NM_LOG_DIR, NMConfig.DEFAULT_NM_LOG_DIR);
    this.remoteRootLogDir =
        new Path(conf.get(NMConfig.REMOTE_USER_LOG_DIR,
            NMConfig.DEFAULT_REMOTE_APP_LOG_DIR));
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    String address =
        getConfig().get(NM_BIND_ADDRESS, DEFAULT_NM_BIND_ADDRESS);
    InetSocketAddress cmBindAddress = NetUtils.createSocketAddr(address);
    try {
      this.nodeFile =
          InetAddress.getLocalHost().getHostAddress() + "_"
              + cmBindAddress.getPort();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }
    super.start();
  }

  Path getRemoteNodeLogFileForApp(ApplicationId appId) {
    return getRemoteNodeLogFileForApp(this.remoteRootLogDir, appId,
        this.nodeFile);
  }

  static Path getRemoteNodeLogFileForApp(Path remoteRootLogDir,
      ApplicationId appId, String nodeFile) {
    return new Path(getRemoteAppLogDir(remoteRootLogDir, appId),
        nodeFile);
  }

  static Path getRemoteAppLogDir(Path remoteRootLogDir,
      ApplicationId appId) {
    return new Path(remoteRootLogDir, ConverterUtils.toString(appId));
  }

  @Override
  public synchronized void stop() {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    for (AppLogAggregator appLogAggregator : this.appLogAggregators.values()) {
      appLogAggregator.join();
    }
    super.stop();
  }

  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy logRetentionPolicy) {

    // Get user's FileSystem credentials
    UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    if (credentials != null) {
      for (Token<? extends TokenIdentifier> token : credentials
          .getAllTokens()) {
        userUgi.addToken(token);
      }
    }

    // New application
    AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.deletionService, getConfig(), appId,
            userUgi, this.localRootLogDirs,
            getRemoteNodeLogFileForApp(appId), logRetentionPolicy);
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnException("Duplicate initApp for " + appId);
    }

    // Create the app dir
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // TODO: Reuse FS for user?
          FileSystem remoteFS = FileSystem.get(getConfig());
          remoteFS.mkdirs(getRemoteAppLogDir(
              LogAggregationService.this.remoteRootLogDir, appId)
              .makeQualified(remoteFS.getUri(),
                  remoteFS.getWorkingDirectory()));
          return null;
        }
      });
    } catch (Exception e) {
      throw new YarnException(e);
    }

    // Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    this.threadPool.execute(appLogAggregator);
  }

  private void stopContainer(ContainerId containerId, String exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.

    if (!this.appLogAggregators.containsKey(containerId.getAppId())) {
      throw new YarnException("Application is not initialized yet for "
          + containerId);
    }
    this.appLogAggregators.get(containerId.getAppId())
        .startContainerLogAggregation(containerId, exitCode.equals("0"));
  }

  private void stopApp(ApplicationId appId) {

    // App is complete. Finish up any containers' pending log aggregation and
    // close the application specific logFile.

    if (!this.appLogAggregators.containsKey(appId)) {
      throw new YarnException("Application is not initialized yet for "
          + appId);
    }
    this.appLogAggregators.get(appId).finishLogAggregation();
  }

  @Override
  public void handle(LogAggregatorEvent event) {
//    switch (event.getType()) {
//    case APPLICATION_STARTED:
//      LogAggregatorAppStartedEvent appStartEvent =
//          (LogAggregatorAppStartedEvent) event;
//      initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
//          appStartEvent.getCredentials(),
//          appStartEvent.getLogRetentionPolicy());
//      break;
//    case CONTAINER_FINISHED:
//      LogAggregatorContainerFinishedEvent containerFinishEvent =
//          (LogAggregatorContainerFinishedEvent) event;
//      stopContainer(containerFinishEvent.getContainerId(),
//          containerFinishEvent.getExitCode());
//      break;
//    case APPLICATION_FINISHED:
//      LogAggregatorAppFinishedEvent appFinishedEvent =
//          (LogAggregatorAppFinishedEvent) event;
//      stopApp(appFinishedEvent.getApplicationId());
//      break;
//    default:
//      ; // Ignore
//    }
  }
}
