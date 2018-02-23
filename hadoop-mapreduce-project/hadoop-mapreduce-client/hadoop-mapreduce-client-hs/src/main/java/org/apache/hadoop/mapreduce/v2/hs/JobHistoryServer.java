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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.v2.hs.HistoryServerStateStoreService.HistoryServerState;
import org.apache.hadoop.mapreduce.v2.hs.server.HSAdminServer;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/******************************************************************
 * {@link JobHistoryServer} is responsible for servicing all job history
 * related requests from client.
 *
 *****************************************************************/
public class JobHistoryServer extends CompositeService {

  /**
   * Priority of the JobHistoryServer shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  public static final long historyServerTimeStamp = System.currentTimeMillis();

  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryServer.class);
  protected HistoryContext historyContext;
  private HistoryClientService clientService;
  private JobHistory jobHistoryService;
  protected JHSDelegationTokenSecretManager jhsDTSecretManager;
  private AggregatedLogDeletionService aggLogDelService;
  private HSAdminServer hsAdminServer;
  private HistoryServerStateStoreService stateStore;
  private JvmPauseMonitor pauseMonitor;

  // utility class to start and stop secret manager as part of service
  // framework and implement state recovery for secret manager on startup
  private class HistoryServerSecretManagerService
      extends AbstractService {

    public HistoryServerSecretManagerService() {
      super(HistoryServerSecretManagerService.class.getName());
    }

    @Override
    protected void serviceStart() throws Exception {
      boolean recoveryEnabled = getConfig().getBoolean(
          JHAdminConfig.MR_HS_RECOVERY_ENABLE,
          JHAdminConfig.DEFAULT_MR_HS_RECOVERY_ENABLE);
      if (recoveryEnabled) {
        assert stateStore.isInState(STATE.STARTED);
        HistoryServerState state = stateStore.loadState();
        jhsDTSecretManager.recover(state);
      }

      try {
        jhsDTSecretManager.startThreads();
      } catch(IOException io) {
        LOG.error("Error while starting the Secret Manager threads", io);
        throw io;
      }

      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      if (jhsDTSecretManager != null) {
        jhsDTSecretManager.stopThreads();
      }
      super.serviceStop();
    }
  }

  public JobHistoryServer() {
    super(JobHistoryServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new YarnConfiguration(conf);

    // This is required for WebApps to use https if enabled.
    MRWebAppUtil.initialize(getConfig());
    try {
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnRuntimeException("History Server Failed to login", ie);
    }
    jobHistoryService = new JobHistory();
    historyContext = (HistoryContext)jobHistoryService;
    stateStore = createStateStore(conf);
    this.jhsDTSecretManager = createJHSSecretManager(conf, stateStore);
    clientService = createHistoryClientService();
    aggLogDelService = new AggregatedLogDeletionService();
    hsAdminServer = new HSAdminServer(aggLogDelService, jobHistoryService);
    addService(stateStore);
    addService(new HistoryServerSecretManagerService());
    addService(jobHistoryService);
    addService(clientService);
    addService(aggLogDelService);
    addService(hsAdminServer);

    DefaultMetricsSystem.initialize("JobHistoryServer");
    JvmMetrics jm = JvmMetrics.initSingleton("JobHistoryServer", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    super.serviceInit(config);
  }

  @VisibleForTesting
  protected HistoryClientService createHistoryClientService() {
    return new HistoryClientService(historyContext, 
        this.jhsDTSecretManager);
  }

  protected JHSDelegationTokenSecretManager createJHSSecretManager(
      Configuration conf, HistoryServerStateStoreService store) {
    long secretKeyInterval = 
        conf.getLong(MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
                     MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
      long tokenMaxLifetime =
        conf.getLong(MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                     MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
      long tokenRenewInterval =
        conf.getLong(MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
                     MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      
    return new JHSDelegationTokenSecretManager(secretKeyInterval, 
        tokenMaxLifetime, tokenRenewInterval, 3600000, store);
  }

  protected HistoryServerStateStoreService createStateStore(
      Configuration conf) {
    return HistoryServerStateStoreServiceFactory.getStore(conf);
  }

  protected void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(conf, JHAdminConfig.MR_HISTORY_KEYTAB,
        JHAdminConfig.MR_HISTORY_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * Retrieve JHS bind address from configuration
   *
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(JHAdminConfig.MR_HISTORY_ADDRESS,
      JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS,
      JHAdminConfig.DEFAULT_MR_HISTORY_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  @Private
  public HistoryClientService getClientService() {
    return this.clientService;
  }

  static JobHistoryServer launchJobHistoryServer(String[] args) {
    Thread.
        setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);
    JobHistoryServer jobHistoryServer = null;
    try {
      jobHistoryServer = new JobHistoryServer();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(jobHistoryServer),
          SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration(new JobConf());
      new GenericOptionsParser(conf, args);
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting JobHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting JobHistoryServer");
    }
    return jobHistoryServer;
  }

  public static void main(String[] args) {
    launchJobHistoryServer(args);
  }
}
