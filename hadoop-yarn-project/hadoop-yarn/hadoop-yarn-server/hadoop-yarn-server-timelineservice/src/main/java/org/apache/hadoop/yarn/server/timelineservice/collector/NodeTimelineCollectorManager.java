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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.timelineservice.security.TimelineV2DelegationTokenSecretManagerService;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class on the NodeManager side that manages adding and removing collectors and
 * their lifecycle. Also instantiates the per-node collector webapp.
 */
@Private
@Unstable
public class NodeTimelineCollectorManager extends TimelineCollectorManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(NodeTimelineCollectorManager.class);

  // REST server for this collector manager.
  private HttpServer2 timelineRestServer;

  private String timelineRestServerBindAddress;

  private volatile CollectorNodemanagerProtocol nmCollectorService;

  private TimelineV2DelegationTokenSecretManagerService tokenMgrService;

  private final boolean runningAsAuxService;

  private UserGroupInformation loginUGI;

  private ScheduledThreadPoolExecutor tokenRenewalExecutor;

  private long tokenRenewInterval;

  private static final long TIME_BEFORE_RENEW_DATE = 10 * 1000; // 10 seconds.

  private static final long TIME_BEFORE_EXPIRY = 5 * 60 * 1000; // 5 minutes.

  static final String COLLECTOR_MANAGER_ATTR_KEY = "collector.manager";

  @VisibleForTesting
  protected NodeTimelineCollectorManager() {
    this(true);
  }

  protected NodeTimelineCollectorManager(boolean asAuxService) {
    super(NodeTimelineCollectorManager.class.getName());
    this.runningAsAuxService = asAuxService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    tokenMgrService = createTokenManagerService();
    addService(tokenMgrService);
    this.loginUGI = UserGroupInformation.getCurrentUser();
    tokenRenewInterval = conf.getLong(
        YarnConfiguration.TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL,
        YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      // Do security login for cases where collector is running outside NM.
      if (!runningAsAuxService) {
        try {
          doSecureLogin();
        } catch(IOException ie) {
          throw new YarnRuntimeException("Failed to login", ie);
        }
      }
      this.loginUGI = UserGroupInformation.getLoginUser();
    }
    tokenRenewalExecutor = new ScheduledThreadPoolExecutor(
        1, new ThreadFactoryBuilder().setNameFormat(
            "App Collector Token Renewal thread").build());
    super.serviceStart();
    startWebApp();
  }

  protected TimelineV2DelegationTokenSecretManagerService
      createTokenManagerService() {
    return new TimelineV2DelegationTokenSecretManagerService();
  }

  @VisibleForTesting
  public TimelineV2DelegationTokenSecretManagerService
      getTokenManagerService() {
    return tokenMgrService;
  }

  private void doSecureLogin() throws IOException {
    Configuration conf = getConfig();
    String webAppURLWithoutScheme =
        WebAppUtils.getTimelineCollectorWebAppURLWithoutScheme(conf);
    InetSocketAddress addr = NetUtils.createSocketAddr(webAppURLWithoutScheme);
    SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
        YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, addr.getHostName());
  }

  @Override
  protected void serviceStop() throws Exception {
    if (timelineRestServer != null) {
      timelineRestServer.stop();
    }
    if (tokenRenewalExecutor != null) {
      tokenRenewalExecutor.shutdownNow();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  public Token<TimelineDelegationTokenIdentifier> generateTokenForAppCollector(
      String user) {
    Token<TimelineDelegationTokenIdentifier> token  = tokenMgrService.
        generateToken(UserGroupInformation.createRemoteUser(user),
            loginUGI.getShortUserName());
    token.setService(new Text(timelineRestServerBindAddress));
    return token;
  }

  @VisibleForTesting
  public long renewTokenForAppCollector(
      AppLevelTimelineCollector appCollector) throws IOException {
    if (appCollector.getDelegationTokenForApp() != null) {
      return tokenMgrService.renewToken(appCollector.getDelegationTokenForApp(),
          appCollector.getAppDelegationTokenRenewer());
    } else {
      LOG.info("Delegation token not available for renewal for app {}",
          appCollector.getTimelineEntityContext().getAppId());
      return -1;
    }
  }

  @VisibleForTesting
  public void cancelTokenForAppCollector(
      AppLevelTimelineCollector appCollector) throws IOException {
    if (appCollector.getDelegationTokenForApp() != null) {
      tokenMgrService.cancelToken(appCollector.getDelegationTokenForApp(),
          appCollector.getAppUser());
    }
  }

  private long getRenewalDelay(long renewInterval) {
    return ((renewInterval > TIME_BEFORE_RENEW_DATE) ?
        renewInterval - TIME_BEFORE_RENEW_DATE : renewInterval);
  }

  private long getRegenerationDelay(long tokenMaxDate) {
    long regenerateTime = tokenMaxDate - Time.now();
    return ((regenerateTime > TIME_BEFORE_EXPIRY) ?
        regenerateTime - TIME_BEFORE_EXPIRY : regenerateTime);
  }

  private org.apache.hadoop.yarn.api.records.Token generateTokenAndSetTimer(
      ApplicationId appId, AppLevelTimelineCollector appCollector)
      throws IOException {
    Token<TimelineDelegationTokenIdentifier> timelineToken =
        generateTokenForAppCollector(appCollector.getAppUser());
    TimelineDelegationTokenIdentifier tokenId =
        timelineToken.decodeIdentifier();
    long renewalDelay = getRenewalDelay(tokenRenewInterval);
    long regenerationDelay = getRegenerationDelay(tokenId.getMaxDate());
    if (renewalDelay > 0 || regenerationDelay > 0) {
      boolean isTimerForRenewal = renewalDelay < regenerationDelay;
      Future<?> renewalOrRegenerationFuture = tokenRenewalExecutor.schedule(
          new CollectorTokenRenewer(appId, isTimerForRenewal),
          isTimerForRenewal? renewalDelay : regenerationDelay,
          TimeUnit.MILLISECONDS);
      appCollector.setDelegationTokenAndFutureForApp(timelineToken,
          renewalOrRegenerationFuture, tokenId.getMaxDate(),
          tokenId.getRenewer().toString());
    }
    LOG.info("Generated a new token {} for app {}", timelineToken, appId);
    return org.apache.hadoop.yarn.api.records.Token.newInstance(
        timelineToken.getIdentifier(), timelineToken.getKind().toString(),
        timelineToken.getPassword(), timelineToken.getService().toString());
  }

  @Override
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
    try {
      // Get context info from NM
      updateTimelineCollectorContext(appId, collector);
      // Generate token for app collector.
      org.apache.hadoop.yarn.api.records.Token token = null;
      if (UserGroupInformation.isSecurityEnabled() &&
          collector instanceof AppLevelTimelineCollector) {
        AppLevelTimelineCollector appCollector =
            (AppLevelTimelineCollector) collector;
        token = generateTokenAndSetTimer(appId, appCollector);
      }
      // Report to NM if a new collector is added.
      reportNewCollectorInfoToNM(appId, token);
    } catch (YarnException | IOException e) {
      // throw exception here as it cannot be used if failed communicate with NM
      LOG.error("Failed to communicate with NM Collector Service for {}", appId);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void postRemove(ApplicationId appId, TimelineCollector collector) {
    if (collector instanceof AppLevelTimelineCollector) {
      try {
        cancelTokenForAppCollector((AppLevelTimelineCollector) collector);
      } catch (IOException e) {
        LOG.warn("Failed to cancel token for app collector with appId {}",
            appId, e);
      }
    }
  }

  /**
   * Launch the REST web server for this collector manager.
   */
  private void startWebApp() {
    Configuration conf = getConfig();
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<String>();
    TimelineServerUtils.addTimelineAuthFilter(
        initializers, defaultInitializers, tokenMgrService);
    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);

    String bindAddress = null;
    String host =
        conf.getTrimmed(YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_BIND_HOST);
    Configuration.IntegerRanges portRanges = conf.getRange(
        YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_BIND_PORT_RANGES, "");
    int startPort = 0;
    if (portRanges != null && !portRanges.isEmpty()) {
      startPort = portRanges.getRangeStart();
    }
    if (host == null || host.isEmpty()) {
      // if collector bind-host is not set, fall back to
      // timeline-service.bind-host to maintain compatibility
      bindAddress =
          conf.get(YarnConfiguration.DEFAULT_TIMELINE_SERVICE_BIND_HOST,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_BIND_HOST)
              + ":" + startPort;
    } else {
      bindAddress = host + ":" + startPort;
    }

    try {
      HttpServer2.Builder builder = new HttpServer2.Builder()
          .setName("timeline")
          .setConf(conf)
          .addEndpoint(URI.create(
              (YarnConfiguration.useHttps(conf) ? "https://" : "http://") +
                  bindAddress));
      if (portRanges != null && !portRanges.isEmpty()) {
        builder.setPortRanges(portRanges);
      }
      if (YarnConfiguration.useHttps(conf)) {
        builder = WebAppUtils.loadSslConfiguration(builder, conf);
      }
      timelineRestServer = builder.build();

      timelineRestServer.addJerseyResourcePackage(
          TimelineCollectorWebService.class.getPackage().getName() + ";"
              + GenericExceptionHandler.class.getPackage().getName() + ";"
              + YarnJacksonJaxbJsonProvider.class.getPackage().getName(),
          "/*");
      timelineRestServer.setAttribute(COLLECTOR_MANAGER_ATTR_KEY, this);
      timelineRestServer.start();
    } catch (Exception e) {
      String msg = "The per-node collector webapp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    //TODO: We need to think of the case of multiple interfaces
    this.timelineRestServerBindAddress = WebAppUtils.getResolvedAddress(
        timelineRestServer.getConnectorAddress(0));
    LOG.info("Instantiated the per-node collector webapp at {}",
        timelineRestServerBindAddress);
  }

  private void reportNewCollectorInfoToNM(ApplicationId appId,
      org.apache.hadoop.yarn.api.records.Token token)
      throws YarnException, IOException {
    ReportNewCollectorInfoRequest request =
        ReportNewCollectorInfoRequest.newInstance(appId,
            this.timelineRestServerBindAddress, token);
    LOG.info("Report a new collector for application: {}" +
        " to the NM Collector Service.", appId);
    getNMCollectorService().reportNewCollectorInfo(request);
  }

  private void updateTimelineCollectorContext(
      ApplicationId appId, TimelineCollector collector)
      throws YarnException, IOException {
    GetTimelineCollectorContextRequest request =
        GetTimelineCollectorContextRequest.newInstance(appId);
    LOG.info("Get timeline collector context for {}", appId);
    GetTimelineCollectorContextResponse response =
        getNMCollectorService().getTimelineCollectorContext(request);
    String userId = response.getUserId();
    if (userId != null && !userId.isEmpty()) {
      LOG.debug("Setting the user in the context: {}", userId);
      collector.getTimelineEntityContext().setUserId(userId);
    }
    String flowName = response.getFlowName();
    if (flowName != null && !flowName.isEmpty()) {
      LOG.debug("Setting the flow name: {}", flowName);
      collector.getTimelineEntityContext().setFlowName(flowName);
    }
    String flowVersion = response.getFlowVersion();
    if (flowVersion != null && !flowVersion.isEmpty()) {
      LOG.debug("Setting the flow version: {}", flowVersion);
      collector.getTimelineEntityContext().setFlowVersion(flowVersion);
    }
    long flowRunId = response.getFlowRunId();
    if (flowRunId != 0L) {
      LOG.debug("Setting the flow run id: {}", flowRunId);
      collector.getTimelineEntityContext().setFlowRunId(flowRunId);
    }
  }

  @VisibleForTesting
  protected CollectorNodemanagerProtocol getNMCollectorService() {
    if (nmCollectorService == null) {
      synchronized (this) {
        if (nmCollectorService == null) {
          Configuration conf = getConfig();
          InetSocketAddress nmCollectorServiceAddress = conf.getSocketAddr(
              YarnConfiguration.NM_BIND_HOST,
              YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
              YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
              YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_PORT);
          LOG.info("nmCollectorServiceAddress: {}", nmCollectorServiceAddress);
          final YarnRPC rpc = YarnRPC.create(conf);

          // TODO Security settings.
          nmCollectorService = (CollectorNodemanagerProtocol) rpc.getProxy(
              CollectorNodemanagerProtocol.class,
              nmCollectorServiceAddress, conf);
        }
      }
    }
    return nmCollectorService;
  }

  @VisibleForTesting
  public String getRestServerBindAddress() {
    return timelineRestServerBindAddress;
  }

  private final class CollectorTokenRenewer implements Runnable {
    private ApplicationId appId;
    // Indicates whether timer is for renewal or regeneration of token.
    private boolean timerForRenewal = true;
    private CollectorTokenRenewer(ApplicationId applicationId,
        boolean forRenewal) {
      appId = applicationId;
      timerForRenewal = forRenewal;
    }

    private void renewToken(AppLevelTimelineCollector appCollector)
        throws IOException {
      long newExpirationTime = renewTokenForAppCollector(appCollector);
      // Set renewal or regeneration timer based on delay.
      long renewalDelay = 0;
      if (newExpirationTime > 0) {
        LOG.info("Renewed token for {} with new expiration " +
            "timestamp = {}", appId, newExpirationTime);
        renewalDelay = getRenewalDelay(newExpirationTime - Time.now());
      }
      long regenerationDelay =
          getRegenerationDelay(appCollector.getAppDelegationTokenMaxDate());
      if (renewalDelay > 0 || regenerationDelay > 0) {
        this.timerForRenewal = renewalDelay < regenerationDelay;
        Future<?> renewalOrRegenerationFuture = tokenRenewalExecutor.schedule(
            this, timerForRenewal ? renewalDelay : regenerationDelay,
            TimeUnit.MILLISECONDS);
        appCollector.setRenewalOrRegenerationFutureForApp(
            renewalOrRegenerationFuture);
      }
    }

    private void regenerateToken(AppLevelTimelineCollector appCollector)
        throws IOException {
      org.apache.hadoop.yarn.api.records.Token token =
          generateTokenAndSetTimer(appId, appCollector);
      // Report to NM if a new collector is added.
      try {
        reportNewCollectorInfoToNM(appId, token);
      } catch (YarnException e) {
        LOG.warn("Unable to report regenerated token to NM for {}", appId);
      }
    }

    @Override
    public void run() {
      TimelineCollector collector = get(appId);
      if (collector == null) {
        LOG.info("Cannot find active collector while {} token for {}",
            (timerForRenewal ? "renewing" : "regenerating"), appId);
        return;
      }
      AppLevelTimelineCollector appCollector =
          (AppLevelTimelineCollector) collector;

      synchronized (collector) {
        if (!collector.isStopped()) {
          try {
            if (timerForRenewal) {
              renewToken(appCollector);
            } else {
              regenerateToken(appCollector);
            }
          } catch (Exception e) {
            LOG.warn("Unable to {} token for {}",
                (timerForRenewal ? "renew" : "regenerate"), appId, e);
          }
        }
      }
    }
  }
}
