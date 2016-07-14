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

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class on the NodeManager side that manages adding and removing collectors and
 * their lifecycle. Also instantiates the per-node collector webapp.
 */
@Private
@Unstable
public class NodeTimelineCollectorManager extends TimelineCollectorManager {
  private static final Log LOG =
      LogFactory.getLog(NodeTimelineCollectorManager.class);

  // REST server for this collector manager.
  private HttpServer2 timelineRestServer;

  private String timelineRestServerBindAddress;

  private volatile CollectorNodemanagerProtocol nmCollectorService;

  static final String COLLECTOR_MANAGER_ATTR_KEY = "collector.manager";

  @VisibleForTesting
  protected NodeTimelineCollectorManager() {
    super(NodeTimelineCollectorManager.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    startWebApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (timelineRestServer != null) {
      timelineRestServer.stop();
    }
    super.serviceStop();
  }

  @Override
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
    try {
      // Get context info from NM
      updateTimelineCollectorContext(appId, collector);
      // Report to NM if a new collector is added.
      reportNewCollectorToNM(appId);
    } catch (YarnException | IOException e) {
      // throw exception here as it cannot be used if failed communicate with NM
      LOG.error("Failed to communicate with NM Collector Service for " + appId);
      throw new YarnRuntimeException(e);
    }
  }

  /**
   * Launch the REST web server for this collector manager.
   */
  private void startWebApp() {
    Configuration conf = getConfig();
    String bindAddress = conf.get(YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_BIND_HOST) + ":0";
    try {
      HttpServer2.Builder builder = new HttpServer2.Builder()
          .setName("timeline")
          .setConf(conf)
          .addEndpoint(URI.create(
              (YarnConfiguration.useHttps(conf) ? "https://" : "http://") +
                  bindAddress));
      timelineRestServer = builder.build();
      // TODO: replace this by an authentication filter in future.
      HashMap<String, String> options = new HashMap<>();
      String username = conf.get(HADOOP_HTTP_STATIC_USER,
          DEFAULT_HADOOP_HTTP_STATIC_USER);
      options.put(HADOOP_HTTP_STATIC_USER, username);
      HttpServer2.defineFilter(timelineRestServer.getWebAppContext(),
          "static_user_filter_timeline",
          StaticUserWebFilter.StaticUserFilter.class.getName(),
          options, new String[] {"/*"});

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
    LOG.info("Instantiated the per-node collector webapp at " +
        timelineRestServerBindAddress);
  }

  private void reportNewCollectorToNM(ApplicationId appId)
      throws YarnException, IOException {
    ReportNewCollectorInfoRequest request =
        ReportNewCollectorInfoRequest.newInstance(appId,
            this.timelineRestServerBindAddress);
    LOG.info("Report a new collector for application: " + appId +
        " to the NM Collector Service.");
    getNMCollectorService().reportNewCollectorInfo(request);
  }

  private void updateTimelineCollectorContext(
      ApplicationId appId, TimelineCollector collector)
      throws YarnException, IOException {
    GetTimelineCollectorContextRequest request =
        GetTimelineCollectorContextRequest.newInstance(appId);
    LOG.info("Get timeline collector context for " + appId);
    GetTimelineCollectorContextResponse response =
        getNMCollectorService().getTimelineCollectorContext(request);
    String userId = response.getUserId();
    if (userId != null && !userId.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting the user in the context: " + userId);
      }
      collector.getTimelineEntityContext().setUserId(userId);
    }
    String flowName = response.getFlowName();
    if (flowName != null && !flowName.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting the flow name: " + flowName);
      }
      collector.getTimelineEntityContext().setFlowName(flowName);
    }
    String flowVersion = response.getFlowVersion();
    if (flowVersion != null && !flowVersion.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting the flow version: " + flowVersion);
      }
      collector.getTimelineEntityContext().setFlowVersion(flowVersion);
    }
    long flowRunId = response.getFlowRunId();
    if (flowRunId != 0L) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting the flow run id: " + flowRunId);
      }
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
          LOG.info("nmCollectorServiceAddress: " + nmCollectorServiceAddress);
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
}
