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
package org.apache.hadoop.yarn.server.nodemanager.collectormanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;

/**
 * Service that handles collector information. It is used only if the timeline
 * service v.2 is enabled.
 */
public class NMCollectorService extends CompositeService implements
    CollectorNodemanagerProtocol {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMCollectorService.class);

  private final Context context;

  private Server server;

  public NMCollectorService(Context context) {
    super(NMCollectorService.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();

    InetSocketAddress collectorServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_PORT);

    Configuration serverConf = new Configuration(conf);

    YarnRPC rpc = YarnRPC.create(conf);

    // Kerberos based authentication to be used for CollectorNodemanager
    // protocol if security is enabled.
    server =
        rpc.getServer(CollectorNodemanagerProtocol.class, this,
            collectorServerAddress, serverConf, null,
            conf.getInt(YarnConfiguration.NM_COLLECTOR_SERVICE_THREAD_COUNT,
                YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_THREAD_COUNT));

    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, NMPolicyProvider.getInstance());
    }

    server.start();
    collectorServerAddress = conf.updateConnectAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
        server.getListenerAddress());
    // start remaining services
    super.serviceStart();
    LOG.info("NMCollectorService started at " + collectorServerAddress);
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    // TODO may cleanup app collectors running on this NM in future.
    super.serviceStop();
  }

  @Override
  public ReportNewCollectorInfoResponse reportNewCollectorInfo(
      ReportNewCollectorInfoRequest request) throws YarnException, IOException {
    List<AppCollectorData> newCollectorsList = request.getAppCollectorsList();
    if (newCollectorsList != null && !newCollectorsList.isEmpty()) {
      Map<ApplicationId, AppCollectorData> newCollectorsMap =
          new HashMap<>();
      for (AppCollectorData collector : newCollectorsList) {
        ApplicationId appId = collector.getApplicationId();
        newCollectorsMap.put(appId, collector);
        // set registered collector address to TimelineClient.
        // TODO: Do we need to do this after we received confirmation from
        // the RM?
        NMTimelinePublisher nmTimelinePublisher =
            context.getNMTimelinePublisher();
        if (nmTimelinePublisher != null) {
          nmTimelinePublisher.setTimelineServiceAddress(appId,
              collector.getCollectorAddr());
        }
      }
      Map<ApplicationId, AppCollectorData> registeringCollectors
          = context.getRegisteringCollectors();
      if (registeringCollectors != null) {
        registeringCollectors.putAll(newCollectorsMap);
      } else {
        LOG.warn("collectors are added when the registered collectors are " +
            "initialized");
      }
    }

    return ReportNewCollectorInfoResponse.newInstance();
  }

  @Override
  public GetTimelineCollectorContextResponse getTimelineCollectorContext(
      GetTimelineCollectorContextRequest request)
      throws YarnException, IOException {
    Application app = context.getApplications().get(request.getApplicationId());
    if (app == null) {
      throw new YarnException("Application " + request.getApplicationId() +
          " doesn't exist on NM.");
    }
    return GetTimelineCollectorContextResponse.newInstance(
        app.getUser(), app.getFlowName(), app.getFlowVersion(),
        app.getFlowRunId());
  }
}
