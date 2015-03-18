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
package org.apache.hadoop.yarn.server.nodemanager.aggregatormanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.CompositeService;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.AggregatorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.records.AppAggregatorsMap;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewAggregatorsInfoResponse;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;

public class NMAggregatorService extends CompositeService implements 
    AggregatorNodemanagerProtocol {

  private static final Log LOG = LogFactory.getLog(NMAggregatorService.class);

  final Context context;
  
  private Server server;

  public NMAggregatorService(Context context) {
    
    super(NMAggregatorService.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    
    InetSocketAddress aggregatorServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_AGGREGATOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_AGGREGATOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_AGGREGATOR_SERVICE_PORT);

    Configuration serverConf = new Configuration(conf);

    // TODO Security settings.
    YarnRPC rpc = YarnRPC.create(conf);

    server =
        rpc.getServer(AggregatorNodemanagerProtocol.class, this, 
            aggregatorServerAddress, serverConf,
            this.context.getNMTokenSecretManager(),
            conf.getInt(YarnConfiguration.NM_AGGREGATOR_SERVICE_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_AGGREGATOR_SERVICE_THREAD_COUNT));

    server.start();
    // start remaining services
    super.serviceStart();
    LOG.info("NMAggregatorService started at " + aggregatorServerAddress);
  }

  
  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    // TODO may cleanup app aggregators running on this NM in future.
    super.serviceStop();
  }

  @Override
  public ReportNewAggregatorsInfoResponse reportNewAggregatorInfo(
      ReportNewAggregatorsInfoRequest request) throws IOException {
    List<AppAggregatorsMap> newAggregatorsList = request.getAppAggregatorsList();
    if (newAggregatorsList != null && !newAggregatorsList.isEmpty()) {
      Map<ApplicationId, String> newAggregatorsMap = 
          new HashMap<ApplicationId, String>();
      for (AppAggregatorsMap aggregator : newAggregatorsList) {
        newAggregatorsMap.put(aggregator.getApplicationId(), aggregator.getAggregatorAddr());
      }
      ((NodeManager.NMContext)context).addRegisteredAggregators(newAggregatorsMap);
    }
    
    return ReportNewAggregatorsInfoResponse.newInstance();
  }
  
}
