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

package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_CLUSTER_ID;

public class TestableFederationRMAdminInterceptor extends FederationRMAdminInterceptor {

  // Record log information
  private static final Logger LOG =
      LoggerFactory.getLogger(TestableFederationRMAdminInterceptor.class);

  // Used to Store the relationship between SubClusterId and RM
  private ConcurrentHashMap<SubClusterId, MockRM> mockRMs = new ConcurrentHashMap<>();

  // Store Bad subCluster
  private Set<SubClusterId> badSubCluster = new HashSet<>();

  @Override
  protected ResourceManagerAdministrationProtocol getAdminRMProxyForSubCluster(
      SubClusterId subClusterId) throws Exception {
    MockRM mockRM;
    synchronized (this) {
      if (mockRMs.containsKey(subClusterId)) {
        mockRM = mockRMs.get(subClusterId);
      } else {
        YarnConfiguration config = new YarnConfiguration(super.getConf());
        config.set(RM_CLUSTER_ID, "subcluster." + subClusterId);
        mockRM = new MockRM(config);
        if (badSubCluster.contains(subClusterId)) {
          return new MockRMAdminBadService(mockRM);
        }
        mockRM.init(config);
        mockRM.start();
        mockRM.registerNode("127.0.0.1:1", 102400, 100);
        mockRMs.put(subClusterId, mockRM);
      }
      return mockRM.getAdminService();
    }
  }

  // This represents an unserviceable SubCluster
  private class MockRMAdminBadService extends AdminService {
    MockRMAdminBadService(ResourceManager rm) {
      super(rm);
    }

    @Override
    public void refreshQueues() throws IOException, YarnException {
      throw new ConnectException("RM is stopped");
    }
  }

  @Override
  public void shutdown() {
    // if mockRMs is not null
    if (MapUtils.isNotEmpty(mockRMs)) {
      for (Map.Entry<SubClusterId, MockRM> item : mockRMs.entrySet()) {
        SubClusterId subClusterId = item.getKey();
        // close mockRM.
        MockRM mockRM = item.getValue();
        if (mockRM != null) {
          LOG.info("subClusterId = {} mockRM shutdown.", subClusterId);
          mockRM.stop();
        }
      }
    }
    mockRMs.clear();
    super.shutdown();
  }
}
