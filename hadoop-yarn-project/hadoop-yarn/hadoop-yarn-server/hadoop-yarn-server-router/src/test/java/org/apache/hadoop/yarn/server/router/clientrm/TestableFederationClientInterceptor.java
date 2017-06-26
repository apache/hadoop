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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * Extends the FederationClientInterceptor and overrides methods to provide a
 * testable implementation of FederationClientInterceptor.
 */
public class TestableFederationClientInterceptor
    extends FederationClientInterceptor {

  private ConcurrentHashMap<SubClusterId, MockResourceManagerFacade> mockRMs =
      new ConcurrentHashMap<>();

  private List<SubClusterId> badSubCluster = new ArrayList<SubClusterId>();

  @Override
  protected ApplicationClientProtocol getClientRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    MockResourceManagerFacade mockRM = null;
    synchronized (this) {
      if (mockRMs.containsKey(subClusterId)) {
        mockRM = mockRMs.get(subClusterId);
      } else {
        mockRM = new MockResourceManagerFacade(super.getConf(), 0,
            Integer.parseInt(subClusterId.getId()),
            !badSubCluster.contains(subClusterId));
        mockRMs.put(subClusterId, mockRM);

      }
      return mockRM;
    }
  }

  /**
   * For testing purpose, some subclusters has to be down to simulate particular
   * scenarios as RM Failover, network issues. For this reason we keep track of
   * these bad subclusters. This method make the subcluster unusable.
   *
   * @param badSC the subcluster to make unusable
   */
  protected void registerBadSubCluster(SubClusterId badSC) {
    badSubCluster.add(badSC);
    if (mockRMs.contains(badSC)) {
      mockRMs.get(badSC).setRunningMode(false);
    }
  }

}
