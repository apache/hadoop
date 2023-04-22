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

import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.junit.Assert;

/**
 * This class mocks the ClientRequestInterceptor.
 */
public class MockClientRequestInterceptor
    extends DefaultClientRequestInterceptor {

  MockRM mockRM = null;

  public void init(String user) {
    mockRM = new MockRM(super.getConf()) {
      @Override
      protected ClientRMService createClientRMService() {
        return new ClientRMService(getRMContext(), getResourceScheduler(),
            rmAppManager, applicationACLsManager, queueACLsManager,
            getRMContext().getRMDelegationTokenSecretManager()) {
          @Override
          protected void serviceStart() {
            // override to not start rpc handler
          }

          @Override
          protected void serviceStop() {
            // don't do anything
          }

          @Override
          public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
              MoveApplicationAcrossQueuesRequest request) throws YarnException {
            return MoveApplicationAcrossQueuesResponse.newInstance();
          }
        };
      }
    };
    mockRM.init(super.getConf());
    mockRM.start();
    try {
      mockRM.registerNode("127.0.0.1:1", 102400, 100);
      // allow plan follower to synchronize
      Thread.sleep(1050);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    super.setRMClient(mockRM.getClientRMService());
  }

  @Override
  public void shutdown() {
    if (mockRM != null) {
      mockRM.stop();
    }
    super.shutdown();
  }
}
