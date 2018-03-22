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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestMemoryRMStateStore {

  @Test
  public void testNotifyStoreOperationFailed() throws Exception {
    RMStateStore store = new MemoryRMStateStore() {
      @Override
      public synchronized void removeRMDelegationTokenState(
          RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
        throw new Exception("testNotifyStoreOperationFailed");
      }
    };
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    store.init(conf);
    ResourceManager mockRM = mock(ResourceManager.class);
    store.setResourceManager(mockRM);
    store.setRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
    RMDelegationTokenIdentifier mockTokenId =
        mock(RMDelegationTokenIdentifier.class);
    store.removeRMDelegationToken(mockTokenId);
    assertTrue("RMStateStore should have been in fenced state",
        store.isFencedState());
    store = new MemoryRMStateStore() {
      @Override
      public synchronized void removeRMDelegationToken(
          RMDelegationTokenIdentifier rmDTIdentifier) {
        notifyStoreOperationFailed(new Exception(
            "testNotifyStoreOperationFailed"));
      }
    };
    store.init(conf);
    store.setResourceManager(mockRM);
    store.setRMDispatcher(new RMStateStoreTestBase.TestDispatcher());
    store.removeRMDelegationToken(mockTokenId);
    assertTrue("RMStateStore should have been in fenced state",
        store.isFencedState());
  }
}
