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

package org.apache.hadoop.yarn.server.resourcemanager.security;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.junit.Test;

/**
 * This test replicates the condition os MAPREDUCE-3431 -a failure
 * during startup triggered an NPE during shutdown
 *
 */
public class TestDelegationTokenRenewerLifecycle {

  @Test
  public void testStartupFailure() throws Exception {
    Configuration conf = new Configuration();
    DelegationTokenRenewer delegationTokenRenewer =
        new DelegationTokenRenewer();
    RMContext mockContext = mock(RMContext.class);
    ClientRMService mockClientRMService = mock(ClientRMService.class);
    when(mockContext.getClientRMService()).thenReturn(mockClientRMService);
    delegationTokenRenewer.setRMContext(mockContext);
    delegationTokenRenewer.init(conf);
    delegationTokenRenewer.stop();
  }
}
