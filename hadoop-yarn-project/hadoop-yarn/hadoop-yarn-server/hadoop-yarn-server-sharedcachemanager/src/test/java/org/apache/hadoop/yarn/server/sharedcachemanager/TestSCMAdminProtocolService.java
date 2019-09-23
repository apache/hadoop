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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RunSharedCacheCleanerTaskResponsePBImpl;
import org.apache.hadoop.yarn.client.SCMAdmin;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic unit tests for the SCM Admin Protocol Service and SCMAdmin.
 */
public class TestSCMAdminProtocolService {

  static SCMAdminProtocolService service;
  static SCMAdminProtocol SCMAdminProxy;
  static SCMAdminProtocol mockAdmin;
  static SCMAdmin adminCLI;
  static SCMStore store;
  static CleanerService cleaner;
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @Before
  public void startUp() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCM_STORE_CLASS,
        InMemorySCMStore.class.getName());

    cleaner = mock(CleanerService.class);

    service = spy(new SCMAdminProtocolService(cleaner));
    service.init(conf);
    service.start();

    YarnRPC rpc = YarnRPC.create(new Configuration());

    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.SCM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_ADMIN_PORT);

    SCMAdminProxy =
        (SCMAdminProtocol) rpc.getProxy(SCMAdminProtocol.class, scmAddress,
            conf);

    mockAdmin = mock(SCMAdminProtocol.class);
    adminCLI = new SCMAdmin(new Configuration()) {
      @Override
      protected SCMAdminProtocol createSCMAdminProtocol() throws IOException {
        return mockAdmin;
      }
    };
  }

  @After
  public void cleanUpTest() {
    if (service != null) {
      service.stop();
    }

    if (SCMAdminProxy != null) {
      RPC.stopProxy(SCMAdminProxy);
    }
  }

  @Test
  public void testRunCleanerTask() throws Exception {
    doNothing().when(cleaner).runCleanerTask();
    RunSharedCacheCleanerTaskRequest request =
        recordFactory.newRecordInstance(RunSharedCacheCleanerTaskRequest.class);
    RunSharedCacheCleanerTaskResponse response = SCMAdminProxy.runCleanerTask(request);
    Assert.assertTrue("cleaner task request isn't accepted", response.getAccepted());
    verify(service, times(1)).runCleanerTask(any(RunSharedCacheCleanerTaskRequest.class));
  }

  @Test
  public void testRunCleanerTaskCLI() throws Exception {
    String[] args = { "-runCleanerTask" };
    RunSharedCacheCleanerTaskResponse rp =
        new RunSharedCacheCleanerTaskResponsePBImpl();
    rp.setAccepted(true);
    when(mockAdmin.runCleanerTask(isA(RunSharedCacheCleanerTaskRequest.class)))
        .thenReturn(rp);
    assertEquals(0, adminCLI.run(args));
    rp.setAccepted(false);
    when(mockAdmin.runCleanerTask(isA(RunSharedCacheCleanerTaskRequest.class)))
        .thenReturn(rp);
    assertEquals(1, adminCLI.run(args));
    verify(mockAdmin, times(2)).runCleanerTask(
        any(RunSharedCacheCleanerTaskRequest.class));
  }
}
