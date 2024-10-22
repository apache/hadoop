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
package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.impl.pb.client.ResourceManagerAdministrationProtocolPBClientImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test ResourceManagerAdministrationProtocolPBClientImpl. Test a methods and the proxy without  logic.
 */
public class TestResourceManagerAdministrationProtocolPBClientImpl {
  private static ResourceManager resourceManager;
  private static final Logger LOG = LoggerFactory
          .getLogger(TestResourceManagerAdministrationProtocolPBClientImpl.class);
  private final RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  private static ResourceManagerAdministrationProtocol client;

  /**
   * Start resource manager server
   */

  @BeforeClass
  public static void setUpResourceManager() throws IOException,
          InterruptedException {
    Configuration.addDefaultResource("config-with-security.xml");
    Configuration configuration = new YarnConfiguration();
    resourceManager = new ResourceManager() {
      @Override
      protected void doSecureLogin() throws IOException {
      }
    };

    // a reliable way to wait for resource manager to fully start
    final CountDownLatch rmStartedSignal = new CountDownLatch(1);
    ServiceStateChangeListener rmStateChangeListener =
        new ServiceStateChangeListener() {
          @Override
          public void stateChanged(Service service) {
            if (service.getServiceState() == STATE.STARTED) {
              rmStartedSignal.countDown();
            }
          }
        };
    resourceManager.registerServiceListener(rmStateChangeListener);

    resourceManager.init(configuration);
    new Thread() {
      public void run() {
        resourceManager.start();
      }
    }.start();

    boolean rmStarted = rmStartedSignal.await(60000L, TimeUnit.MILLISECONDS);
    Assert.assertTrue("ResourceManager failed to start up.", rmStarted);

    LOG.info("ResourceManager RMAdmin address: {}.",
        configuration.get(YarnConfiguration.RM_ADMIN_ADDRESS));

    client = new ResourceManagerAdministrationProtocolPBClientImpl(1L,
            getProtocolAddress(configuration), configuration);

  }

  /**
   * Test method refreshQueues. This method is present and it works.
   */
  @Test
  public void testRefreshQueues() throws Exception {

    RefreshQueuesRequest request = recordFactory
            .newRecordInstance(RefreshQueuesRequest.class);
    RefreshQueuesResponse response = client.refreshQueues(request);
    assertNotNull(response);
  }

  /**
   * Test method refreshNodes. This method is present and it works.
   */

  @Test
  public void testRefreshNodes() throws Exception {
    resourceManager.getClientRMService();
    RefreshNodesRequest request = RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL);
    RefreshNodesResponse response = client.refreshNodes(request);
    assertNotNull(response);
  }

  /**
   * Test method refreshSuperUserGroupsConfiguration. This method present and it works.
   */
  @Test
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {

    RefreshSuperUserGroupsConfigurationRequest request = recordFactory
            .newRecordInstance(RefreshSuperUserGroupsConfigurationRequest.class);
    RefreshSuperUserGroupsConfigurationResponse response = client
            .refreshSuperUserGroupsConfiguration(request);
    assertNotNull(response);
  }

  /**
   * Test method refreshUserToGroupsMappings. This method is present and it works.
   */
  @Test
  public void testRefreshUserToGroupsMappings() throws Exception {
    RefreshUserToGroupsMappingsRequest request = recordFactory
            .newRecordInstance(RefreshUserToGroupsMappingsRequest.class);
    RefreshUserToGroupsMappingsResponse response = client
            .refreshUserToGroupsMappings(request);
    assertNotNull(response);
  }

  /**
   * Test method refreshAdminAcls. This method is present and it works.
   */

  @Test
  public void testRefreshAdminAcls() throws Exception {
    RefreshAdminAclsRequest request = recordFactory
            .newRecordInstance(RefreshAdminAclsRequest.class);
    RefreshAdminAclsResponse response = client.refreshAdminAcls(request);
    assertNotNull(response);
  }
  
  @Test
  public void testUpdateNodeResource() throws Exception {
    UpdateNodeResourceRequest request = recordFactory
            .newRecordInstance(UpdateNodeResourceRequest.class);
    UpdateNodeResourceResponse response = client.updateNodeResource(request);
    assertNotNull(response);
  }

  @Test
  public void testRefreshServiceAcls() throws Exception {
    RefreshServiceAclsRequest request = recordFactory
            .newRecordInstance(RefreshServiceAclsRequest.class);
    RefreshServiceAclsResponse response = client.refreshServiceAcls(request);
    assertNotNull(response);

  }

  /**
   * Stop server
   */

  @AfterClass
  public static void tearDownResourceManager() throws InterruptedException {
    if (resourceManager != null) {
      LOG.info("Stopping ResourceManager...");
      resourceManager.stop();
    }
  }

  private static InetSocketAddress getProtocolAddress(Configuration conf)
          throws IOException {
    return conf.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }

}
