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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService.RequestInterceptorChainWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to validate the RMAdmin Service inside the Router.
 */
public class TestRouterRMAdminService extends BaseRouterRMAdminTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRMAdminService.class);

  /**
   * Tests if the pipeline is created properly.
   */
  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RMAdminRequestInterceptor root =
        super.getRouterRMAdminService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      // The current pipeline is:
      // PassThroughRMAdminRequestInterceptor - index = 0
      // PassThroughRMAdminRequestInterceptor - index = 1
      // PassThroughRMAdminRequestInterceptor - index = 2
      // MockClientRequestInterceptor - index = 3
      switch (index) {
      case 0: // Fall to the next case
      case 1: // Fall to the next case
      case 2:
        // If index is equal to 0,1 or 2 we fall in this check
        Assert.assertEquals(
            PassThroughRMAdminRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 3:
        Assert.assertEquals(MockRMAdminRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      default:
        Assert.fail();
      }
      root = root.getNextInterceptor();
      index++;
    }
    Assert.assertEquals("The number of interceptors in chain does not match", 4,
        index);
  }

  /**
   * Test if the RouterRMAdmin forwards all the requests to the MockRM and get
   * back the responses.
   */
  @Test
  public void testRouterRMAdminServiceE2E() throws Exception {

    String user = "test1";

    LOG.info("testRouterRMAdminServiceE2E - Refresh Queues");

    RefreshQueuesResponse responseRefreshQueues = refreshQueues(user);
    Assert.assertNotNull(responseRefreshQueues);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Nodes");

    RefreshNodesResponse responseRefreshNodes = refreshNodes(user);
    Assert.assertNotNull(responseRefreshNodes);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Super User");

    RefreshSuperUserGroupsConfigurationResponse responseRefreshSuperUser =
        refreshSuperUserGroupsConfiguration(user);
    Assert.assertNotNull(responseRefreshSuperUser);

    LOG.info("testRouterRMAdminServiceE2E - Refresh User to Group");

    RefreshUserToGroupsMappingsResponse responseRefreshUserToGroup =
        refreshUserToGroupsMappings(user);
    Assert.assertNotNull(responseRefreshUserToGroup);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Admin Acls");

    RefreshAdminAclsResponse responseRefreshAdminAcls = refreshAdminAcls(user);
    Assert.assertNotNull(responseRefreshAdminAcls);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Service Acls");

    RefreshServiceAclsResponse responseRefreshServiceAcls =
        refreshServiceAcls(user);
    Assert.assertNotNull(responseRefreshServiceAcls);

    LOG.info("testRouterRMAdminServiceE2E - Update Node Resource");

    UpdateNodeResourceResponse responseUpdateNodeResource =
        updateNodeResource(user);
    Assert.assertNotNull(responseUpdateNodeResource);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Nodes Resource");

    RefreshNodesResourcesResponse responseRefreshNodesResources =
        refreshNodesResources(user);
    Assert.assertNotNull(responseRefreshNodesResources);

    LOG.info("testRouterRMAdminServiceE2E - Add To Cluster NodeLabels");

    AddToClusterNodeLabelsResponse responseAddToClusterNodeLabels =
        addToClusterNodeLabels(user);
    Assert.assertNotNull(responseAddToClusterNodeLabels);

    LOG.info("testRouterRMAdminServiceE2E - Remove To Cluster NodeLabels");

    RemoveFromClusterNodeLabelsResponse responseRemoveFromClusterNodeLabels =
        removeFromClusterNodeLabels(user);
    Assert.assertNotNull(responseRemoveFromClusterNodeLabels);

    LOG.info("testRouterRMAdminServiceE2E - Replace Labels On Node");

    ReplaceLabelsOnNodeResponse responseReplaceLabelsOnNode =
        replaceLabelsOnNode(user);
    Assert.assertNotNull(responseReplaceLabelsOnNode);

    LOG.info("testRouterRMAdminServiceE2E - Check For Decommissioning Nodes");

    CheckForDecommissioningNodesResponse responseCheckForDecom =
        checkForDecommissioningNodes(user);
    Assert.assertNotNull(responseCheckForDecom);

    LOG.info("testRouterRMAdminServiceE2E - Refresh Cluster Max Priority");

    RefreshClusterMaxPriorityResponse responseRefreshClusterMaxPriority =
        refreshClusterMaxPriority(user);
    Assert.assertNotNull(responseRefreshClusterMaxPriority);

    LOG.info("testRouterRMAdminServiceE2E - Get Groups For User");

    String[] responseGetGroupsForUser = getGroupsForUser(user);
    Assert.assertNotNull(responseGetGroupsForUser);

  }

  /**
   * Test if the different chains for users are generated, and LRU cache is
   * working as expected.
   */
  @Test
  public void testUsersChainMapWithLRUCache()
      throws YarnException, IOException, InterruptedException {

    Map<String, RequestInterceptorChainWrapper> pipelines;
    RequestInterceptorChainWrapper chain;

    refreshQueues("test1");
    refreshQueues("test2");
    refreshQueues("test3");
    refreshQueues("test4");
    refreshQueues("test5");
    refreshQueues("test6");
    refreshQueues("test7");
    refreshQueues("test8");

    pipelines = super.getRouterRMAdminService().getPipelines();
    Assert.assertEquals(8, pipelines.size());

    refreshQueues("test9");
    refreshQueues("test10");
    refreshQueues("test1");
    refreshQueues("test11");

    // The cache max size is defined in
    // BaseRouterClientRMTest.TEST_MAX_CACHE_SIZE
    Assert.assertEquals(10, pipelines.size());

    chain = pipelines.get("test1");
    Assert.assertNotNull("test1 should not be evicted", chain);

    chain = pipelines.get("test2");
    Assert.assertNull("test2 should have been evicted", chain);
  }

}
