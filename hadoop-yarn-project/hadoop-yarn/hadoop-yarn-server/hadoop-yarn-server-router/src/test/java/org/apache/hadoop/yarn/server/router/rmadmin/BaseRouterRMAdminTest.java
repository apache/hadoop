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
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class for all the RouterRMAdminService test cases. It provides utility
 * methods that can be used by the concrete test case classes.
 *
 */
public abstract class BaseRouterRMAdminTest {

  /**
   * The RouterRMAdminService instance that will be used by all the test cases.
   */
  private MockRouterRMAdminService rmAdminService;
  /**
   * Thread pool used for asynchronous operations.
   */
  private static ExecutorService threadpool = Executors.newCachedThreadPool();
  private Configuration conf;
  private AsyncDispatcher dispatcher;

  public final static int TEST_MAX_CACHE_SIZE = 10;

  protected MockRouterRMAdminService getRouterRMAdminService() {
    Assert.assertNotNull(this.rmAdminService);
    return this.rmAdminService;
  }

  @Before
  public void setUp() {
    this.conf = new YarnConfiguration();
    String mockPassThroughInterceptorClass =
        PassThroughRMAdminRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain will call the mock resource manager. The others in the chain will
    // simply forward it to the next one in the chain
    this.conf.set(YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + mockPassThroughInterceptorClass + ","
            + MockRMAdminRequestInterceptor.class.getName());

    this.conf.setInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
        TEST_MAX_CACHE_SIZE);

    this.dispatcher = new AsyncDispatcher();
    this.dispatcher.init(conf);
    this.dispatcher.start();
    this.rmAdminService = createAndStartRouterRMAdminService();
  }

  @After
  public void tearDown() {
    if (rmAdminService != null) {
      rmAdminService.stop();
      rmAdminService = null;
    }
    if (this.dispatcher != null) {
      this.dispatcher.stop();
    }
  }

  protected ExecutorService getThreadPool() {
    return threadpool;
  }

  protected MockRouterRMAdminService createAndStartRouterRMAdminService() {
    MockRouterRMAdminService svc = new MockRouterRMAdminService();
    svc.init(conf);
    svc.start();
    return svc;
  }

  protected static class MockRouterRMAdminService extends RouterRMAdminService {
    public MockRouterRMAdminService() {
      super();
    }
  }

  protected RefreshQueuesResponse refreshQueues(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RefreshQueuesResponse>() {
          @Override
          public RefreshQueuesResponse run() throws Exception {
            RefreshQueuesRequest req = RefreshQueuesRequest.newInstance();
            RefreshQueuesResponse response =
                getRouterRMAdminService().refreshQueues(req);
            return response;
          }
        });
  }

  protected RefreshNodesResponse refreshNodes(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RefreshNodesResponse>() {
          @Override
          public RefreshNodesResponse run() throws Exception {
            RefreshNodesRequest req = RefreshNodesRequest.newInstance();
            RefreshNodesResponse response =
                getRouterRMAdminService().refreshNodes(req);
            return response;
          }
        });
  }

  protected RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      String user) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<RefreshSuperUserGroupsConfigurationResponse>() {
          @Override
          public RefreshSuperUserGroupsConfigurationResponse run()
              throws Exception {
            RefreshSuperUserGroupsConfigurationRequest req =
                RefreshSuperUserGroupsConfigurationRequest.newInstance();
            RefreshSuperUserGroupsConfigurationResponse response =
                getRouterRMAdminService()
                    .refreshSuperUserGroupsConfiguration(req);
            return response;
          }
        });
  }

  protected RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      String user) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<RefreshUserToGroupsMappingsResponse>() {
          @Override
          public RefreshUserToGroupsMappingsResponse run() throws Exception {
            RefreshUserToGroupsMappingsRequest req =
                RefreshUserToGroupsMappingsRequest.newInstance();
            RefreshUserToGroupsMappingsResponse response =
                getRouterRMAdminService().refreshUserToGroupsMappings(req);
            return response;
          }
        });
  }

  protected RefreshAdminAclsResponse refreshAdminAcls(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RefreshAdminAclsResponse>() {
          @Override
          public RefreshAdminAclsResponse run() throws Exception {
            RefreshAdminAclsRequest req = RefreshAdminAclsRequest.newInstance();
            RefreshAdminAclsResponse response =
                getRouterRMAdminService().refreshAdminAcls(req);
            return response;
          }
        });
  }

  protected RefreshServiceAclsResponse refreshServiceAcls(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RefreshServiceAclsResponse>() {
          @Override
          public RefreshServiceAclsResponse run() throws Exception {
            RefreshServiceAclsRequest req =
                RefreshServiceAclsRequest.newInstance();
            RefreshServiceAclsResponse response =
                getRouterRMAdminService().refreshServiceAcls(req);
            return response;
          }
        });
  }

  protected UpdateNodeResourceResponse updateNodeResource(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<UpdateNodeResourceResponse>() {
          @Override
          public UpdateNodeResourceResponse run() throws Exception {
            UpdateNodeResourceRequest req =
                UpdateNodeResourceRequest.newInstance(null);
            UpdateNodeResourceResponse response =
                getRouterRMAdminService().updateNodeResource(req);
            return response;
          }
        });
  }

  protected RefreshNodesResourcesResponse refreshNodesResources(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<RefreshNodesResourcesResponse>() {
          @Override
          public RefreshNodesResourcesResponse run() throws Exception {
            RefreshNodesResourcesRequest req =
                RefreshNodesResourcesRequest.newInstance();
            RefreshNodesResourcesResponse response =
                getRouterRMAdminService().refreshNodesResources(req);
            return response;
          }
        });
  }

  protected AddToClusterNodeLabelsResponse addToClusterNodeLabels(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<AddToClusterNodeLabelsResponse>() {
          @Override
          public AddToClusterNodeLabelsResponse run() throws Exception {
            AddToClusterNodeLabelsRequest req =
                AddToClusterNodeLabelsRequest.newInstance(null);
            AddToClusterNodeLabelsResponse response =
                getRouterRMAdminService().addToClusterNodeLabels(req);
            return response;
          }
        });
  }

  protected RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      String user) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<RemoveFromClusterNodeLabelsResponse>() {
          @Override
          public RemoveFromClusterNodeLabelsResponse run() throws Exception {
            RemoveFromClusterNodeLabelsRequest req =
                RemoveFromClusterNodeLabelsRequest.newInstance(null);
            RemoveFromClusterNodeLabelsResponse response =
                getRouterRMAdminService().removeFromClusterNodeLabels(req);
            return response;
          }
        });
  }

  protected ReplaceLabelsOnNodeResponse replaceLabelsOnNode(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<ReplaceLabelsOnNodeResponse>() {
          @Override
          public ReplaceLabelsOnNodeResponse run() throws Exception {
            ReplaceLabelsOnNodeRequest req = ReplaceLabelsOnNodeRequest
                .newInstance(new HashMap<NodeId, Set<String>>());
            ReplaceLabelsOnNodeResponse response =
                getRouterRMAdminService().replaceLabelsOnNode(req);
            return response;
          }
        });
  }

  protected CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      String user) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<CheckForDecommissioningNodesResponse>() {
          @Override
          public CheckForDecommissioningNodesResponse run() throws Exception {
            CheckForDecommissioningNodesRequest req =
                CheckForDecommissioningNodesRequest.newInstance();
            CheckForDecommissioningNodesResponse response =
                getRouterRMAdminService().checkForDecommissioningNodes(req);
            return response;
          }
        });
  }

  protected RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      String user) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user).doAs(
        new PrivilegedExceptionAction<RefreshClusterMaxPriorityResponse>() {
          @Override
          public RefreshClusterMaxPriorityResponse run() throws Exception {
            RefreshClusterMaxPriorityRequest req =
                RefreshClusterMaxPriorityRequest.newInstance();
            RefreshClusterMaxPriorityResponse response =
                getRouterRMAdminService().refreshClusterMaxPriority(req);
            return response;
          }
        });
  }

  protected String[] getGroupsForUser(String user)
      throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
        .doAs(new PrivilegedExceptionAction<String[]>() {
          @Override
          public String[] run() throws Exception {
            String[] response =
                getRouterRMAdminService().getGroupsForUser(user);
            return response;
          }
        });
  }

}
