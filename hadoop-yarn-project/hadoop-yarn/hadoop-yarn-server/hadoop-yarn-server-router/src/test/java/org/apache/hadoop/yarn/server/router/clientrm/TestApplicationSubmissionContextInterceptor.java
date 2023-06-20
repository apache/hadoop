/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router.clientrm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.junit.Test;

/**
 * Extends the {@code BaseRouterClientRMTest} and overrides methods in order to
 * use the {@code RouterClientRMService} pipeline test cases for testing the
 * {@code ApplicationSubmissionContextInterceptor} class. The tests for
 * {@code RouterClientRMService} has been written cleverly so that it can be
 * reused to validate different request interceptor chains.
 */
public class TestApplicationSubmissionContextInterceptor extends BaseRouterClientRMTest {

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    String mockPassThroughInterceptorClass =
        PassThroughClientRequestInterceptor.class.getName();

    // Create a request interceptor pipeline for testing. The last one in the
    // chain is the application submission context interceptor that checks
    // for exceeded submission context size
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," +
        ApplicationSubmissionContextInterceptor.class.getName() + "," +
        MockClientRequestInterceptor.class.getName());

    // Lower the max application context size
    conf.set(YarnConfiguration.ROUTER_ASC_INTERCEPTOR_MAX_SIZE, "512B");

    return conf;
  }

  /**
   * This test validates the correctness of SubmitApplication in case of empty
   * request.
   * @throws Exception error occur.
   */
  @Test
  public void testSubmitApplicationEmptyRequest() throws Exception {

    MockRouterClientRMService rmService = getRouterClientRMService();
    LambdaTestUtils.intercept(YarnException.class,
        "Missing submitApplication request or applicationSubmissionContext information.",
        () -> rmService.submitApplication(null));

    ApplicationSubmissionContext context = ApplicationSubmissionContext.newInstance(
        null, "", "", null, null, false, false, -1, null, null);
    SubmitApplicationRequest request = SubmitApplicationRequest.newInstance(context);
    LambdaTestUtils.intercept(YarnException.class,
        "Missing submitApplication request or applicationSubmissionContext information.",
        () -> rmService.submitApplication(null));
  }

  /**
   * This test validates the correctness of SubmitApplication by setting up
   * null, valid, and large ContainerLaunchContexts.
   * @throws Exception error occur.
   */
  @Test
  public void testCLCExceedSize() throws Exception {

    ApplicationSubmissionContext context = ApplicationSubmissionContext.newInstance(
        ApplicationId.newInstance(1, 1), "test", "default",
        Priority.newInstance(0), null, false, true, 2,
        Resource.newInstance(10, 2), "test");

    LocalResource localResource = LocalResource.newInstance(
        URL.newInstance("hdfs", "somehost", 12345, "/some/path/to/rsrc"),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, 123L,
        1234567890L);

    Map<String, LocalResource> localResources = new HashMap<>();
    localResources.put("rsrc", localResource);

    Map<String, String> env = new HashMap<>();
    env.put("somevar", "someval");

    List<String> containerCmds = new ArrayList<>();
    containerCmds.add("somecmd");
    containerCmds.add("somearg");

    Map<String, ByteBuffer> serviceData = new HashMap<>();
    serviceData.put("someservice", ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3}));
    ByteBuffer containerTokens = ByteBuffer.wrap(new byte[] {0x7, 0x8, 0x9, 0xa});

    Map<ApplicationAccessType, String> acls = new HashMap<>();
    acls.put(ApplicationAccessType.VIEW_APP, "viewuser");
    acls.put(ApplicationAccessType.MODIFY_APP, "moduser");
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, env, containerCmds, serviceData, containerTokens, acls);
    ApplicationSubmissionContextPBImpl appSubmissionContextPB =
        (ApplicationSubmissionContextPBImpl) context;
    Configuration configuration = getConf();

    // Null ApplicationSubmissionContext
    RouterServerUtil.checkAppSubmissionContext(null, configuration);

    // Null ContainerLaunchContext
    RouterServerUtil.checkAppSubmissionContext(appSubmissionContextPB, configuration);

    // Valid ContainerLaunchContext
    context.setAMContainerSpec(clc);
    RouterServerUtil.checkAppSubmissionContext(appSubmissionContextPB, configuration);

    // ContainerLaunchContext exceeds 1MB
    for (int i = 0; i < 1000; i++) {
      localResources.put("rsrc" + i, localResource);
    }
    ContainerLaunchContext clcExceedSize = ContainerLaunchContext.newInstance(
        localResources, env, containerCmds, serviceData, containerTokens, acls);
    context.setAMContainerSpec(clcExceedSize);
    LambdaTestUtils.intercept(YarnException.class,
        "The size of the ApplicationSubmissionContext of the application",
        () -> RouterServerUtil.checkAppSubmissionContext(appSubmissionContextPB, configuration));
  }
}
