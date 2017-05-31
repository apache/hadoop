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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the TestAMRMProxyService and overrides methods in order to use the
 * AMRMProxyService's pipeline test cases for testing the FederationInterceptor
 * class. The tests for AMRMProxyService has been written cleverly so that it
 * can be reused to validate different request intercepter chains.
 */
public class TestFederationInterceptor extends BaseAMRMProxyTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInterceptor.class);

  public static final String HOME_SC_ID = "SC-home";

  private TestableFederationInterceptor interceptor;

  private int testAppId;
  private ApplicationAttemptId attemptId;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    interceptor = new TestableFederationInterceptor();

    testAppId = 1;
    attemptId = getApplicationAttemptId(testAppId);
    interceptor.init(new AMRMProxyApplicationContextImpl(null, getConf(),
        attemptId, "test-user", null, null));
  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    String mockPassThroughInterceptorClass =
        PassThroughRequestInterceptor.class.getName();

    // Create a request intercepter pipeline for testing. The last one in the
    // chain is the federation intercepter that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + TestableFederationInterceptor.class.getName());

    conf.set(YarnConfiguration.RM_CLUSTER_ID, HOME_SC_ID);

    return conf;
  }

  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RequestInterceptor root =
        super.getAMRMProxyService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      switch (index) {
      case 0:
      case 1:
        Assert.assertEquals(PassThroughRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 2:
        Assert.assertEquals(TestableFederationInterceptor.class.getName(),
            root.getClass().getName());
        break;
      default:
        Assert.fail();
      }
      root = root.getNextInterceptor();
      index++;
    }
    Assert.assertEquals("The number of interceptors in chain does not match",
        Integer.toString(3), Integer.toString(index));
  }

  /**
   * Between AM and AMRMProxy, FederationInterceptor modifies the RM behavior,
   * so that when AM registers more than once, it returns the same register
   * success response instead of throwing
   * {@link InvalidApplicationMasterRequestException}
   *
   * We did this because FederationInterceptor can receive concurrent register
   * requests from AM because of timeout between AM and AMRMProxy. This can
   * possible since the timeout between FederationInterceptor and RM longer
   * because of performFailover + timeout.
   */
  @Test
  public void testTwoIdenticalRegisterRequest() throws Exception {
    // Register the application twice
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    for (int i = 0; i < 2; i++) {
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
    }
  }

  @Test
  public void testTwoDifferentRegisterRequest() throws Exception {
    // Register the application first time
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);

    // Register the application second time with a different request obj
    registerReq = Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("different");
    try {
      registerResponse = interceptor.registerApplicationMaster(registerReq);
      Assert.fail("Should throw if a different request obj is used");
    } catch (YarnException e) {
    }
  }
}
