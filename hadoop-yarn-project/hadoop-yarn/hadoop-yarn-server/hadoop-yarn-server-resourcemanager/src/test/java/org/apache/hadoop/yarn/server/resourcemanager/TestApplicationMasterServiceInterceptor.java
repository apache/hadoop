/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

/**
 * This class tests whether {@link ApplicationMasterServiceProcessor}s
 * work fine, e.g. allocation is invoked on preprocessor and the next processor
 * in the chain is also invoked.
 */
public class TestApplicationMasterServiceInterceptor {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestApplicationMasterServiceInterceptor.class);

  private static AtomicInteger beforeRegCount = new AtomicInteger(0);
  private static AtomicInteger afterRegCount = new AtomicInteger(0);
  private static AtomicInteger beforeAllocCount = new AtomicInteger(0);
  private static AtomicInteger afterAllocCount = new AtomicInteger(0);
  private static AtomicInteger beforeFinishCount = new AtomicInteger(0);
  private static AtomicInteger afterFinishCount = new AtomicInteger(0);
  private static AtomicInteger initCount = new AtomicInteger(0);

  static class TestInterceptor1 implements
      ApplicationMasterServiceProcessor {

    private ApplicationMasterServiceProcessor nextProcessor;

    @Override
    public void init(ApplicationMasterServiceContext amsContext,
        ApplicationMasterServiceProcessor next) {
      initCount.incrementAndGet();
      this.nextProcessor = next;
    }

    @Override
    public void registerApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        RegisterApplicationMasterRequest request,
        RegisterApplicationMasterResponse response)
        throws IOException, YarnException {
      nextProcessor.registerApplicationMaster(
          applicationAttemptId, request, response);
    }

    @Override
    public void allocate(ApplicationAttemptId appAttemptId,
        AllocateRequest request,
        AllocateResponse response) throws YarnException {
      beforeAllocCount.incrementAndGet();
      nextProcessor.allocate(appAttemptId, request, response);
      afterAllocCount.incrementAndGet();
    }

    @Override
    public void finishApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        FinishApplicationMasterRequest request,
        FinishApplicationMasterResponse response) {
      beforeFinishCount.incrementAndGet();
      afterFinishCount.incrementAndGet();
    }
  }

  static class TestInterceptor2 implements
      ApplicationMasterServiceProcessor {

    private ApplicationMasterServiceProcessor nextProcessor;

    @Override
    public void init(ApplicationMasterServiceContext amsContext,
        ApplicationMasterServiceProcessor next) {
      initCount.incrementAndGet();
      this.nextProcessor = next;
    }

    @Override
    public void registerApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        RegisterApplicationMasterRequest request,
        RegisterApplicationMasterResponse response)
        throws IOException, YarnException {
      beforeRegCount.incrementAndGet();
      nextProcessor.registerApplicationMaster(applicationAttemptId,
          request, response);
      afterRegCount.incrementAndGet();
    }

    @Override
    public void allocate(ApplicationAttemptId appAttemptId,
        AllocateRequest request, AllocateResponse response)
        throws YarnException {
      beforeAllocCount.incrementAndGet();
      nextProcessor.allocate(appAttemptId, request, response);
      afterAllocCount.incrementAndGet();
    }

    @Override
    public void finishApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        FinishApplicationMasterRequest request,
        FinishApplicationMasterResponse response) {
      beforeFinishCount.incrementAndGet();
      nextProcessor.finishApplicationMaster(
          applicationAttemptId, request, response);
      afterFinishCount.incrementAndGet();
    }
  }

  private static YarnConfiguration conf;
  private static final int GB = 1024;

  @Before
  public void setup() {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);
  }

  @Test(timeout = 300000)
  public void testApplicationMasterInterceptor() throws Exception {
    conf.set(YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        TestInterceptor1.class.getName() + ","
            + TestInterceptor2.class.getName());
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 6 * GB);

    // Submit an application
    RMApp app1 = MockRMAppSubmitter.submitWithMemory(2048, rm);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    int allocCount = 0;

    am1.addRequests(new String[] {"127.0.0.1"}, GB, 1, 1);
    AllocateResponse alloc1Response = am1.schedule(); // send the request
    allocCount++;

    // kick the scheduler
    nm1.nodeHeartbeat(true);
    while (alloc1Response.getAllocatedContainers().size() < 1) {
      LOG.info("Waiting for containers to be created for app 1...");
      sleep(1000);
      alloc1Response = am1.schedule();
      allocCount++;
    }

    // assert RMIdentifier is set properly in allocated containers
    Container allocatedContainer =
        alloc1Response.getAllocatedContainers().get(0);
    ContainerTokenIdentifier tokenId =
        BuilderUtils.newContainerTokenIdentifier(allocatedContainer
            .getContainerToken());
    am1.unregisterAppAttempt();

    Assert.assertEquals(1, beforeRegCount.get());
    Assert.assertEquals(1, afterRegCount.get());

    // The allocate calls should be incremented twice
    Assert.assertEquals(allocCount * 2, beforeAllocCount.get());
    Assert.assertEquals(allocCount * 2, afterAllocCount.get());

    // Finish should only be called once, since the FirstInterceptor
    // does not forward the call.
    Assert.assertEquals(1, beforeFinishCount.get());
    Assert.assertEquals(1, afterFinishCount.get());
    rm.stop();
  }
}
