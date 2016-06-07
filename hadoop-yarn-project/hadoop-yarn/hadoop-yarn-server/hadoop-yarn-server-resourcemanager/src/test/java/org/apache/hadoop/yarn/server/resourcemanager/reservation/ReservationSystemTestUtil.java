/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ReservationSystemTestUtil {

  private static Random rand = new Random();

  public final static String reservationQ = "dedicated";

  public static ReservationId getNewReservationId() {
    return ReservationId.newInstance(rand.nextLong(), rand.nextLong());
  }

  public static ReservationSchedulerConfiguration createConf(
      String reservationQ, long timeWindow, float instConstraint,
      float avgConstraint) {

    ReservationSchedulerConfiguration realConf = new CapacitySchedulerConfiguration();
    ReservationSchedulerConfiguration conf = spy(realConf);
    when(conf.getReservationWindow(reservationQ)).thenReturn(timeWindow);
    when(conf.getInstantaneousMaxCapacity(reservationQ))
        .thenReturn(instConstraint);
    when(conf.getAverageCapacity(reservationQ)).thenReturn(avgConstraint);

    return conf;
  }

  public static void validateReservationQueue(
      AbstractReservationSystem reservationSystem, String planQName) {
    Plan plan = reservationSystem.getPlan(planQName);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan instanceof InMemoryPlan);
    Assert.assertEquals(planQName, plan.getQueueName());
    Assert.assertEquals(8192, plan.getTotalCapacity().getMemorySize());
    Assert.assertTrue(
        plan.getReservationAgent() instanceof AlignedPlannerWithGreedy);
    Assert
        .assertTrue(plan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }

  public static void setupFSAllocationFile(String allocationFile)
      throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(allocationFile));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<weight>1</weight>");
    out.println("</queue>");
    out.println("<queue name=\"a\">");
    out.println("<weight>1</weight>");
    out.println("<queue name=\"a1\">");
    out.println("<weight>3</weight>");
    out.println("</queue>");
    out.println("<queue name=\"a2\">");
    out.println("<weight>7</weight>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"dedicated\">");
    out.println("<reservation></reservation>");
    out.println("<weight>8</weight>");
    out.println("</queue>");
    out.println(
        "<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();
  }

  public static void updateFSAllocationFile(String allocationFile)
      throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(allocationFile));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"default\">");
    out.println("<weight>5</weight>");
    out.println("</queue>");
    out.println("<queue name=\"a\">");
    out.println("<weight>5</weight>");
    out.println("<queue name=\"a1\">");
    out.println("<weight>3</weight>");
    out.println("</queue>");
    out.println("<queue name=\"a2\">");
    out.println("<weight>7</weight>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("<queue name=\"dedicated\">");
    out.println("<reservation></reservation>");
    out.println("<weight>10</weight>");
    out.println("</queue>");
    out.println("<queue name=\"reservation\">");
    out.println("<reservation></reservation>");
    out.println("<weight>80</weight>");
    out.println("</queue>");
    out.println(
        "<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();
  }

  public static FairScheduler setupFairScheduler(RMContext rmContext,
      Configuration conf, int numContainers) throws IOException {
    FairScheduler scheduler = new FairScheduler();
    scheduler.setRMContext(rmContext);

    when(rmContext.getScheduler()).thenReturn(scheduler);

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, rmContext);


    Resource resource =
        ReservationSystemTestUtil.calculateClusterResource(numContainers);
    RMNode node1 = MockNodes.newNodeInfo(1, resource, 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    return scheduler;
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration) {
    return createSimpleReservationDefinition(arrival, deadline, duration, 1);
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration, int parallelism) {
    // create a request with a single atomic ask
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1),
            parallelism, parallelism, duration);
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    return rDef;
  }

  public static ReservationSubmissionRequest createSimpleReservationRequest(
      ReservationId reservationId, int numContainers, long arrival,
      long deadline, long duration) {
    // create a request with a single atomic ask
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1),
            numContainers, 1, duration);
    ReservationRequests reqs =
        ReservationRequests.newInstance(Collections.singletonList(r),
            ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef =
        ReservationDefinition.newInstance(arrival, deadline, reqs,
            "testClientRMService#reservation");
    ReservationSubmissionRequest request =
        ReservationSubmissionRequest.newInstance(rDef,
            reservationQ, reservationId);
    return request;
  }

  public static RMContext createMockRMContext() {
    RMContext context = mock(RMContext.class);
    when(context.getStateStore()).thenReturn(new MemoryRMStateStore());
    return context;
  }

  public CapacityScheduler mockCapacityScheduler(int numContainers)
      throws IOException {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);

    CapacityScheduler cs = Mockito.spy(new CapacityScheduler());
    cs.setConf(new YarnConfiguration());

    RMContext mockRmContext = createRMContext(conf);

    cs.setRMContext(mockRmContext);
    try {
      cs.serviceInit(conf);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    initializeRMContext(numContainers, cs, mockRmContext);
    return cs;
  }

  @SuppressWarnings("rawtypes") public static void initializeRMContext(
      int numContainers, AbstractYarnScheduler scheduler,
      RMContext mockRMContext) {

    when(mockRMContext.getScheduler()).thenReturn(scheduler);
    Resource r = calculateClusterResource(numContainers);
    doReturn(r).when(scheduler).getClusterResource();
  }

  public static RMContext createRMContext(Configuration conf) {
    RMContext mockRmContext = Mockito.spy(
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null));

    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(nlm.getQueueResource(any(String.class), anySetOf(String.class),
            any(Resource.class))).thenAnswer(new Answer<Resource>() {
      @Override public Resource answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        return (Resource) args[2];
      }
    });

    when(nlm.getResourceByLabel(any(String.class), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override public Resource answer(InvocationOnMock invocation)
              throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
        });

    mockRmContext.setNodeLabelManager(nlm);
    return mockRmContext;
  }

  public static void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    // Define default queue
    final String defQ = CapacitySchedulerConfiguration.ROOT + ".default";
    conf.setCapacity(defQ, 10);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "default", "a", reservationQ });

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);

    final String dedicated =
        CapacitySchedulerConfiguration.ROOT + CapacitySchedulerConfiguration.DOT
            + reservationQ;
    conf.setCapacity(dedicated, 80);
    // Set as reservation queue
    conf.setReservable(dedicated, true);

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, 30);
    conf.setCapacity(A2, 70);
  }

  public static void setupDynamicQueueConfiguration(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { reservationQ });
    final String dedicated = CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT + reservationQ;
    conf.setCapacity(dedicated, 100);
    // Set as reservation queue
    conf.setReservable(dedicated, true);
  }

  public static String getFullReservationQueueName() {
    return CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT + reservationQ;
  }

  public static String getReservationQueueName() {
    return reservationQ;
  }

  public static void updateQueueConfiguration(
      CapacitySchedulerConfiguration conf, String newQ) {
    // Define default queue
    final String prefix = CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT;
    final String defQ = prefix + "default";
    conf.setCapacity(defQ, 5);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { "default", "a", reservationQ, newQ });

    final String A = prefix + "a";
    conf.setCapacity(A, 5);

    final String dedicated = prefix + reservationQ;
    conf.setCapacity(dedicated, 10);
    // Set as reservation queue
    conf.setReservable(dedicated, true);

    conf.setCapacity(prefix + newQ, 80);
    // Set as reservation queue
    conf.setReservable(prefix + newQ, true);

    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] { "a1", "a2" });
    conf.setCapacity(A1, 30);
    conf.setCapacity(A2, 70);
  }

  public static ReservationDefinition generateRandomRR(Random rand, long i) {
    rand.setSeed(i);
    long now = System.currentTimeMillis();

    // start time at random in the next 12 hours
    long arrival = rand.nextInt(12 * 3600 * 1000);
    // deadline at random in the next day
    long deadline = arrival + rand.nextInt(24 * 3600 * 1000);

    // create a request with a single atomic ask
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(now + arrival);
    rr.setDeadline(now + deadline);

    int gang = 1 + rand.nextInt(9);
    int par = (rand.nextInt(1000) + 1) * gang;
    long dur = rand.nextInt(2 * 3600 * 1000); // random duration within 2h
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), par, gang, dur);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rand.nextInt(3);
    ReservationRequestInterpreter[] type =
        ReservationRequestInterpreter.values();
    reqs.setInterpreter(type[rand.nextInt(type.length)]);
    rr.setReservationRequests(reqs);

    return rr;

  }

  public static Map<ReservationInterval, Resource> generateAllocation(
      long startTime, long step, int[] alloc) {
    Map<ReservationInterval, Resource> req = new TreeMap<>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i * step,
          startTime + (i + 1) * step), ReservationSystemUtil.toResource(
          ReservationRequest
              .newInstance(Resource.newInstance(1024, 1), alloc[i])));
    }
    return req;
  }

  public static Resource calculateClusterResource(int numContainers) {
    return Resource.newInstance(numContainers * 1024, numContainers);
  }
}
