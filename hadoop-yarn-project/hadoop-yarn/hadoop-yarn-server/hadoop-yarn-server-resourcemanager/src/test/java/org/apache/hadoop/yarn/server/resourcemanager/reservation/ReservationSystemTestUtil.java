/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
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

  @SuppressWarnings("unchecked")
  public CapacityScheduler mockCapacityScheduler(int numContainers)
      throws IOException {
    // stolen from TestCapacityScheduler
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(conf);

    CapacityScheduler cs = Mockito.spy(new CapacityScheduler());
    cs.setConf(new YarnConfiguration());
    RMContext mockRmContext =
        Mockito.spy(new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null));
    
    RMNodeLabelsManager nlm = mock(RMNodeLabelsManager.class);
    when(
        nlm.getQueueResource(any(String.class), any(Set.class),
            any(Resource.class))).thenAnswer(new Answer<Resource>() {
      @Override
      public Resource answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return (Resource) args[2];
      }
    });
    
    when(nlm.getResourceByLabel(any(String.class), any(Resource.class)))
        .thenAnswer(new Answer<Resource>() {
          @Override
          public Resource answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return (Resource) args[1];
          }
        });
    
    mockRmContext.setNodeLabelManager(nlm);
    
    cs.setRMContext(mockRmContext);
    try {
      cs.serviceInit(conf);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    when(mockRmContext.getScheduler()).thenReturn(cs);
    Resource r = Resource.newInstance(numContainers * 1024, numContainers);
    doReturn(r).when(cs).getClusterResource();
    return cs;
  }

  public static void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    // Define default queue
    final String defQ = CapacitySchedulerConfiguration.ROOT + ".default";
    conf.setCapacity(defQ, 10);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
        "default", "a", reservationQ });

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);

    final String dedicated =
        CapacitySchedulerConfiguration.ROOT
            + CapacitySchedulerConfiguration.DOT + reservationQ;
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

  public String getFullReservationQueueName() {
    return CapacitySchedulerConfiguration.ROOT
        + CapacitySchedulerConfiguration.DOT + reservationQ;
  }

  public String getreservationQueueName() {
    return reservationQ;
  }

  public void updateQueueConfiguration(CapacitySchedulerConfiguration conf,
      String newQ) {
    // Define default queue
    final String prefix =
        CapacitySchedulerConfiguration.ROOT
            + CapacitySchedulerConfiguration.DOT;
    final String defQ = prefix + "default";
    conf.setCapacity(defQ, 5);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {
        "default", "a", reservationQ, newQ });

    final String A = prefix + "a";
    conf.setCapacity(A, 5);

    final String dedicated = prefix + reservationQ;
    conf.setCapacity(dedicated, 80);
    // Set as reservation queue
    conf.setReservable(dedicated, true);

    conf.setCapacity(prefix + newQ, 10);
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
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), par,
            gang, dur);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rand.nextInt(3);
    ReservationRequestInterpreter[] type =
        ReservationRequestInterpreter.values();
    reqs.setInterpreter(type[rand.nextInt(type.length)]);
    rr.setReservationRequests(reqs);

    return rr;

  }

  public static ReservationDefinition generateBigRR(Random rand, long i) {
    rand.setSeed(i);
    long now = System.currentTimeMillis();

    // start time at random in the next 2 hours
    long arrival = rand.nextInt(2 * 3600 * 1000);
    // deadline at random in the next day
    long deadline = rand.nextInt(24 * 3600 * 1000);

    // create a request with a single atomic ask
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(now + arrival);
    rr.setDeadline(now + deadline);

    int gang = 1;
    int par = 100000; // 100k tasks
    long dur = rand.nextInt(60 * 1000); // 1min tasks
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), par,
            gang, dur);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rand.nextInt(3);
    ReservationRequestInterpreter[] type =
        ReservationRequestInterpreter.values();
    reqs.setInterpreter(type[rand.nextInt(type.length)]);
    rr.setReservationRequests(reqs);

    return rr;
  }

  public static Map<ReservationInterval, ReservationRequest> generateAllocation(
      long startTime, long step, int[] alloc) {
    Map<ReservationInterval, ReservationRequest> req =
        new TreeMap<ReservationInterval, ReservationRequest>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i * step, startTime + (i + 1)
          * step), ReservationRequest.newInstance(
          Resource.newInstance(1024, 1), alloc[i]));
    }
    return req;
  }

}
