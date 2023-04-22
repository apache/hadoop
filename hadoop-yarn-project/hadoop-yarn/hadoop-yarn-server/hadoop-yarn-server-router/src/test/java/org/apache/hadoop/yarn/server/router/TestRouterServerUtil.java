/*
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
 */package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDefinitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationRequestsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hadoop.yarn.server.router.webapp.TestFederationInterceptorREST.getReservationSubmissionRequestInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestRouterServerUtil {

  public static final Logger LOG = LoggerFactory.getLogger(TestRouterServerUtil.class);

  @Test
  public void testConvertReservationDefinition() {
    // Prepare parameters
    ReservationId reservationId = ReservationId.newInstance(Time.now(), 1);
    ReservationSubmissionRequestInfo requestInfo =
        getReservationSubmissionRequestInfo(reservationId);
    ReservationDefinitionInfo expectDefinitionInfo = requestInfo.getReservationDefinition();

    // ReservationDefinitionInfo conversion ReservationDefinition
    ReservationDefinition convertDefinition =
        RouterServerUtil.convertReservationDefinition(expectDefinitionInfo);

    // reservationDefinition is not null
    assertNotNull(convertDefinition);
    assertEquals(expectDefinitionInfo.getArrival(), convertDefinition.getArrival());
    assertEquals(expectDefinitionInfo.getDeadline(), convertDefinition.getDeadline());

    Priority priority = convertDefinition.getPriority();
    assertNotNull(priority);
    assertEquals(expectDefinitionInfo.getPriority(), priority.getPriority());
    assertEquals(expectDefinitionInfo.getRecurrenceExpression(),
        convertDefinition.getRecurrenceExpression());
    assertEquals(expectDefinitionInfo.getReservationName(), convertDefinition.getReservationName());

    ReservationRequestsInfo expectRequestsInfo = expectDefinitionInfo.getReservationRequests();
    List<ReservationRequestInfo> expectRequestsInfoList =
        expectRequestsInfo.getReservationRequest();

    ReservationRequests convertReservationRequests =
        convertDefinition.getReservationRequests();
    assertNotNull(convertReservationRequests);

    List<ReservationRequest> convertRequestList =
        convertReservationRequests.getReservationResources();
    assertNotNull(convertRequestList);
    assertEquals(1, convertRequestList.size());

    ReservationRequestInfo expectResRequestInfo = expectRequestsInfoList.get(0);
    ReservationRequest convertResRequest = convertRequestList.get(0);
    assertNotNull(convertResRequest);
    assertEquals(expectResRequestInfo.getNumContainers(), convertResRequest.getNumContainers());
    assertEquals(expectResRequestInfo.getDuration(), convertResRequest.getDuration());

    ResourceInfo expectResourceInfo = expectResRequestInfo.getCapability();
    Resource convertResource = convertResRequest.getCapability();
    assertNotNull(expectResourceInfo);
    assertEquals(expectResourceInfo.getMemorySize(), convertResource.getMemorySize());
    assertEquals(expectResourceInfo.getvCores(), convertResource.getVirtualCores());
  }

  @Test
  public void testConvertReservationDefinitionEmpty() throws Exception {

    // param ReservationDefinitionInfo is Null
    ReservationDefinitionInfo definitionInfo = null;

    // null request1
    LambdaTestUtils.intercept(RuntimeException.class,
        "definitionInfo Or ReservationRequests is Null.",
        () -> RouterServerUtil.convertReservationDefinition(definitionInfo));

    // param ReservationRequests is Null
    ReservationDefinitionInfo definitionInfo2 = new ReservationDefinitionInfo();

    // null request2
    LambdaTestUtils.intercept(RuntimeException.class,
        "definitionInfo Or ReservationRequests is Null.",
        () -> RouterServerUtil.convertReservationDefinition(definitionInfo2));

    // param ReservationRequests is Null
    ReservationDefinitionInfo definitionInfo3 = new ReservationDefinitionInfo();
    ReservationRequestsInfo requestsInfo = new ReservationRequestsInfo();
    definitionInfo3.setReservationRequests(requestsInfo);

    // null request3
    LambdaTestUtils.intercept(RuntimeException.class,
        "definitionInfo Or ReservationRequests is Null.",
        () -> RouterServerUtil.convertReservationDefinition(definitionInfo3));
  }
}
