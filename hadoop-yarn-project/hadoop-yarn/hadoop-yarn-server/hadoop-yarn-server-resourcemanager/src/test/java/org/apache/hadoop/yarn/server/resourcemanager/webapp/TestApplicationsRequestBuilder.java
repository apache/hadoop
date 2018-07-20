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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.apache.hadoop.yarn.server.webapp.WebServices.parseQueries;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestApplicationsRequestBuilder {

  private GetApplicationsRequest getDefaultRequest() {
    GetApplicationsRequest req = GetApplicationsRequest.newInstance();
    req.setStartRange(0, Long.MAX_VALUE);
    req.setFinishRange(0, Long.MAX_VALUE);
    return req;
  }

  @Test
  public void testDefaultRequest() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullStateQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStateQuery(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyStateQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStateQuery("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidStateQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStateQuery("invalidState").build();
  }

  @Test
  public void testRequestWithValidStateQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStateQuery(YarnApplicationState.NEW_SAVING.toString()).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    Set<String> appStates =
        Sets.newHashSet(YarnApplicationState.NEW_SAVING.toString());
    Set<String> appStatesLowerCase = parseQueries(appStates, true);
    expectedRequest.setApplicationStates(appStatesLowerCase);

    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyStateQueries() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStatesQuery(Sets.newHashSet()).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();

    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidStateQueries() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStatesQuery(Sets.newHashSet("a1", "a2", "")).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();

    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullStateQueries() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStatesQuery(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();

    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidStateQueries() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStatesQuery(
            Sets.newHashSet(YarnApplicationState.NEW_SAVING.toString(),
                YarnApplicationState.NEW.toString()))
        .build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    Set<String> appStates =
        Sets.newHashSet(YarnApplicationState.NEW_SAVING.toString(),
            YarnApplicationState.NEW.toString());
    Set<String> appStatesLowerCase = parseQueries(appStates, true);
    expectedRequest.setApplicationStates(appStatesLowerCase);

    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullUserQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withUserQuery(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyUserQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withUserQuery("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithUserQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withUserQuery("user1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setUsers(Sets.newHashSet("user1"));
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullQueueQuery() {
    ResourceManager rm = mock(ResourceManager.class);
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withQueueQuery(rm, null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyQueueQuery() {
    ResourceManager rm = mock(ResourceManager.class);
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withQueueQuery(rm, "").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithQueueQueryExistingQueue() {
    ResourceManager rm = mock(ResourceManager.class);
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withQueueQuery(rm, "queue1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setQueues(Sets.newHashSet("queue1"));
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithQueueQueryNotExistingQueue() throws IOException {
    CapacityScheduler cs = mock(CapacityScheduler.class);
    when(cs.getQueueInfo(eq("queue1"), anyBoolean(), anyBoolean()))
        .thenThrow(new IOException());
    ResourceManager rm = mock(ResourceManager.class);
    when(rm.getResourceScheduler()).thenReturn(cs);

    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withQueueQuery(rm, "queue1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setQueues(Sets.newHashSet("queue1"));
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullLimitQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withLimit(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyLimitQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withLimit("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidLimitQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withLimit("bla").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidNegativeLimitQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withLimit("-10").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidLimitQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withLimit("999").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setLimit(999L);
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullStartedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyStartedTimeBeginQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStartedTimeBegin("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidStartedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin("bla").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidNegativeStartedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin("-1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidStartedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin("999").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setStartRange(999L, Long.MAX_VALUE);
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullStartedTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStartedTimeEnd(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptywithStartedTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStartedTimeEnd("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidStartedTimeEndQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeEnd("bla").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidNegativeStartedTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withStartedTimeEnd("-1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidStartedTimeEndQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeEnd("999").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setStartRange(0L, 999L);
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullFinishedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyFinishedTimeBeginQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeBegin("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidFinishedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin("bla").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidNegativeFinishedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin("-1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidFinishedTimeBeginQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin("999").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setFinishRange(999L, Long.MAX_VALUE);
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullFinishedTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeEnd(null).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithEmptyFinishTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeEnd("").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidFinishTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeEnd("bla").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidNegativeFinishedTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeEnd("-1").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidFinishTimeEndQuery() {
    GetApplicationsRequest request =
        ApplicationsRequestBuilder.create().withFinishTimeEnd("999").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setFinishRange(0L, 999L);
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidStartTimeRangeQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin("1000").withStartedTimeEnd("2000").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setStartRange(1000L, 2000L);
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidStartTimeRangeQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withStartedTimeBegin("2000").withStartedTimeEnd("1000").build();
  }

  @Test
  public void testRequestWithValidFinishTimeRangeQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin("1000").withFinishTimeEnd("2000").build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setFinishRange(1000L, 2000L);
    assertEquals(expectedRequest, request);
  }

  @Test(expected = BadRequestException.class)
  public void testRequestWithInvalidFinishTimeRangeQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withFinishTimeBegin("2000").withFinishTimeEnd("1000").build();
  }

  @Test
  public void testRequestWithNullApplicationTypesQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTypes(null).build();
  }

  @Test
  public void testRequestWithEmptyApplicationTypesQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTypes(Sets.newHashSet()).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setApplicationTypes(Sets.newHashSet());
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidApplicationTypesQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTypes(Sets.newHashSet("type1")).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setApplicationTypes(Sets.newHashSet("type1"));
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithNullApplicationTagsQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTags(null).build();
  }

  @Test
  public void testRequestWithEmptyApplicationTagsQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTags(Sets.newHashSet()).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setApplicationTags(Sets.newHashSet());
    assertEquals(expectedRequest, request);
  }

  @Test
  public void testRequestWithValidApplicationTagsQuery() {
    GetApplicationsRequest request = ApplicationsRequestBuilder.create()
        .withApplicationTags(Sets.newHashSet("tag1")).build();

    GetApplicationsRequest expectedRequest = getDefaultRequest();
    expectedRequest.setApplicationTags(Sets.newHashSet("tag1"));
    assertEquals(expectedRequest, request);
  }
}