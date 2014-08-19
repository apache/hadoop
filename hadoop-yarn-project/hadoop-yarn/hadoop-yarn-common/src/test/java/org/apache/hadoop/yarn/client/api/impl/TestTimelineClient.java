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

package org.apache.hadoop.yarn.client.api.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.ConnectException;

import org.junit.Assert;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;

public class TestTimelineClient {

  private TimelineClientImpl client;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    client = createTimelineClient(conf);
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.stop();
    }
  }

  @Test
  public void testPostEntities() throws Exception {
    mockClientResponse(client, ClientResponse.Status.OK, false, false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      Assert.assertEquals(0, response.getErrors().size());
    } catch (YarnException e) {
      Assert.fail("Exception is not expected");
    }
  }

  @Test
  public void testPostEntitiesWithError() throws Exception {
    mockClientResponse(client, ClientResponse.Status.OK, true, false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      Assert.assertEquals(1, response.getErrors().size());
      Assert.assertEquals("test entity id", response.getErrors().get(0)
          .getEntityId());
      Assert.assertEquals("test entity type", response.getErrors().get(0)
          .getEntityType());
      Assert.assertEquals(TimelinePutResponse.TimelinePutError.IO_EXCEPTION,
          response.getErrors().get(0).getErrorCode());
    } catch (YarnException e) {
      Assert.fail("Exception is not expected");
    }
  }

  @Test
  public void testPostEntitiesNoResponse() throws Exception {
    mockClientResponse(
        client, ClientResponse.Status.INTERNAL_SERVER_ERROR, false, false);
    try {
      client.putEntities(generateEntity());
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Failed to get the response from the timeline server."));
    }
  }

  @Test
  public void testPostEntitiesConnectionRefused() throws Exception {
    mockClientResponse(client, null, false, true);
    try {
      client.putEntities(generateEntity());
      Assert.fail("RuntimeException is expected");
    } catch (RuntimeException re) {
      Assert.assertTrue(re instanceof ClientHandlerException);
    }
  }

  @Test
  public void testPostEntitiesTimelineServiceNotEnabled() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    TimelineClientImpl client = createTimelineClient(conf);
    mockClientResponse(
        client, ClientResponse.Status.INTERNAL_SERVER_ERROR, false, false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      Assert.assertEquals(0, response.getErrors().size());
    } catch (YarnException e) {
      Assert.fail(
          "putEntities should already return before throwing the exception");
    }
  }

  @Test
  public void testPostEntitiesTimelineServiceDefaultNotEnabled()
      throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    // Unset the timeline service's enabled properties.
    // Make sure default value is pickup up
    conf.unset(YarnConfiguration.TIMELINE_SERVICE_ENABLED);
    TimelineClientImpl client = createTimelineClient(conf);
    mockClientResponse(client, ClientResponse.Status.INTERNAL_SERVER_ERROR,
        false, false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      Assert.assertEquals(0, response.getErrors().size());
    } catch (YarnException e) {
      Assert
          .fail("putEntities should already return before throwing the exception");
    }
  }

  private static ClientResponse mockClientResponse(TimelineClientImpl client,
      ClientResponse.Status status, boolean hasError, boolean hasRuntimeError) {
    ClientResponse response = mock(ClientResponse.class);
    if (hasRuntimeError) {
      doThrow(new ClientHandlerException(new ConnectException())).when(client)
          .doPostingEntities(any(TimelineEntities.class));
      return response;
    }
    doReturn(response).when(client)
        .doPostingEntities(any(TimelineEntities.class));
    when(response.getClientResponseStatus()).thenReturn(status);
    TimelinePutResponse.TimelinePutError error =
        new TimelinePutResponse.TimelinePutError();
    error.setEntityId("test entity id");
    error.setEntityType("test entity type");
    error.setErrorCode(TimelinePutResponse.TimelinePutError.IO_EXCEPTION);
    TimelinePutResponse putResponse = new TimelinePutResponse();
    if (hasError) {
      putResponse.addError(error);
    }
    when(response.getEntity(TimelinePutResponse.class)).thenReturn(putResponse);
    return response;
  }

  private static TimelineEntity generateEntity() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("entity id");
    entity.setEntityType("entity type");
    entity.setStartTime(System.currentTimeMillis());
    for (int i = 0; i < 2; ++i) {
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(System.currentTimeMillis());
      event.setEventType("test event type " + i);
      event.addEventInfo("key1", "val1");
      event.addEventInfo("key2", "val2");
      entity.addEvent(event);
    }
    entity.addRelatedEntity("test ref type 1", "test ref id 1");
    entity.addRelatedEntity("test ref type 2", "test ref id 2");
    entity.addPrimaryFilter("pkey1", "pval1");
    entity.addPrimaryFilter("pkey2", "pval2");
    entity.addOtherInfo("okey1", "oval1");
    entity.addOtherInfo("okey2", "oval2");
    return entity;
  }

  private static TimelineClientImpl createTimelineClient(
      YarnConfiguration conf) {
    TimelineClientImpl client =
        spy((TimelineClientImpl) TimelineClient.createTimelineClient());
    client.init(conf);
    client.start();
    return client;
  }

}
