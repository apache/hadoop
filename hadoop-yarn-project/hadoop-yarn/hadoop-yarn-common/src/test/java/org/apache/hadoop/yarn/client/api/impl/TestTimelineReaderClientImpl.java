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

import static org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType.YARN_APPLICATION_ATTEMPT;
import static org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType.YARN_CONTAINER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineReaderClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Timeline Reader Client.
 */
public class TestTimelineReaderClientImpl {

  private TimelineReaderClient client;

  @Before
  public void setup() {
    client = new MockTimelineReaderClient();
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    client.init(conf);
    client.start();
  }

  @Test
  public void testGetApplication() throws Exception {
    ApplicationId applicationId =
        ApplicationId.fromString("application_1234_0001");
    TimelineEntity entity = client.getApplicationEntity(applicationId,
        null, null);
    Assert.assertEquals("mockApp1", entity.getId());
  }

  @Test
  public void getApplicationAttemptEntity() throws Exception {
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.fromString("appattempt_1234_0001_000001");
    TimelineEntity entity = client.getApplicationAttemptEntity(attemptId,
        null, null);
    Assert.assertEquals("mockAppAttempt1", entity.getId());
  }

  @Test
  public void getApplicationAttemptEntities() throws Exception {
    ApplicationId applicationId =
        ApplicationId.fromString("application_1234_0001");
    List<TimelineEntity> entities =
        client.getApplicationAttemptEntities(applicationId, null,
            null, 0, null);
    Assert.assertEquals(2, entities.size());
    Assert.assertEquals("mockAppAttempt2", entities.get(1).getId());
  }

  @Test
  public void testGetContainer() throws Exception {
    ContainerId containerId =
        ContainerId.fromString("container_1234_0001_01_000001");
    TimelineEntity entity = client.getContainerEntity(containerId,
        null, null);
    Assert.assertEquals("mockContainer1", entity.getId());
  }

  @Test
  public void testGetContainers() throws Exception {
    ApplicationId appId =
        ApplicationId.fromString("application_1234_0001");
    List<TimelineEntity> entities = client.getContainerEntities(appId,
        null, null, 0, null);
    Assert.assertEquals(2, entities.size());
    Assert.assertEquals("mockContainer2", entities.get(1).getId());
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.stop();
    }
  }

  private static TimelineEntity createTimelineEntity(String id) {
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    return entity;
  }

  private static TimelineEntity[] createTimelineEntities(String... ids) {
    List<TimelineEntity> entities = new ArrayList<>();
    for (String id : ids) {
      TimelineEntity entity = new TimelineEntity();
      entity.setId(id);
      entities.add(entity);
    }
    return entities.toArray(new TimelineEntity[entities.size()]);
  }

  private class MockTimelineReaderClient extends TimelineReaderClientImpl {
    @Override
    protected ClientResponse doGetUri(URI base, String path,
        MultivaluedMap<String, String> params) throws IOException {
      ClientResponse mockClientResponse = mock(ClientResponse.class);
      if (path.contains(YARN_CONTAINER.toString())) {
        when(mockClientResponse.getEntity(TimelineEntity.class)).thenReturn(
            createTimelineEntity("mockContainer1"));
        when(mockClientResponse.getEntity(TimelineEntity[].class)).thenReturn(
            createTimelineEntities("mockContainer1", "mockContainer2"));
      } else if (path.contains(YARN_APPLICATION_ATTEMPT.toString())) {
        when(mockClientResponse.getEntity(TimelineEntity.class)).thenReturn(
            createTimelineEntity("mockAppAttempt1"));
        when(mockClientResponse.getEntity(TimelineEntity[].class)).thenReturn(
            createTimelineEntities("mockAppAttempt1", "mockAppAttempt2"));
      } else {
        when(mockClientResponse.getEntity(TimelineEntity.class)).thenReturn(
            createTimelineEntity("mockApp1"));
        when(mockClientResponse.getEntity(TimelineEntity[].class)).thenReturn(
            createTimelineEntities("mockApp1", "mockApp2"));
      }
      return mockClientResponse;
    }
  }
}
