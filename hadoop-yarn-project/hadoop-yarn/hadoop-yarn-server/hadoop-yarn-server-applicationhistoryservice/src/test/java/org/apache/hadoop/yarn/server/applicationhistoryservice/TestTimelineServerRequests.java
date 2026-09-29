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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_HTTP_AUTH_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineServerRequests {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTimelineServerRequests.class);

  private static final String HOST = "localhost";
  private static final String TIMELINE_SERVICE_WEBAPP_ADDRESS = HOST + ":0";
  private static final String ENTITY_TYPE = "TEST_ENTITY_TYPE";
  private static final String ENTITY_ID = "test_entity_1";
  private static ApplicationHistoryServer testTimelineServer;
  private static Configuration conf;

  @BeforeAll
  public static void setup() {
    try {
      testTimelineServer = new ApplicationHistoryServer();
      conf = new Configuration(false);
      conf.setStrings(TIMELINE_HTTP_AUTH_PREFIX + "type", "simple");

      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
          MemoryTimelineStore.class, TimelineStore.class);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, TIMELINE_SERVICE_WEBAPP_ADDRESS);
      conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES, 1);

      testTimelineServer.init(conf);
      testTimelineServer.start();
    } catch (Exception e) {
      LOG.error("Failed to setup TimelineServer", e);
      fail("Couldn't setup TimelineServer");
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (testTimelineServer != null) {
      testTimelineServer.stop();
    }
  }

  @Test
  void testPutAndGetTimelineEntity() throws Exception {
    putEntity();
    getEntity();
  }

  private void putEntity() throws IOException, YarnException {
    TimelineClient client = createTimelineClient();
    try {
      TimelineEntity entityToStore = new TimelineEntity();
      entityToStore.setEntityType(ENTITY_TYPE);
      entityToStore.setEntityId(ENTITY_ID);
      entityToStore.setStartTime(System.currentTimeMillis());
      TimelinePutResponse putResponse = client.putEntities(entityToStore);
      assertTrue(putResponse.getErrors().isEmpty(),
          String.format("There were some errors in the putResponse: %s",
              putResponse.getErrors()));
      TimelineEntity entityToRead =
          testTimelineServer.getTimelineStore().getEntity(ENTITY_ID, ENTITY_TYPE,
              null);
      assertNotNull(entityToRead, "Timeline entity should not be null");
    } finally {
      client.stop();
    }
  }

  private TimelineClient createTimelineClient() {
    TimelineClient client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
    return client;
  }

  private void getEntity() {
    String appUrl = String.format("http://%s:%d/ws/v1/timeline/%s/%s?user.name=foo",
        HOST, testTimelineServer.getPort(), ENTITY_TYPE, ENTITY_ID);
    LOG.info("Getting timeline entity: {}", appUrl);

    Client client = ClientBuilder.newClient();
    try {
      client.register(TimelineEntityReader.class);
      WebTarget target = client.target(appUrl);

      Response response =
          target.request(MediaType.APPLICATION_JSON).get(Response.class);
      try {
        assertEquals(200, response.getStatus());
        assertTrue(MediaType.APPLICATION_JSON_TYPE.isCompatible(
            response.getMediaType()));

        TimelineEntity entity = response.readEntity(TimelineEntity.class);
        assertNotNull(entity, "Timeline entity should not be null");
        assertEquals(ENTITY_TYPE, entity.getEntityType());
        assertEquals(ENTITY_ID, entity.getEntityId());
        assertNotNull(entity.getStartTime(),
            "Timeline entity start time should not be null");
      } finally {
        response.close();
      }
    } finally {
      client.close();
    }
  }
}
