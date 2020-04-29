/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * test class for log web service.
 */
public class TestLogWebService {

  private HttpServletRequest request;
  private LogWebServiceTest logWebService;
  private ApplicationId appId;
  private ContainerId cId;
  private String user = "user1";
  private Map<String, TimelineEntity> entities;
  private String nodeHttpAddress = "localhost:0";

  @Before
  public void setup() throws Exception {
    appId = ApplicationId.fromString("application_1518143905142_509690");
    cId =
        ContainerId.fromString("container_e138_1518143905142_509690_01_000001");
    entities = new HashMap<>();
    generateEntity();
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.when(request.getRemoteUser())
        .thenReturn(System.getProperty("user.name"));
    logWebService = new LogWebServiceTest();

  }

  @Test
  public void testGetApp() {
    BasicAppInfo app =
        logWebService.getApp(request, appId.toString(), null);
    Assert.assertEquals("RUNNING", app.getAppState().toString());
    Assert.assertEquals(user, app.getUser());
  }

  @Test
  public void testGetContainer() {
    String address = logWebService
        .getNodeHttpAddress(request, appId.toString(), null, cId.toString(),
            null);
    Assert.assertEquals(this.nodeHttpAddress, address);
  }

  class LogWebServiceTest extends LogWebService {

    @Override
    protected TimelineEntity getEntity(String path,
        MultivaluedMap<String, String> params) throws IOException {
      if (path.endsWith(cId.toString())) {
        return entities.get(cId.toString());
      } else if (path.endsWith(appId.toString())) {
        return entities.get(appId.toString());
      } else {
        throw new IOException();
      }
    }
  }

  private void generateEntity() {
    createAppEntities();
    createContainerEntities();
  }

  private void createContainerEntities() {
    TimelineEntity timelineEntity =
        generateEntity(TimelineEntityType.YARN_APPLICATION.toString(),
            appId.toString());
    timelineEntity.addInfo(ApplicationMetricsConstants.USER_ENTITY_INFO, user);
    timelineEntity
        .addInfo(ApplicationMetricsConstants.STATE_EVENT_INFO, "RUNNING");
    entities.put(appId.toString(), timelineEntity);
  }

  private void createAppEntities() {
    TimelineEntity timelineEntity =
        generateEntity(TimelineEntityType.YARN_CONTAINER.toString(),
            cId.toString());
    timelineEntity
        .addInfo(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO,
            nodeHttpAddress);
    entities.put(cId.toString(), timelineEntity);
  }

  private TimelineEntity generateEntity(String entityType,
      String entityId) {
    TimelineEntity timelineEntity = new TimelineEntity();
    timelineEntity.setId(entityId);
    timelineEntity.setType(entityType);
    timelineEntity.setCreatedTime(System.currentTimeMillis());
    return timelineEntity;
  }
}
