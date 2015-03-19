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

package org.apache.hadoop.yarn.server.timelineservice;


import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimelineServiceClientIntegration {
  private static TimelineCollectorManager collectorManager;
  private static PerNodeTimelineCollectorsAuxService auxService;

  @BeforeClass
  public static void setupClass() throws Exception {
    try {
      collectorManager = new MyTimelineCollectorManager();
      auxService =
          PerNodeTimelineCollectorsAuxService.launchServer(new String[0],
              collectorManager);
      auxService.addApplication(ApplicationId.newInstance(0, 1));
    } catch (ExitUtil.ExitException e) {
      fail();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (auxService != null) {
      auxService.stop();
    }
  }

  @Test
  public void testPutEntities() throws Exception {
    TimelineClient client =
        TimelineClient.createTimelineClient(ApplicationId.newInstance(0, 1));
    try {
      // set the timeline service address manually
      client.setTimelineServiceAddress(
          collectorManager.getRestServerBindAddress());
      client.init(new YarnConfiguration());
      client.start();
      TimelineEntity entity = new TimelineEntity();
      entity.setType("test entity type");
      entity.setId("test entity id");
      client.putEntities(entity);
      client.putEntitiesAsync(entity);
    } finally {
      client.stop();
    }
  }

  private static class MyTimelineCollectorManager extends
      TimelineCollectorManager {
    public MyTimelineCollectorManager() {
      super();
    }

    @Override
    protected CollectorNodemanagerProtocol getNMCollectorService() {
      return mock(CollectorNodemanagerProtocol.class);
    }
  }
}
