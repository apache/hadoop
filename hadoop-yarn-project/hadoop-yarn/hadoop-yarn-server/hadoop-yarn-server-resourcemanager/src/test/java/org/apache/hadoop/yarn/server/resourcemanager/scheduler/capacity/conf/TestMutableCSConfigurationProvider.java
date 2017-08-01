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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MutableCSConfigurationProvider}.
 */
public class TestMutableCSConfigurationProvider {

  private MutableCSConfigurationProvider confProvider;
  private RMContext rmContext;
  private SchedConfUpdateInfo goodUpdate;
  private SchedConfUpdateInfo badUpdate;
  private CapacityScheduler cs;
  private AdminService adminService;

  private static final UserGroupInformation TEST_USER = UserGroupInformation
      .createUserForTesting("testUser", new String[] {});

  @Before
  public void setUp() {
    cs = mock(CapacityScheduler.class);
    rmContext = mock(RMContext.class);
    when(rmContext.getScheduler()).thenReturn(cs);
    when(cs.getConfiguration()).thenReturn(
        new CapacitySchedulerConfiguration());
    adminService = mock(AdminService.class);
    when(rmContext.getRMAdminService()).thenReturn(adminService);
    confProvider = new MutableCSConfigurationProvider(rmContext);
    goodUpdate = new SchedConfUpdateInfo();
    Map<String, String> goodUpdateMap = new HashMap<>();
    goodUpdateMap.put("goodKey", "goodVal");
    QueueConfigInfo goodUpdateInfo = new
        QueueConfigInfo("root.a", goodUpdateMap);
    goodUpdate.getUpdateQueueInfo().add(goodUpdateInfo);

    badUpdate = new SchedConfUpdateInfo();
    Map<String, String> badUpdateMap = new HashMap<>();
    badUpdateMap.put("badKey", "badVal");
    QueueConfigInfo badUpdateInfo = new
        QueueConfigInfo("root.a", badUpdateMap);
    badUpdate.getUpdateQueueInfo().add(badUpdateInfo);
  }

  @Test
  public void testInMemoryBackedProvider() throws IOException, YarnException {
    Configuration conf = new Configuration();
    confProvider.init(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    doNothing().when(adminService).refreshQueues();
    confProvider.mutateConfiguration(TEST_USER, goodUpdate);
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("yarn.scheduler.capacity.root.a.goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
    doThrow(new IOException()).when(adminService).refreshQueues();
    try {
      confProvider.mutateConfiguration(TEST_USER, badUpdate);
    } catch (IOException e) {
      // Expected exception.
    }
    assertNull(confProvider.loadConfiguration(conf).get(
        "yarn.scheduler.capacity.root.a.badKey"));
  }
}
