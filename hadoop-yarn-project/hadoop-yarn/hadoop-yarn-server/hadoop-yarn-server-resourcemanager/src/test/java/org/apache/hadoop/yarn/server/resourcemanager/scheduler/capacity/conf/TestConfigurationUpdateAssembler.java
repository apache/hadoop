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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ConfigurationUpdateAssembler}.
 */
public class TestConfigurationUpdateAssembler {

  private static final String A_PATH = "root.a";
  private static final String B_PATH = "root.b";
  private static final String C_PATH = "root.c";

  private static final String CONFIG_NAME = "testConfigName";
  private static final String A_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + A_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private static final String B_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + B_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private static final String C_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + C_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private static final String ROOT_QUEUES_PATH = CapacitySchedulerConfiguration.PREFIX +
          CapacitySchedulerConfiguration.ROOT + CapacitySchedulerConfiguration.DOT +
          CapacitySchedulerConfiguration.QUEUES;

  private static final String A_INIT_CONFIG_VALUE = "aInitValue";
  private static final String A_CONFIG_VALUE = "aValue";
  private static final String B_INIT_CONFIG_VALUE = "bInitValue";
  private static final String B_CONFIG_VALUE = "bValue";
  private static final String C_CONFIG_VALUE = "cValue";

  private CapacitySchedulerConfiguration csConfig;

  @Before
  public void setUp() {
    csConfig = crateInitialCSConfig();
  }

  @Test
  public void testAddQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, C_CONFIG_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo(C_PATH, updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    Map<String, String> configurationUpdate =
            ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertEquals(C_CONFIG_VALUE, configurationUpdate.get(C_CONFIG_PATH));
    assertEquals("a,b,c", configurationUpdate.get(ROOT_QUEUES_PATH));
  }

  @Test
  public void testAddExistingQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_CONFIG_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo(A_PATH, updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    assertThrows(IOException.class, () -> {
      ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testAddInvalidQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_CONFIG_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo("invalidPath", updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    assertThrows(IOException.class, () -> {
      ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testUpdateQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_CONFIG_VALUE);
    QueueConfigInfo queueAConfigInfo = new QueueConfigInfo(A_PATH, updateMap);
    updateInfo.getUpdateQueueInfo().add(queueAConfigInfo);

    Map<String, String> updateMapQueueB = new HashMap<>();
    updateMapQueueB.put(CONFIG_NAME, B_CONFIG_VALUE);
    QueueConfigInfo queueBConfigInfo = new QueueConfigInfo(B_PATH, updateMapQueueB);

    updateInfo.getUpdateQueueInfo().add(queueBConfigInfo);

    Map<String, String> configurationUpdate =
            ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertEquals(A_CONFIG_VALUE, configurationUpdate.get(A_CONFIG_PATH));
    assertEquals(B_CONFIG_VALUE, configurationUpdate.get(B_CONFIG_PATH));
  }

  @Test
  public void testRemoveQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add(A_PATH);

    Map<String, String> configurationUpdate =
            ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertTrue(configurationUpdate.containsKey(A_CONFIG_PATH));
    assertNull(configurationUpdate.get(A_CONFIG_PATH));
    assertEquals("b", configurationUpdate.get(ROOT_QUEUES_PATH));
  }

  @Test
  public void testRemoveInvalidQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("invalidPath");

    assertThrows(IOException.class, () -> {
      ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testRemoveNonExistingQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.d");

    assertThrows(IOException.class, () -> {
      ConfigurationUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  private CapacitySchedulerConfiguration crateInitialCSConfig() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(new QueuePath(CapacitySchedulerConfiguration.ROOT), new String[] {"a, b"});

    csConf.set(A_CONFIG_PATH, A_INIT_CONFIG_VALUE);
    csConf.set(B_CONFIG_PATH, B_INIT_CONFIG_VALUE);

    return csConf;
  }
}
