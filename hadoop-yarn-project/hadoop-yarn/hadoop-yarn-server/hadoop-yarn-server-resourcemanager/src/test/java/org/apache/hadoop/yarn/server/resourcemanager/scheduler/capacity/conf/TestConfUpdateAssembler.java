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
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Tests {@link ConfUpdateAssembler}.
 */
public class TestConfUpdateAssembler {

  private final String A_PATH = "root.a";
  private final String B_PATH = "root.b";
  private final String C_PATH = "root.c";

  private final String CONFIG_NAME = "testConfigName";
  private final String A_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + A_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private final String B_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + B_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private final String C_CONFIG_PATH = CapacitySchedulerConfiguration.PREFIX + C_PATH +
          CapacitySchedulerConfiguration.DOT + CONFIG_NAME;
  private final String ROOT_QUEUES_PATH = CapacitySchedulerConfiguration.PREFIX +
          CapacitySchedulerConfiguration.ROOT + CapacitySchedulerConfiguration.DOT +
          CapacitySchedulerConfiguration.QUEUES;

  private final String A_INIT_VALUE = "aInitValue";
  private final String A_VALUE = "aValue";
  private final String B_INIT_VALUE = "bInitValue";
  private final String B_VALUE = "bValue";
  private final String C_VALUE = "cValue";

  private CapacitySchedulerConfiguration csConfig;

  @Before
  public void setUp() {
    csConfig = crateInitialCSConfig();
  }

  @Test
  public void testAddQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, C_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo(C_PATH, updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    Map<String, String> confUpdate =
            ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertEquals(C_VALUE, confUpdate.get(C_CONFIG_PATH));
    assertEquals("a,b,c", confUpdate.get(ROOT_QUEUES_PATH));
  }

  @Test
  public void testAddExistingQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo(A_PATH, updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    assertThrows(IOException.class, () -> {
      ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testAddInvalidQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_VALUE);
    QueueConfigInfo queueConfigInfo = new QueueConfigInfo("invalidPath", updateMap);
    updateInfo.getAddQueueInfo().add(queueConfigInfo);

    assertThrows(IOException.class, () -> {
      ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testUpdateQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    Map<String, String> updateMap = new HashMap<>();
    updateMap.put(CONFIG_NAME, A_VALUE);
    QueueConfigInfo queueAConfigInfo = new QueueConfigInfo(A_PATH, updateMap);
    updateInfo.getUpdateQueueInfo().add(queueAConfigInfo);

    Map<String, String> updateMapQueueB = new HashMap<>();
    updateMapQueueB.put(CONFIG_NAME, B_VALUE);
    QueueConfigInfo queueBConfigInfo = new QueueConfigInfo(B_PATH, updateMapQueueB);

    updateInfo.getUpdateQueueInfo().add(queueBConfigInfo);

    Map<String, String> confUpdate =
            ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertEquals(A_VALUE, confUpdate.get(A_CONFIG_PATH));
    assertEquals(B_VALUE, confUpdate.get(B_CONFIG_PATH));
  }

  @Test
  public void testRemoveQueue() throws Exception {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add(A_PATH);

    Map<String, String> confUpdate =
            ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);

    assertEquals(null, confUpdate.get(A_CONFIG_PATH));
    assertEquals("b", confUpdate.get(ROOT_QUEUES_PATH));
  }

  @Test
  public void testRemoveInvalidQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("invalidPath");

    assertThrows(IOException.class, () -> {
      ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  @Test
  public void testRemoveNonExistingQueue() {
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    updateInfo.getRemoveQueueInfo().add("root.d");

    assertThrows(IOException.class, () -> {
      ConfUpdateAssembler.constructKeyValueConfUpdate(csConfig, updateInfo);
    });
  }

  private CapacitySchedulerConfiguration crateInitialCSConfig() {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a, b" });

    csConf.set(A_CONFIG_PATH, A_INIT_VALUE);
    csConf.set(B_CONFIG_PATH, B_INIT_VALUE);

    return csConf;
  }
}
