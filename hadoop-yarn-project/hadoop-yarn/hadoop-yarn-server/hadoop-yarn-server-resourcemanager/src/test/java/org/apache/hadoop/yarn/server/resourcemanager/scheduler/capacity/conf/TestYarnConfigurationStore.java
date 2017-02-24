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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestYarnConfigurationStore {

  private YarnConfigurationStore confStore;
  private Configuration schedConf;

  private static final String testUser = "testUser";

  @Before
  public void setUp() {
    schedConf = new Configuration(false);
    schedConf.set("key1", "val1");
  }

  @Test
  public void testInMemoryConfigurationStore() {
    confStore = new InMemoryConfigurationStore();
    confStore.initialize(new Configuration(), schedConf);
    assertEquals("val1", confStore.retrieve().get("key1"));

    Map<String, String> update1 = new HashMap<>();
    update1.put("keyUpdate1", "valUpdate1");
    LogMutation mutation1 = new LogMutation(update1, testUser);
    long id = confStore.logMutation(mutation1);
    assertEquals(1, confStore.getPendingMutations().size());
    confStore.confirmMutation(id, true);
    assertEquals("valUpdate1", confStore.retrieve().get("keyUpdate1"));
    assertEquals(0, confStore.getPendingMutations().size());

    Map<String, String> update2 = new HashMap<>();
    update2.put("keyUpdate2", "valUpdate2");
    LogMutation mutation2 = new LogMutation(update2, testUser);
    id = confStore.logMutation(mutation2);
    assertEquals(1, confStore.getPendingMutations().size());
    confStore.confirmMutation(id, false);
    assertNull("Configuration should not be updated",
        confStore.retrieve().get("keyUpdate2"));
    assertEquals(0, confStore.getPendingMutations().size());
  }
}
