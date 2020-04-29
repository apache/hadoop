/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Base class for {@link YarnConfigurationStore} implementations.
 */
public abstract class ConfigurationStoreBaseTest {
  static final String TEST_USER = "testUser";
  YarnConfigurationStore confStore = createConfStore();
  Configuration conf;
  Configuration schedConf;
  RMContext rmContext;

  abstract YarnConfigurationStore createConfStore();

  @Before
  public void setUp() throws Exception {
    this.conf = new Configuration();
    this.conf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, CapacityScheduler.class);
    this.schedConf = new Configuration(false);
  }

  @Test
  public void testConfigurationUpdate() throws Exception {
    schedConf.set("key1", "val1");
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val1", confStore.retrieve().get("key1"));

    confStore.confirmMutation(prepareLogMutation("keyUpdate1", "valUpdate1"),
        true);
    assertEquals("valUpdate1", confStore.retrieve().get("keyUpdate1"));

    confStore.confirmMutation(prepareLogMutation("keyUpdate2", "valUpdate2"),
        false);
    assertNull("Configuration should not be updated",
        confStore.retrieve().get("keyUpdate2"));
    confStore.close();
  }

  @Test
  public void testNullConfigurationUpdate() throws Exception {
    schedConf.set("key", "val");
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));

    confStore.confirmMutation(prepareLogMutation("key", null), true);
    assertNull(confStore.retrieve().get("key"));
    confStore.close();
  }

  YarnConfigurationStore.LogMutation prepareLogMutation(String... values)
      throws Exception {
    Map<String, String> updates = new HashMap<>();
    String key;
    String value;

    if (values.length % 2 != 0) {
      throw new IllegalArgumentException("The number of parameters should be " +
        "even.");
    }

    for (int i = 1; i <= values.length; i += 2) {
      key = values[i - 1];
      value = values[i];
      updates.put(key, value);
    }
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(updates, TEST_USER);
    confStore.logMutation(mutation);

    return mutation;
  }
}
