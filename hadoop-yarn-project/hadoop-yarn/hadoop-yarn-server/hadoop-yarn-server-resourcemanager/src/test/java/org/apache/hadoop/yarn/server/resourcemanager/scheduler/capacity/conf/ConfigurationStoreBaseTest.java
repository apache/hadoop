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

  protected YarnConfigurationStore confStore = createConfStore();

  protected abstract YarnConfigurationStore createConfStore();

  protected Configuration conf;
  protected Configuration schedConf;
  protected RMContext rmContext;

  protected static final String TEST_USER = "testUser";

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

    Map<String, String> update1 = new HashMap<>();
    update1.put("keyUpdate1", "valUpdate1");
    YarnConfigurationStore.LogMutation mutation1 =
        new YarnConfigurationStore.LogMutation(update1, TEST_USER);
    confStore.logMutation(mutation1);
    confStore.confirmMutation(true);
    assertEquals("valUpdate1", confStore.retrieve().get("keyUpdate1"));

    Map<String, String> update2 = new HashMap<>();
    update2.put("keyUpdate2", "valUpdate2");
    YarnConfigurationStore.LogMutation mutation2 =
        new YarnConfigurationStore.LogMutation(update2, TEST_USER);
    confStore.logMutation(mutation2);
    confStore.confirmMutation(false);
    assertNull("Configuration should not be updated",
        confStore.retrieve().get("keyUpdate2"));
    confStore.close();
  }

  @Test
  public void testNullConfigurationUpdate() throws Exception {
    schedConf.set("key", "val");
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));

    Map<String, String> update = new HashMap<>();
    update.put("key", null);
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(update, TEST_USER);
    confStore.logMutation(mutation);
    confStore.confirmMutation(true);
    assertNull(confStore.retrieve().get("key"));
    confStore.close();
  }
}
