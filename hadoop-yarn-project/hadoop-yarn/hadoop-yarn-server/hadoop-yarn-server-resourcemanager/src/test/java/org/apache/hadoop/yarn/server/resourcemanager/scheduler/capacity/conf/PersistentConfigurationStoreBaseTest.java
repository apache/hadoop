/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;

/**
 * Base class for the persistent {@link YarnConfigurationStore}
 * implementations, namely {@link TestLeveldbConfigurationStore} and
 * {@link TestZKConfigurationStore}.
 */
public abstract class PersistentConfigurationStoreBaseTest extends
    ConfigurationStoreBaseTest {

  abstract Version getVersion();

  @Test
  public void testGetConfigurationVersion() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    long v1 = confStore.getConfigVersion();
    assertEquals(1, v1);
    confStore.confirmMutation(prepareLogMutation("keyver", "valver"), true);
    long v2 = confStore.getConfigVersion();
    assertEquals(2, v2);
    confStore.close();
  }

  @Test
  public void testPersistConfiguration() throws Exception {
    schedConf.set("key", "val");
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));
    confStore.close();

    // Create a new configuration store, and check for old configuration
    confStore = createConfStore();
    schedConf.set("key", "badVal");
    // Should ignore passed-in scheduler configuration.
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));
    confStore.close();
  }

  @Test
  public void testPersistUpdatedConfiguration() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    assertNull(confStore.retrieve().get("key"));

    confStore.confirmMutation(prepareLogMutation("key", "val"), true);
    assertEquals("val", confStore.retrieve().get("key"));
    confStore.close();

    // Create a new configuration store, and check for updated configuration
    confStore = createConfStore();
    schedConf.set("key", "badVal");
    // Should ignore passed-in scheduler configuration.
    confStore.initialize(conf, schedConf, rmContext);
    assertEquals("val", confStore.retrieve().get("key"));
    confStore.close();
  }


  @Test
  public void testVersion() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    assertNull(confStore.getConfStoreVersion());
    confStore.checkVersion();
    assertEquals(getVersion(),
        confStore.getConfStoreVersion());
    confStore.close();
  }

  @Test
  public void testMaxLogs() throws Exception {
    assumeFalse("test should be skipped for TestFSSchedulerConfigurationStore",
      this instanceof TestFSSchedulerConfigurationStore);

    conf.setLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS, 2);
    confStore.initialize(conf, schedConf, rmContext);
    LinkedList<YarnConfigurationStore.LogMutation> logs = confStore.getLogs();
    assertEquals(0, logs.size());

    YarnConfigurationStore.LogMutation mutation =
        prepareLogMutation("key1", "val1");
    logs = confStore.getLogs();
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    confStore.confirmMutation(mutation, true);
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));

    mutation = prepareLogMutation("key2", "val2");
    logs = confStore.getLogs();
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));
    confStore.confirmMutation(mutation, true);
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));

    // Next update should purge first update from logs.
    mutation = prepareLogMutation("key3", "val3");
    logs = confStore.getLogs();
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
    confStore.confirmMutation(mutation, true);
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
    confStore.close();
  }


}
