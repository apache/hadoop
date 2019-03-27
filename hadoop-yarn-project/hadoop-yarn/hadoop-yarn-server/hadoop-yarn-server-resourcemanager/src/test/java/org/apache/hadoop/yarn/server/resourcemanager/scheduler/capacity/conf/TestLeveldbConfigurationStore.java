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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests {@link LeveldbConfigurationStore}.
 */
public class TestLeveldbConfigurationStore extends ConfigurationStoreBaseTest {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestLeveldbConfigurationStore.class);
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestLeveldbConfigurationStore.class.getName());

  private ResourceManager rm;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    FileUtil.fullyDelete(TEST_DIR);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.LEVELDB_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.RM_SCHEDCONF_STORE_PATH, TEST_DIR.toString());
  }

  @Test
  public void testVersioning() throws Exception {
    confStore.initialize(conf, schedConf, rmContext);
    assertNull(confStore.getConfStoreVersion());
    confStore.checkVersion();
    assertEquals(LeveldbConfigurationStore.CURRENT_VERSION_INFO,
        confStore.getConfStoreVersion());
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

    Map<String, String> update = new HashMap<>();
    update.put("key", "val");
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(update, TEST_USER);
    confStore.logMutation(mutation);
    confStore.confirmMutation(true);
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
  public void testMaxLogs() throws Exception {
    conf.setLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS, 2);
    confStore.initialize(conf, schedConf, rmContext);
    LinkedList<YarnConfigurationStore.LogMutation> logs =
        ((LeveldbConfigurationStore) confStore).getLogs();
    assertEquals(0, logs.size());

    Map<String, String> update1 = new HashMap<>();
    update1.put("key1", "val1");
    YarnConfigurationStore.LogMutation mutation =
        new YarnConfigurationStore.LogMutation(update1, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((LeveldbConfigurationStore) confStore).getLogs();
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    confStore.confirmMutation(true);
    assertEquals(1, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));

    Map<String, String> update2 = new HashMap<>();
    update2.put("key2", "val2");
    mutation = new YarnConfigurationStore.LogMutation(update2, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((LeveldbConfigurationStore) confStore).getLogs();
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));
    confStore.confirmMutation(true);
    assertEquals(2, logs.size());
    assertEquals("val1", logs.get(0).getUpdates().get("key1"));
    assertEquals("val2", logs.get(1).getUpdates().get("key2"));

    // Next update should purge first update from logs.
    Map<String, String> update3 = new HashMap<>();
    update3.put("key3", "val3");
    mutation = new YarnConfigurationStore.LogMutation(update3, TEST_USER);
    confStore.logMutation(mutation);
    logs = ((LeveldbConfigurationStore) confStore).getLogs();
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
    confStore.confirmMutation(true);
    assertEquals(2, logs.size());
    assertEquals("val2", logs.get(0).getUpdates().get("key2"));
    assertEquals("val3", logs.get(1).getUpdates().get("key3"));
    confStore.close();
  }

  /**
   * When restarting, RM should read from current state of store, including
   * any updates from the previous RM instance.
   * @throws Exception
   */
  @Test
  public void testRestartReadsFromUpdatedStore() throws Exception {
    ResourceManager rm1 = new MockRM(conf);
    rm1.start();
    assertNull(((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("key"));

    // Update configuration on RM
    SchedConfUpdateInfo schedConfUpdateInfo = new SchedConfUpdateInfo();
    schedConfUpdateInfo.getGlobalParams().put("key", "val");
    MutableConfigurationProvider confProvider = ((MutableConfScheduler)
        rm1.getResourceScheduler()).getMutableConfProvider();
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(TEST_USER, new String[0]);
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);
    rm1.getResourceScheduler().reinitialize(conf, rm1.getRMContext());
    assertEquals("val", ((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("key"));
    confProvider.confirmPendingMutation(true);
    assertEquals("val", ((MutableCSConfigurationProvider) confProvider)
        .getConfStore().retrieve().get("key"));
    // Next update is not persisted, it should not be recovered
    schedConfUpdateInfo.getGlobalParams().put("key", "badVal");
    confProvider.logAndApplyMutation(user, schedConfUpdateInfo);
    rm1.close();

    // Start RM2 and verifies it starts with updated configuration
    ResourceManager rm2 = new MockRM(conf);
    rm2.start();
    assertEquals("val", ((MutableCSConfigurationProvider) (
        (CapacityScheduler) rm2.getResourceScheduler())
        .getMutableConfProvider()).getConfStore().retrieve().get("key"));
    assertEquals("val", ((MutableConfScheduler) rm2.getResourceScheduler())
        .getConfiguration().get("key"));
    rm2.close();
  }

  @Override
  public YarnConfigurationStore createConfStore() {
    return new LeveldbConfigurationStore();
  }
}
