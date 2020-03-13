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

import org.apache.hadoop.yarn.server.records.Version;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.junit.Before;
import org.junit.Test;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests {@link LeveldbConfigurationStore}.
 */
public class TestLeveldbConfigurationStore extends
    PersistentConfigurationStoreBaseTest {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestLeveldbConfigurationStore.class);
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestLeveldbConfigurationStore.class.getName());

  @Before
  public void setUp() throws Exception {
    super.setUp();
    FileUtil.fullyDelete(TEST_DIR);
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.LEVELDB_CONFIGURATION_STORE);
    conf.set(YarnConfiguration.RM_SCHEDCONF_STORE_PATH, TEST_DIR.toString());
  }

  @Test(expected = YarnConfStoreVersionIncompatibleException.class)
  public void testIncompatibleVersion() throws Exception {
    try {
      confStore.initialize(conf, schedConf, rmContext);

      Version otherVersion = Version.newInstance(1, 1);
      ((LeveldbConfigurationStore) confStore).storeVersion(otherVersion);

      assertEquals("The configuration store should have stored the new" +
              "version.", otherVersion,
          confStore.getConfStoreVersion());
      confStore.checkVersion();
    } finally {
      confStore.close();
    }
  }

  @Test
  public void testDisableAuditLogs() throws Exception {
    conf.setLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS, 0);
    confStore.initialize(conf, schedConf, rmContext);

    prepareLogMutation("key1", "val1");

    boolean logKeyPresent = false;
    DB db = ((LeveldbConfigurationStore) confStore).getDB();
    DBIterator itr = db.iterator();
    itr.seekToFirst();
    while (itr.hasNext()) {
      Map.Entry<byte[], byte[]> entry = itr.next();
      String key = new String(entry.getKey(), StandardCharsets.UTF_8);
      if (key.equals("log")) {
        logKeyPresent = true;
        break;
      }
    }
    assertFalse("Audit Log is not disabled", logKeyPresent);
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
    LogMutation log = confProvider.logAndApplyMutation(user,
        schedConfUpdateInfo);
    rm1.getResourceScheduler().reinitialize(conf, rm1.getRMContext());
    assertEquals("val", ((MutableConfScheduler) rm1.getResourceScheduler())
        .getConfiguration().get("key"));
    confProvider.confirmPendingMutation(log, true);
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

  @Override
  Version getVersion() {
    return LeveldbConfigurationStore.CURRENT_VERSION_INFO;
  }

}
