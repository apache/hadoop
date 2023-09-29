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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.DBManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLeveldbRMStateStore extends RMStateStoreTestBase {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestLeveldbRMStateStore.class.getName());

  private YarnConfiguration conf;
  private LeveldbRMStateStore stateStore = null;

  @Before
  public void setup() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_LEVELDB_STORE_PATH, TEST_DIR.toString());
  }

  @After
  public void cleanup() throws IOException {
    if (stateStore != null) {
      stateStore.close();
    }
    FileUtil.fullyDelete(TEST_DIR);
  }

  @Test(timeout = 60000)
  public void testApps() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testRMAppStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testClientTokens() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testRMDTSecretManagerStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testVersion() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testCheckVersion(tester);
  }

  @Test(timeout = 60000)
  public void testEpoch() throws Exception {
    conf.setLong(YarnConfiguration.RM_EPOCH, epoch);
    conf.setLong(YarnConfiguration.RM_EPOCH_RANGE, getEpochRange());
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testEpoch(tester);
  }

  @Test(timeout = 60000)
  public void testAppDeletion() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testAppDeletion(tester);
  }

  @Test(timeout = 60000)
  public void testDeleteStore() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testDeleteStore(tester);
  }

  @Test(timeout = 60000)
  public void testRemoveApplication() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testRemoveApplication(tester);
  }

  @Test(timeout = 60000)
  public void testRemoveAttempt() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testRemoveAttempt(tester);
  }

  @Test(timeout = 60000)
  public void testAMTokens() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testAMRMTokenSecretManagerStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testReservation() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testReservationStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testProxyCA() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testProxyCA(tester);
  }

  @Test(timeout = 60000)
  public void testCompactionCycle() {
    final DB mockdb = mock(DB.class);
    conf.setLong(YarnConfiguration.RM_LEVELDB_COMPACTION_INTERVAL_SECS, 1);
    stateStore = new LeveldbRMStateStore();
    DBManager dbManager = new DBManager() {
      @Override
      public DB initDatabase(File configurationFile, Options options,
                             Consumer<DB> initMethod) {
        return mockdb;
      }
    };
    dbManager.setDb(mockdb);
    stateStore.setDbManager(dbManager);
    stateStore.init(conf);
    stateStore.start();
    verify(mockdb, timeout(10000)).compactRange(
        isNull(), isNull());
  }

  @Test
  public void testBadKeyIteration() throws Exception {
    stateStore = new LeveldbRMStateStore();
    stateStore.init(conf);
    stateStore.start();
    DB db = stateStore.getDatabase();
    // add an entry that appears at the end of the database when iterating
    db.put(JniDBFactory.bytes("zzz"), JniDBFactory.bytes("z"));
    stateStore.loadState();
  }

  class LeveldbStateStoreTester implements RMStateStoreHelper {

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      if (stateStore != null) {
        stateStore.close();
      }
      stateStore = new LeveldbRMStateStore();
      stateStore.init(conf);
      stateStore.start();
      stateStore.dispatcher.disableExitOnDispatchException();
      return stateStore;
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      // There should be 6 total entries:
      //   1 entry for version
      //   2 entries for app 0010 with one attempt
      //   3 entries for app 0001 with two attempts
      return stateStore.getNumEntriesInDatabase() == 6;
    }

    @Override
    public void writeVersion(Version version) {
      stateStore.storeVersion(version);
    }

    @Override
    public Version getCurrentVersion() {
      return stateStore.getCurrentVersion();
    }

    @Override
    public boolean appExists(RMApp app) throws Exception {
      if (stateStore.isClosed()) {
        getRMStateStore();
      }
      return stateStore.loadRMAppState(app.getApplicationId()) != null;
    }

    @Override
    public boolean attemptExists(RMAppAttempt attempt) throws Exception {
      if (stateStore.isClosed()) {
        getRMStateStore();
      }
      return stateStore.loadRMAppAttemptState(attempt.getAppAttemptId())
          != null;
    }
  }
}
