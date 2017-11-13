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
package org.apache.hadoop.hdfs.server.federation.store;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.newStateStore;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.getStateStoreConfiguration;
import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.waitStateStore;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test the basic {@link StateStoreService} {@link MountTableStore}
 * functionality.
 */
public class TestStateStoreBase {

  private static StateStoreService stateStore;
  private static Configuration conf;

  protected static StateStoreService getStateStore() {
    return stateStore;
  }

  protected static Configuration getConf() {
    return conf;
  }

  @BeforeClass
  public static void createBase() throws IOException, InterruptedException {

    conf = getStateStoreConfiguration();

    // Disable auto-reconnect to data store
    conf.setLong(DFSConfigKeys.FEDERATION_STORE_CONNECTION_TEST_MS,
        TimeUnit.HOURS.toMillis(1));
  }

  @AfterClass
  public static void destroyBase() throws Exception {
    if (stateStore != null) {
      stateStore.stop();
      stateStore.close();
      stateStore = null;
    }
  }

  @Before
  public void setupBase() throws IOException, InterruptedException,
      InstantiationException, IllegalAccessException {
    if (stateStore == null) {
      stateStore = newStateStore(conf);
      assertNotNull(stateStore);
    }
    // Wait for state store to connect
    stateStore.loadDriver();
    waitStateStore(stateStore, TimeUnit.SECONDS.toMillis(10));
  }
}