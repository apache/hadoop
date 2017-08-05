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

import static org.apache.hadoop.hdfs.DFSConfigKeys.FEDERATION_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl.FEDERATION_STORE_FILE_DIRECTORY;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileBaseImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.util.Time;

/**
 * Utilities to test the State Store.
 */
public final class FederationStateStoreTestUtils {

  private FederationStateStoreTestUtils() {
    // Utility Class
  }

  /**
   * Get the default State Store driver implementation.
   *
   * @return Class of the default State Store driver implementation.
   */
  public static Class<? extends StateStoreDriver> getDefaultDriver() {
    return DFSConfigKeys.FEDERATION_STORE_DRIVER_CLASS_DEFAULT;
  }

  /**
   * Create a default State Store configuration.
   *
   * @return State Store configuration.
   */
  public static Configuration getStateStoreConfiguration() {
    Class<? extends StateStoreDriver> clazz = getDefaultDriver();
    return getStateStoreConfiguration(clazz);
  }

  /**
   * Create a new State Store configuration for a particular driver.
   *
   * @param clazz Class of the driver to create.
   * @return State Store configuration.
   */
  public static Configuration getStateStoreConfiguration(
      Class<? extends StateStoreDriver> clazz) {
    Configuration conf = new HdfsConfiguration(false);

    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "hdfs://test");

    conf.setClass(FEDERATION_STORE_DRIVER_CLASS, clazz, StateStoreDriver.class);

    if (clazz.isAssignableFrom(StateStoreFileBaseImpl.class)) {
      setFileConfiguration(conf);
    }
    return conf;
  }

  /**
   * Create a new State Store based on a configuration.
   *
   * @param configuration Configuration for the State Store.
   * @return New State Store service.
   * @throws IOException If it cannot create the State Store.
   * @throws InterruptedException If we cannot wait for the store to start.
   */
  public static StateStoreService newStateStore(
      Configuration configuration) throws IOException, InterruptedException {

    StateStoreService stateStore = new StateStoreService();
    assertNotNull(stateStore);

    // Set unique identifier, this is normally the router address
    String identifier = UUID.randomUUID().toString();
    stateStore.setIdentifier(identifier);

    stateStore.init(configuration);
    stateStore.start();

    // Wait for state store to connect
    waitStateStore(stateStore, TimeUnit.SECONDS.toMillis(10));

    return stateStore;
  }

  /**
   * Wait for the State Store to initialize its driver.
   *
   * @param stateStore State Store.
   * @param timeoutMs Time out in milliseconds.
   * @throws IOException If the State Store cannot be reached.
   * @throws InterruptedException If the sleep is interrupted.
   */
  public static void waitStateStore(StateStoreService stateStore,
      long timeoutMs) throws IOException, InterruptedException {
    long startingTime = Time.monotonicNow();
    while (!stateStore.isDriverReady()) {
      Thread.sleep(100);
      if (Time.monotonicNow() - startingTime > timeoutMs) {
        throw new IOException("Timeout waiting for State Store to connect");
      }
    }
  }

  /**
   * Delete the default State Store.
   *
   * @throws IOException
   */
  public static void deleteStateStore() throws IOException {
    Class<? extends StateStoreDriver> driverClass = getDefaultDriver();
    deleteStateStore(driverClass);
  }

  /**
   * Delete the State Store.
   * @param driverClass Class of the State Store driver implementation.
   * @throws IOException If it cannot be deleted.
   */
  public static void deleteStateStore(
      Class<? extends StateStoreDriver> driverClass) throws IOException {

    if (StateStoreFileBaseImpl.class.isAssignableFrom(driverClass)) {
      String workingDirectory = System.getProperty("user.dir");
      File dir = new File(workingDirectory + "/statestore");
      if (dir.exists()) {
        FileUtils.cleanDirectory(dir);
      }
    }
  }

  /**
   * Set the default configuration for drivers based on files.
   *
   * @param conf Configuration to extend.
   */
  public static void setFileConfiguration(Configuration conf) {
    String workingPath = System.getProperty("user.dir");
    String stateStorePath = workingPath + "/statestore";
    conf.set(FEDERATION_STORE_FILE_DIRECTORY, stateStorePath);
  }

  /**
   * Clear all the records from the State Store.
   *
   * @param store State Store to remove records from.
   * @return If the State Store was cleared.
   * @throws IOException If it cannot clear the State Store.
   */
  public static boolean clearAllRecords(StateStoreService store)
      throws IOException {
    Collection<Class<? extends BaseRecord>> allRecords =
        store.getSupportedRecords();
    for (Class<? extends BaseRecord> recordType : allRecords) {
      if (!clearRecords(store, recordType)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Clear records from a certain type from the State Store.
   *
   * @param store State Store to remove records from.
   * @param recordClass Class of the records to remove.
   * @return If the State Store was cleared.
   * @throws IOException If it cannot clear the State Store.
   */
  public static <T extends BaseRecord> boolean clearRecords(
      StateStoreService store, Class<T> recordClass) throws IOException {
    List<T> emptyList = new ArrayList<>();
    if (!synchronizeRecords(store, emptyList, recordClass)) {
      return false;
    }
    store.refreshCaches(true);
    return true;
  }

  /**
   * Synchronize a set of records. Remove all and keep the ones specified.
   *
   * @param stateStore State Store service managing the driver.
   * @param records Records to add.
   * @param clazz Class of the record to synchronize.
   * @return If the synchronization succeeded.
   * @throws IOException If it cannot connect to the State Store.
   */
  public static <T extends BaseRecord> boolean synchronizeRecords(
      StateStoreService stateStore, List<T> records, Class<T> clazz)
          throws IOException {
    StateStoreDriver driver = stateStore.getDriver();
    driver.verifyDriverReady();
    if (driver.removeAll(clazz)) {
      if (driver.putAll(records, true, false)) {
        return true;
      }
    }
    return false;
  }

  public static List<MountTable> createMockMountTable(
      List<String> nameservices) throws IOException {
    // create table entries
    List<MountTable> entries = new ArrayList<>();
    for (String ns : nameservices) {
      Map<String, String> destMap = new HashMap<>();
      destMap.put(ns, "/target-" + ns);
      MountTable entry = MountTable.newInstance("/" + ns, destMap);
      entries.add(entry);
    }
    return entries;
  }

  public static MembershipState createMockRegistrationForNamenode(
      String nameserviceId, String namenodeId,
      FederationNamenodeServiceState state) throws IOException {
    MembershipState entry = MembershipState.newInstance(
        "routerId", nameserviceId, namenodeId, "clusterId", "test",
        "0.0.0.0:0", "0.0.0.0:0", "0.0.0.0:0", "0.0.0.0:0", state, false);
    MembershipStats stats = MembershipStats.newInstance();
    stats.setNumOfActiveDatanodes(100);
    stats.setNumOfDeadDatanodes(10);
    stats.setNumOfDecommissioningDatanodes(20);
    stats.setNumOfDecomActiveDatanodes(15);
    stats.setNumOfDecomDeadDatanodes(5);
    stats.setNumOfBlocks(10);
    entry.setStats(stats);
    return entry;
  }
}