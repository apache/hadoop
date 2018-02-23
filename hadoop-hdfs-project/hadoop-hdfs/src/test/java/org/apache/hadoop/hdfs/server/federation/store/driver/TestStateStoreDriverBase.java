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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base tests for the driver. The particular implementations will use this to
 * test their functionality.
 */
public class TestStateStoreDriverBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStateStoreDriverBase.class);

  private static StateStoreService stateStore;
  private static Configuration conf;

  private static final Random RANDOM = new Random();


  /**
   * Get the State Store driver.
   * @return State Store driver.
   */
  protected StateStoreDriver getStateStoreDriver() {
    return stateStore.getDriver();
  }

  @AfterClass
  public static void tearDownCluster() {
    if (stateStore != null) {
      stateStore.stop();
    }
  }

  /**
   * Get a new State Store using this configuration.
   *
   * @param config Configuration for the State Store.
   * @throws Exception If we cannot get the State Store.
   */
  public static void getStateStore(Configuration config) throws Exception {
    conf = config;
    stateStore = FederationStateStoreTestUtils.newStateStore(conf);
  }

  private String generateRandomString() {
    String randomString = "randomString-" + RANDOM.nextInt();
    return randomString;
  }

  private long generateRandomLong() {
    return RANDOM.nextLong();
  }

  @SuppressWarnings("rawtypes")
  private <T extends Enum> T generateRandomEnum(Class<T> enumClass) {
    int x = RANDOM.nextInt(enumClass.getEnumConstants().length);
    T data = enumClass.getEnumConstants()[x];
    return data;
  }

  @SuppressWarnings("unchecked")
  private <T extends BaseRecord> T generateFakeRecord(Class<T> recordClass)
      throws IllegalArgumentException, IllegalAccessException, IOException {

    if (recordClass == MembershipState.class) {
      return (T) MembershipState.newInstance(generateRandomString(),
          generateRandomString(), generateRandomString(),
          generateRandomString(), generateRandomString(),
          generateRandomString(), generateRandomString(),
          generateRandomString(), generateRandomString(),
          generateRandomEnum(FederationNamenodeServiceState.class), false);
    } else if (recordClass == MountTable.class) {
      String src = "/" + generateRandomString();
      Map<String, String> destMap = Collections.singletonMap(
          generateRandomString(), "/" + generateRandomString());
      return (T) MountTable.newInstance(src, destMap);
    } else if (recordClass == RouterState.class) {
      RouterState routerState = RouterState.newInstance(generateRandomString(),
          generateRandomLong(), generateRandomEnum(RouterServiceState.class));
      StateStoreVersion version = generateFakeRecord(StateStoreVersion.class);
      routerState.setStateStoreVersion(version);
      return (T) routerState;
    }

    return null;
  }

  /**
   * Validate if a record is the same.
   *
   * @param original Original record.
   * @param committed Committed record.
   * @param assertEquals Assert if the records are equal or just return.
   * @return If the record is successfully validated.
   */
  private boolean validateRecord(
      BaseRecord original, BaseRecord committed, boolean assertEquals) {

    boolean ret = true;

    Map<String, Class<?>> fields = getFields(original);
    for (String key : fields.keySet()) {
      if (key.equals("dateModified") ||
          key.equals("dateCreated") ||
          key.equals("proto")) {
        // Fields are updated/set on commit and fetch and may not match
        // the fields that are initialized in a non-committed object.
        continue;
      }
      Object data1 = getField(original, key);
      Object data2 = getField(committed, key);
      if (assertEquals) {
        assertEquals("Field " + key + " does not match", data1, data2);
      } else if (!data1.equals(data2)) {
        ret = false;
      }
    }

    long now = stateStore.getDriver().getTime();
    assertTrue(
        committed.getDateCreated() <= now && committed.getDateCreated() > 0);
    assertTrue(committed.getDateModified() >= committed.getDateCreated());

    return ret;
  }

  public static void removeAll(StateStoreDriver driver) throws IOException {
    driver.removeAll(MembershipState.class);
    driver.removeAll(MountTable.class);
  }

  public <T extends BaseRecord> void testInsert(
      StateStoreDriver driver, Class<T> recordClass)
          throws IllegalArgumentException, IllegalAccessException, IOException {

    assertTrue(driver.removeAll(recordClass));
    QueryResult<T> queryResult0 = driver.get(recordClass);
    List<T> records0 = queryResult0.getRecords();
    assertTrue(records0.isEmpty());

    // Insert single
    BaseRecord record = generateFakeRecord(recordClass);
    driver.put(record, true, false);

    // Verify
    QueryResult<T> queryResult1 = driver.get(recordClass);
    List<T> records1 = queryResult1.getRecords();
    assertEquals(1, records1.size());
    T record0 = records1.get(0);
    validateRecord(record, record0, true);

    // Insert multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(recordClass);
      insertList.add(newRecord);
    }
    driver.putAll(insertList, true, false);

    // Verify
    QueryResult<T> queryResult2 = driver.get(recordClass);
    List<T> records2 = queryResult2.getRecords();
    assertEquals(11, records2.size());
  }

  public <T extends BaseRecord> void testFetchErrors(StateStoreDriver driver,
      Class<T> clazz) throws IllegalAccessException, IOException {

    // Fetch empty list
    driver.removeAll(clazz);
    QueryResult<T> result0 = driver.get(clazz);
    assertNotNull(result0);
    List<T> records0 = result0.getRecords();
    assertEquals(records0.size(), 0);

    // Insert single
    BaseRecord record = generateFakeRecord(clazz);
    assertTrue(driver.put(record, true, false));

    // Verify
    QueryResult<T> result1 = driver.get(clazz);
    List<T> records1 = result1.getRecords();
    assertEquals(1, records1.size());
    validateRecord(record, records1.get(0), true);

    // Test fetch single object with a bad query
    final T fakeRecord = generateFakeRecord(clazz);
    final Query<T> query = new Query<T>(fakeRecord);
    T getRecord = driver.get(clazz, query);
    assertNull(getRecord);

    // Test fetch multiple objects does not exist returns empty list
    assertEquals(driver.getMultiple(clazz, query).size(), 0);
  }

  public <T extends BaseRecord> void testPut(
      StateStoreDriver driver, Class<T> clazz)
          throws IllegalArgumentException, ReflectiveOperationException,
          IOException, SecurityException {

    driver.removeAll(clazz);
    QueryResult<T> records = driver.get(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Insert multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }

    // Verify
    assertTrue(driver.putAll(insertList, false, true));
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 10);

    // Generate a new record with the same PK fields as an existing record
    BaseRecord updatedRecord = generateFakeRecord(clazz);
    BaseRecord existingRecord = records.getRecords().get(0);
    Map<String, String> primaryKeys = existingRecord.getPrimaryKeys();
    for (Entry<String, String> entry : primaryKeys.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      Class<?> fieldType = getFieldType(existingRecord, key);
      Object field = fromString(value, fieldType);
      assertTrue(setField(updatedRecord, key, field));
    }

    // Attempt an update of an existing entry, but it is not allowed.
    assertFalse(driver.put(updatedRecord, false, true));

    // Verify no update occurred, all original records are unchanged
    QueryResult<T> newRecords = driver.get(clazz);
    assertTrue(newRecords.getRecords().size() == 10);
    assertEquals("A single entry was improperly updated in the store", 10,
        countMatchingEntries(records.getRecords(), newRecords.getRecords()));

    // Update the entry (allowing updates)
    assertTrue(driver.put(updatedRecord, true, false));

    // Verify that one entry no longer matches the original set
    newRecords = driver.get(clazz);
    assertEquals(10, newRecords.getRecords().size());
    assertEquals(
        "Record of type " + clazz + " not updated in the store", 9,
        countMatchingEntries(records.getRecords(), newRecords.getRecords()));
  }

  private int countMatchingEntries(
      Collection<? extends BaseRecord> committedList,
      Collection<? extends BaseRecord> matchList) {

    int matchingCount = 0;
    for (BaseRecord committed : committedList) {
      for (BaseRecord match : matchList) {
        try {
          if (match.getPrimaryKey().equals(committed.getPrimaryKey())) {
            if (validateRecord(match, committed, false)) {
              matchingCount++;
            }
            break;
          }
        } catch (Exception ex) {
        }
      }
    }
    return matchingCount;
  }

  public <T extends BaseRecord> void testRemove(
      StateStoreDriver driver, Class<T> clazz)
          throws IllegalArgumentException, IllegalAccessException, IOException {

    // Remove all
    assertTrue(driver.removeAll(clazz));
    QueryResult<T> records = driver.get(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Insert multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }

    // Verify
    assertTrue(driver.putAll(insertList, false, true));
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 10);

    // Remove Single
    assertTrue(driver.remove(records.getRecords().get(0)));

    // Verify
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 9);

    // Remove with filter
    final T firstRecord = records.getRecords().get(0);
    final Query<T> query0 = new Query<T>(firstRecord);
    assertTrue(driver.remove(clazz, query0) > 0);

    final T secondRecord = records.getRecords().get(1);
    final Query<T> query1 = new Query<T>(secondRecord);
    assertTrue(driver.remove(clazz, query1) > 0);

    // Verify
    records = driver.get(clazz);
    assertEquals(records.getRecords().size(), 7);

    // Remove all
    assertTrue(driver.removeAll(clazz));

    // Verify
    records = driver.get(clazz);
    assertTrue(records.getRecords().isEmpty());
  }

  public void testInsert(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(driver, MembershipState.class);
    testInsert(driver, MountTable.class);
  }

  public void testPut(StateStoreDriver driver)
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    testPut(driver, MembershipState.class);
    testPut(driver, MountTable.class);
  }

  public void testRemove(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testRemove(driver, MembershipState.class);
    testRemove(driver, MountTable.class);
  }

  public void testFetchErrors(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testFetchErrors(driver, MembershipState.class);
    testFetchErrors(driver, MountTable.class);
  }

  public void testMetrics(StateStoreDriver driver)
      throws IOException, IllegalArgumentException, IllegalAccessException {

    MountTable insertRecord =
        this.generateFakeRecord(MountTable.class);

    // Put single
    StateStoreMetrics metrics = stateStore.getMetrics();
    assertEquals(0, metrics.getWriteOps());
    driver.put(insertRecord, true, false);
    assertEquals(1, metrics.getWriteOps());

    // Put multiple
    metrics.reset();
    assertEquals(0, metrics.getWriteOps());
    driver.put(insertRecord, true, false);
    assertEquals(1, metrics.getWriteOps());

    // Get Single
    metrics.reset();
    assertEquals(0, metrics.getReadOps());

    final String querySourcePath = insertRecord.getSourcePath();
    MountTable partial = MountTable.newInstance();
    partial.setSourcePath(querySourcePath);
    final Query<MountTable> query = new Query<>(partial);
    driver.get(MountTable.class, query);
    assertEquals(1, metrics.getReadOps());

    // GetAll
    metrics.reset();
    assertEquals(0, metrics.getReadOps());
    driver.get(MountTable.class);
    assertEquals(1, metrics.getReadOps());

    // GetMultiple
    metrics.reset();
    assertEquals(0, metrics.getReadOps());
    driver.getMultiple(MountTable.class, query);
    assertEquals(1, metrics.getReadOps());

    // Insert fails
    metrics.reset();
    assertEquals(0, metrics.getFailureOps());
    driver.put(insertRecord, false, true);
    assertEquals(1, metrics.getFailureOps());

    // Remove single
    metrics.reset();
    assertEquals(0, metrics.getRemoveOps());
    driver.remove(insertRecord);
    assertEquals(1, metrics.getRemoveOps());

    // Remove multiple
    metrics.reset();
    driver.put(insertRecord, true, false);
    assertEquals(0, metrics.getRemoveOps());
    driver.remove(MountTable.class, query);
    assertEquals(1, metrics.getRemoveOps());

    // Remove all
    metrics.reset();
    driver.put(insertRecord, true, false);
    assertEquals(0, metrics.getRemoveOps());
    driver.removeAll(MountTable.class);
    assertEquals(1, metrics.getRemoveOps());
  }

  /**
   * Sets the value of a field on the object.
   *
   * @param fieldName The string name of the field.
   * @param data The data to pass to the field's setter.
   *
   * @return True if successful, fails if failed.
   */
  private static boolean setField(
      BaseRecord record, String fieldName, Object data) {

    Method m = locateSetter(record, fieldName);
    if (m != null) {
      try {
        m.invoke(record, data);
      } catch (Exception e) {
        LOG.error("Cannot set field " + fieldName + " on object "
            + record.getClass().getName() + " to data " + data + " of type "
            + data.getClass(), e);
        return false;
      }
    }
    return true;
  }

  /**
   * Finds the appropriate setter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching setter or null if not found.
   */
  private static Method locateSetter(BaseRecord record, String fieldName) {
    for (Method m : record.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("set" + fieldName)) {
        return m;
      }
    }
    return null;
  }

  /**
   * Returns all serializable fields in the object.
   *
   * @return Map with the fields.
   */
  private static Map<String, Class<?>> getFields(BaseRecord record) {
    Map<String, Class<?>> getters = new HashMap<>();
    for (Method m : record.getClass().getDeclaredMethods()) {
      if (m.getName().startsWith("get")) {
        try {
          Class<?> type = m.getReturnType();
          char[] c = m.getName().substring(3).toCharArray();
          c[0] = Character.toLowerCase(c[0]);
          String key = new String(c);
          getters.put(key, type);
        } catch (Exception e) {
          LOG.error("Cannot execute getter " + m.getName()
              + " on object " + record);
        }
      }
    }
    return getters;
  }

  /**
   * Get the type of a field.
   *
   * @param fieldName
   * @return Field type
   */
  private static Class<?> getFieldType(BaseRecord record, String fieldName) {
    Method m = locateGetter(record, fieldName);
    return m.getReturnType();
  }

  /**
   * Fetches the value for a field name.
   *
   * @param fieldName the legacy name of the field.
   * @return The field data or null if not found.
   */
  private static Object getField(BaseRecord record, String fieldName) {
    Object result = null;
    Method m = locateGetter(record, fieldName);
    if (m != null) {
      try {
        result = m.invoke(record);
      } catch (Exception e) {
        LOG.error("Cannot get field " + fieldName + " on object " + record);
      }
    }
    return result;
  }

  /**
   * Finds the appropriate getter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching getter or null if not found.
   */
  private static Method locateGetter(BaseRecord record, String fieldName) {
    for (Method m : record.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("get" + fieldName)) {
        return m;
      }
    }
    return null;
  }

  /**
   * Expands a data object from the store into an record object. Default store
   * data type is a String. Override if additional serialization is required.
   *
   * @param data Object containing the serialized data. Only string is
   *          supported.
   * @param clazz Target object class to hold the deserialized data.
   * @return An instance of the target data object initialized with the
   *         deserialized data.
   */
  @Deprecated
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static <T> T fromString(String data, Class<T> clazz) {

    if (data.equals("null")) {
      return null;
    } else if (clazz == String.class) {
      return (T) data;
    } else if (clazz == Long.class || clazz == long.class) {
      return (T) Long.valueOf(data);
    } else if (clazz == Integer.class || clazz == int.class) {
      return (T) Integer.valueOf(data);
    } else if (clazz == Double.class || clazz == double.class) {
      return (T) Double.valueOf(data);
    } else if (clazz == Float.class || clazz == float.class) {
      return (T) Float.valueOf(data);
    } else if (clazz == Boolean.class || clazz == boolean.class) {
      return (T) Boolean.valueOf(data);
    } else if (clazz.isEnum()) {
      return (T) Enum.valueOf((Class<Enum>) clazz, data);
    }
    return null;
  }
}