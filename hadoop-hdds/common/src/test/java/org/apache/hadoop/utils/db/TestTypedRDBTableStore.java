/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Optional;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.db.Table.KeyValue;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * Tests for RocksDBTable Store.
 */
public class TestTypedRDBTableStore {
  private static int count = 0;
  private final List<String> families =
      Arrays.asList(DFSUtil.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth", "Seven", "Eighth",
          "Ninth");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private RDBStore rdbStore = null;
  private DBOptions options = null;
  private CodecRegistry codecRegistry;

  @Before
  public void setUp() throws Exception {
    options = new DBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options = options.setStatistics(statistics);

    Set<TableConfig> configSet = new HashSet<>();
    for (String name : families) {
      TableConfig newConfig = new TableConfig(name, new ColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = new RDBStore(folder.newFolder(), options, configSet);

    codecRegistry = new CodecRegistry();

  }

  @After
  public void tearDown() throws Exception {
    if (rdbStore != null) {
      rdbStore.close();
    }
  }

  @Test
  public void toIOException() {
  }

  @Test
  public void putGetAndEmpty() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "First")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.put(key, value);
      Assert.assertFalse(testTable.isEmpty());
      String readValue = testTable.get(key);
      Assert.assertEquals(value, readValue);
    }
    try (Table secondTable = rdbStore.getTable("Second")) {
      Assert.assertTrue(secondTable.isEmpty());
    }
  }

  private Table<String, String> createTypedTable(String name)
      throws IOException {
    return new TypedTable<String, String>(
        rdbStore.getTable(name),
        codecRegistry,
        String.class, String.class);
  }

  @Test
  public void delete() throws Exception {
    List<String> deletedKeys = new LinkedList<>();
    List<String> validKeys = new LinkedList<>();
    String value =
        RandomStringUtils.random(10);
    for (int x = 0; x < 100; x++) {
      deletedKeys.add(
          RandomStringUtils.random(10));
    }

    for (int x = 0; x < 100; x++) {
      validKeys.add(
          RandomStringUtils.random(10));
    }

    // Write all the keys and delete the keys scheduled for delete.
    //Assert we find only expected keys in the Table.
    try (Table<String, String> testTable = createTypedTable(
        "Fourth")) {
      for (int x = 0; x < deletedKeys.size(); x++) {
        testTable.put(deletedKeys.get(x), value);
        testTable.delete(deletedKeys.get(x));
      }

      for (int x = 0; x < validKeys.size(); x++) {
        testTable.put(validKeys.get(x), value);
      }

      for (int x = 0; x < validKeys.size(); x++) {
        Assert.assertNotNull(testTable.get(validKeys.get(0)));
      }

      for (int x = 0; x < deletedKeys.size(); x++) {
        Assert.assertNull(testTable.get(deletedKeys.get(0)));
      }
    }
  }

  @Test
  public void batchPut() throws Exception {

    try (Table<String, String> testTable = createTypedTable(
        "Fourth");
        BatchOperation batch = rdbStore.initBatchOperation()) {
      //given
      String key =
          RandomStringUtils.random(10);
      String value =
          RandomStringUtils.random(10);

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNotNull(testTable.get(key));
    }
  }

  @Test
  public void batchDelete() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Fourth");
        BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      String key =
          RandomStringUtils.random(10);
      String value =
          RandomStringUtils.random(10);
      testTable.put(key, value);

      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNull(testTable.get(key));
    }
  }

  private static boolean consume(Table.KeyValue keyValue) {
    count++;
    try {
      Assert.assertNotNull(keyValue.getKey());
    } catch (IOException ex) {
      Assert.fail(ex.toString());
    }
    return true;
  }

  @Test
  public void forEachAndIterator() throws Exception {
    final int iterCount = 100;
    try (Table<String, String> testTable = createTypedTable(
        "Sixth")) {
      for (int x = 0; x < iterCount; x++) {
        String key =
            RandomStringUtils.random(10);
        String value =
            RandomStringUtils.random(10);
        testTable.put(key, value);
      }
      int localCount = 0;

      try (TableIterator<String, ? extends KeyValue<String, String>> iter =
          testTable.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue keyValue = iter.next();
          localCount++;
        }

        Assert.assertEquals(iterCount, localCount);
        iter.seekToFirst();
        iter.forEachRemaining(TestTypedRDBTableStore::consume);
        Assert.assertEquals(iterCount, count);

      }
    }
  }

  @Test
  public void testTypedTableWithCache() throws Exception {
    int iterCount = 10;
    try (Table<String, String> testTable = createTypedTable(
        "Seven")) {

      for (int x = 0; x < iterCount; x++) {
        String key = Integer.toString(x);
        String value = Integer.toString(x);
        testTable.addCacheEntry(new CacheKey<>(key),
            new CacheValue<>(Optional.of(value),
            x));
      }

      // As we have added to cache, so get should return value even if it
      // does not exist in DB.
      for (int x = 0; x < iterCount; x++) {
        Assert.assertEquals(Integer.toString(1),
            testTable.get(Integer.toString(1)));
      }

    }
  }

  @Test
  public void testTypedTableWithCacheWithFewDeletedOperationType()
      throws Exception {
    int iterCount = 10;
    try (Table<String, String> testTable = createTypedTable(
        "Seven")) {

      for (int x = 0; x < iterCount; x++) {
        String key = Integer.toString(x);
        String value = Integer.toString(x);
        if (x % 2 == 0) {
          testTable.addCacheEntry(new CacheKey<>(key),
              new CacheValue<>(Optional.of(value), x));
        } else {
          testTable.addCacheEntry(new CacheKey<>(key),
              new CacheValue<>(Optional.absent(),
              x));
        }
      }

      // As we have added to cache, so get should return value even if it
      // does not exist in DB.
      for (int x = 0; x < iterCount; x++) {
        if (x % 2 == 0) {
          Assert.assertEquals(Integer.toString(x),
              testTable.get(Integer.toString(x)));
        } else {
          Assert.assertNull(testTable.get(Integer.toString(x)));
        }
      }

      testTable.cleanupCache(5);

      GenericTestUtils.waitFor(() ->
          ((TypedTable<String, String>) testTable).getCache().size() == 4,
          100, 5000);


      //Check remaining values
      for (int x = 6; x < iterCount; x++) {
        if (x % 2 == 0) {
          Assert.assertEquals(Integer.toString(x),
              testTable.get(Integer.toString(x)));
        } else {
          Assert.assertNull(testTable.get(Integer.toString(x)));
        }
      }


    }
  }

  @Test
  public void testIsExist() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Eighth")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.put(key, value);
      Assert.assertTrue(testTable.isExist(key));

      String invalidKey = key + RandomStringUtils.random(1);
      Assert.assertFalse(testTable.isExist(invalidKey));

      testTable.delete(key);
      Assert.assertFalse(testTable.isExist(key));
    }
  }

  @Test
  public void testIsExistCache() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Eighth")) {
      String key =
          RandomStringUtils.random(10);
      String value = RandomStringUtils.random(10);
      testTable.addCacheEntry(new CacheKey<>(key),
          new CacheValue<>(Optional.of(value), 1L));
      Assert.assertTrue(testTable.isExist(key));

      testTable.addCacheEntry(new CacheKey<>(key),
          new CacheValue<>(Optional.absent(), 1L));
      Assert.assertFalse(testTable.isExist(key));
    }
  }

  @Test
  public void testCountEstimatedRowsInTable() throws Exception {
    try (Table<String, String> testTable = createTypedTable(
        "Ninth")) {
      // Add a few keys
      final int numKeys = 12345;
      for (int i = 0; i < numKeys; i++) {
        String key =
            RandomStringUtils.random(10);
        String value = RandomStringUtils.random(10);
        testTable.put(key, value);
      }
      long keyCount = testTable.getEstimatedKeyCount();
      // The result should be larger than zero but not exceed(?) numKeys
      Assert.assertTrue(keyCount > 0 && keyCount <= numKeys);
    }
  }
}
