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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.commons.lang3.RandomStringUtils;
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
public class TestRDBTableStore {
  private static int count = 0;
  private final List<String> families =
      Arrays.asList(DFSUtil.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private RDBStore rdbStore = null;
  private DBOptions options = null;

  private static boolean consume(Table.KeyValue keyValue)  {
    count++;
    try {
      Assert.assertNotNull(keyValue.getKey());
    } catch(IOException ex) {
      Assert.fail("Unexpected Exception " + ex.toString());
    }
    return true;
  }

  @Before
  public void setUp() throws Exception {
    options = new DBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options = options.setStatistics(statistics);

    Set<TableConfig> configSet = new HashSet<>();
    for(String name : families) {
      TableConfig newConfig = new TableConfig(name, new ColumnFamilyOptions());
      configSet.add(newConfig);
    }
    rdbStore = new RDBStore(folder.newFolder(), options, configSet);
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
  public void getHandle() throws Exception {
    try (Table testTable = rdbStore.getTable("First")) {
      Assert.assertNotNull(testTable);
      Assert.assertNotNull(((RDBTable) testTable).getHandle());
    }
  }

  @Test
  public void putGetAndEmpty() throws Exception {
    try (Table<byte[], byte[]> testTable = rdbStore.getTable("First")) {
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
      Assert.assertFalse(testTable.isEmpty());
      byte[] readValue = testTable.get(key);
      Assert.assertArrayEquals(value, readValue);
    }
    try (Table secondTable = rdbStore.getTable("Second")) {
      Assert.assertTrue(secondTable.isEmpty());
    }
  }

  @Test
  public void delete() throws Exception {
    List<byte[]> deletedKeys = new ArrayList<>();
    List<byte[]> validKeys = new ArrayList<>();
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    for (int x = 0; x < 100; x++) {
      deletedKeys.add(
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8));
    }

    for (int x = 0; x < 100; x++) {
      validKeys.add(
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8));
    }

    // Write all the keys and delete the keys scheduled for delete.
    //Assert we find only expected keys in the Table.
    try (Table testTable = rdbStore.getTable("Fourth")) {
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
    try (Table testTable = rdbStore.getTable("Fifth");
        BatchOperation batch = rdbStore.initBatchOperation()) {
      //given
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      Assert.assertNull(testTable.get(key));

      //when
      testTable.putWithBatch(batch, key, value);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNotNull(testTable.get(key));
    }
  }

  @Test
  public void batchDelete() throws Exception {
    try (Table testTable = rdbStore.getTable("Fifth");
        BatchOperation batch = rdbStore.initBatchOperation()) {

      //given
      byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
      testTable.put(key, value);
      Assert.assertNotNull(testTable.get(key));


      //when
      testTable.deleteWithBatch(batch, key);
      rdbStore.commitBatchOperation(batch);

      //then
      Assert.assertNull(testTable.get(key));
    }
  }

  @Test
  public void forEachAndIterator() throws Exception {
    final int iterCount = 100;
    try (Table testTable = rdbStore.getTable("Sixth")) {
      for (int x = 0; x < iterCount; x++) {
        byte[] key =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        testTable.put(key, value);
      }
      int localCount = 0;
      try (TableIterator<byte[], Table.KeyValue> iter = testTable.iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue keyValue = iter.next();
          localCount++;
        }

        Assert.assertEquals(iterCount, localCount);
        iter.seekToFirst();
        iter.forEachRemaining(TestRDBTableStore::consume);
        Assert.assertEquals(iterCount, count);

      }
    }
  }
}
