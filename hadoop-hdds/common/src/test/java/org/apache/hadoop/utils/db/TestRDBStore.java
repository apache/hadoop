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

import javax.management.MBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdfs.DFSUtil;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * RDBStore Tests.
 */
public class TestRDBStore {
  private final List<String> families =
      Arrays.asList(DFSUtil.bytes2String(RocksDB.DEFAULT_COLUMN_FAMILY),
          "First", "Second", "Third",
          "Fourth", "Fifth",
          "Sixth");
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private RDBStore rdbStore = null;
  private DBOptions options = null;
  private Set<TableConfig> configSet;

  @Before
  public void setUp() throws Exception {
    options = new DBOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);

    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options = options.setStatistics(statistics);
    configSet = new HashSet<>();
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

  private void insertRandomData(RDBStore dbStore, int familyIndex)
      throws Exception {
    try (Table firstTable = dbStore.getTable(families.get(familyIndex))) {
      Assert.assertNotNull("Table cannot be null", firstTable);
      for (int x = 0; x < 100; x++) {
        byte[] key =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
          RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
      }
    }
  }

  @Test
  public void compactDB() throws Exception {
    try (RDBStore newStore =
             new RDBStore(folder.newFolder(), options, configSet)) {
      Assert.assertNotNull("DB Store cannot be null", newStore);
      insertRandomData(newStore, 1);
      // This test does not assert anything if there is any error this test
      // will throw and fail.
      newStore.compactDB();
    }
  }

  @Test
  public void close() throws Exception {
    RDBStore newStore =
        new RDBStore(folder.newFolder(), options, configSet);
    Assert.assertNotNull("DBStore cannot be null", newStore);
    // This test does not assert anything if there is any error this test
    // will throw and fail.
    newStore.close();
  }

  @Test
  public void moveKey() throws Exception {
    byte[] key =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(key, value);
      try (Table<byte[], byte[]> secondTable = rdbStore
          .getTable(families.get(2))) {
        rdbStore.move(key, firstTable, secondTable);
        byte[] newvalue = secondTable.get(key);
        // Make sure we have value in the second table
        Assert.assertNotNull(newvalue);
        //and it is same as what we wrote to the FirstTable
        Assert.assertArrayEquals(value, newvalue);
      }
      // After move this key must not exist in the first table.
      Assert.assertNull(firstTable.get(key));
    }
  }

  @Test
  public void moveWithValue() throws Exception {
    byte[] key =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    byte[] value =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);

    byte[] nextValue =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      firstTable.put(key, value);
      try (Table<byte[], byte[]> secondTable = rdbStore
          .getTable(families.get(2))) {
        rdbStore.move(key, nextValue, firstTable, secondTable);
        byte[] newvalue = secondTable.get(key);
        // Make sure we have value in the second table
        Assert.assertNotNull(newvalue);
        //and it is not same as what we wrote to the FirstTable, and equals
        // the new value.
        Assert.assertArrayEquals(nextValue, nextValue);
      }
    }

  }

  @Test
  public void getEstimatedKeyCount() throws Exception {
    try (RDBStore newStore =
             new RDBStore(folder.newFolder(), options, configSet)) {
      Assert.assertNotNull("DB Store cannot be null", newStore);

      // Write 100 keys to the first table.
      insertRandomData(newStore, 1);

      // Write 100 keys to the secondTable table.
      insertRandomData(newStore, 2);

      // Let us make sure that our estimate is not off by 10%
      Assert.assertTrue(newStore.getEstimatedKeyCount() > 180
          || newStore.getEstimatedKeyCount() < 220);
    }
  }

  @Test
  public void getStatMBeanName() throws Exception {

    try (Table firstTable = rdbStore.getTable(families.get(1))) {
      for (int y = 0; y < 100; y++) {
        byte[] key =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        byte[] value =
            RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
        firstTable.put(key, value);
      }
    }
    MBeanServer platformMBeanServer =
        ManagementFactory.getPlatformMBeanServer();
    Thread.sleep(2000);

    Object keysWritten = platformMBeanServer
        .getAttribute(rdbStore.getStatMBeanName(), "NUMBER_KEYS_WRITTEN");

    Assert.assertTrue(((Long) keysWritten) >= 99L);

    Object dbWriteAverage = platformMBeanServer
        .getAttribute(rdbStore.getStatMBeanName(), "DB_WRITE_AVERAGE");
    Assert.assertTrue((double) dbWriteAverage > 0);
  }

  @Test
  public void getTable() throws Exception {
    for (String tableName : families) {
      try (Table table = rdbStore.getTable(tableName)) {
        Assert.assertNotNull(tableName + "is null", table);
      }
    }
    thrown.expect(IOException.class);
    rdbStore.getTable("ATableWithNoName");
  }

  @Test
  public void listTables() throws Exception {
    List<Table> tableList = rdbStore.listTables();
    Assert.assertNotNull("Table list cannot be null", tableList);
    Map<String, Table> hashTable = new HashMap<>();

    for (Table t : tableList) {
      hashTable.put(t.getName(), t);
    }

    int count = families.size();
    // Assert that we have all the tables in the list and no more.
    for (String name : families) {
      Assert.assertTrue(hashTable.containsKey(name));
      count--;
    }
    Assert.assertEquals(0, count);
  }

  @Test
  public void testRocksDBCheckpoint() throws Exception {
    try (RDBStore newStore =
             new RDBStore(folder.newFolder(), options, configSet)) {
      Assert.assertNotNull("DB Store cannot be null", newStore);

      insertRandomData(newStore, 1);
      DBCheckpoint checkpoint =
          newStore.getCheckpoint(true);
      Assert.assertNotNull(checkpoint);

      RDBStore restoredStoreFromCheckPoint =
          new RDBStore(checkpoint.getCheckpointLocation().toFile(),
              options, configSet);

      // Let us make sure that our estimate is not off by 10%
      Assert.assertTrue(
          restoredStoreFromCheckPoint.getEstimatedKeyCount() > 90
          || restoredStoreFromCheckPoint.getEstimatedKeyCount() < 110);
      checkpoint.cleanupCheckpoint();
    }

  }

  @Test
  public void testRocksDBCheckpointCleanup() throws Exception {
    try (RDBStore newStore =
             new RDBStore(folder.newFolder(), options, configSet)) {
      Assert.assertNotNull("DB Store cannot be null", newStore);

      insertRandomData(newStore, 1);
      DBCheckpoint checkpoint =
          newStore.getCheckpoint(true);
      Assert.assertNotNull(checkpoint);

      Assert.assertTrue(Files.exists(
          checkpoint.getCheckpointLocation()));
      checkpoint.cleanupCheckpoint();
      Assert.assertFalse(Files.exists(
          checkpoint.getCheckpointLocation()));
    }
  }

  @Test
  public void testReadOnlyRocksDB() throws Exception {
    File dbFile = folder.newFolder();
    byte[] key = "Key1".getBytes();
    byte[] value = "Value1".getBytes();

    //Create Rdb and write some data into it.
    RDBStore newStore = new RDBStore(dbFile, options, configSet);
    Assert.assertNotNull("DB Store cannot be null", newStore);
    Table firstTable = newStore.getTable(families.get(0));
    Assert.assertNotNull("Table cannot be null", firstTable);
    firstTable.put(key, value);

    RocksDBCheckpoint checkpoint = (RocksDBCheckpoint) newStore.getCheckpoint(
        true);

    //Create Read Only DB from snapshot of first DB.
    RDBStore snapshotStore = new RDBStore(checkpoint.getCheckpointLocation()
        .toFile(), options, configSet, new CodecRegistry(), true);

    Assert.assertNotNull("DB Store cannot be null", newStore);

    //Verify read is allowed.
    firstTable = snapshotStore.getTable(families.get(0));
    Assert.assertNotNull("Table cannot be null", firstTable);
    Assert.assertTrue(Arrays.equals(((byte[])firstTable.get(key)), value));

    //Verify write is not allowed.
    byte[] key2 =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    byte[] value2 =
        RandomStringUtils.random(10).getBytes(StandardCharsets.UTF_8);
    try {
      firstTable.put(key2, value2);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Not supported operation in read only mode"));
    }
    checkpoint.cleanupCheckpoint();
  }
}