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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import static org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils.filterMultiple;
import static org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils.getRecordName;
import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StateStoreDriver} driver implementation that uses ZooKeeper as a
 * backend.
 * <p>
 * The structure of the znodes in the ensemble is:
 * PARENT_PATH
 * |--- MOUNT
 * |--- MEMBERSHIP
 * |--- REBALANCER
 * |--- ROUTERS
 */
public class StateStoreZooKeeperImpl extends StateStoreSerializableImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreZooKeeperImpl.class);


  /** Configuration keys. */
  public static final String FEDERATION_STORE_ZK_DRIVER_PREFIX =
      DFSConfigKeys.FEDERATION_STORE_PREFIX + "driver.zk.";
  public static final String FEDERATION_STORE_ZK_PARENT_PATH =
      FEDERATION_STORE_ZK_DRIVER_PREFIX + "parent-path";
  public static final String FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT =
      "/hdfs-federation";


  /** Directory to store the state store data. */
  private String baseZNode;

  /** Interface to ZooKeeper. */
  private ZKCuratorManager zkManager;


  @Override
  public boolean initDriver() {
    LOG.info("Initializing ZooKeeper connection");

    Configuration conf = getConf();
    baseZNode = conf.get(
        FEDERATION_STORE_ZK_PARENT_PATH,
        FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT);
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
      return false;
    }
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> clazz) {
    try {
      String checkPath = getNodePath(baseZNode, className);
      zkManager.createRootDirRecursively(checkPath);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot initialize ZK node for {}: {}",
          className, e.getMessage());
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    if (zkManager  != null) {
      zkManager.close();
    }
  }

  @Override
  public boolean isDriverReady() {
    return zkManager != null;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    return get(clazz, (String)null);
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz, String sub)
      throws IOException {
    verifyDriverReady();
    long start = monotonicNow();
    List<T> ret = new ArrayList<>();
    String znode = getZNodeForClass(clazz);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        try {
          String path = getNodePath(znode, child);
          Stat stat = new Stat();
          String data = zkManager.getStringData(path, stat);
          boolean corrupted = false;
          if (data == null || data.equals("")) {
            // All records should have data, otherwise this is corrupted
            corrupted = true;
          } else {
            try {
              T record = createRecord(data, stat, clazz);
              ret.add(record);
            } catch (IOException e) {
              LOG.error("Cannot create record type \"{}\" from \"{}\": {}",
                  clazz.getSimpleName(), data, e.getMessage());
              corrupted = true;
            }
          }

          if (corrupted) {
            LOG.error("Cannot get data for {} at {}, cleaning corrupted data",
                child, path);
            zkManager.delete(path);
          }
        } catch (Exception e) {
          LOG.error("Cannot get data for {}: {}", child, e.getMessage());
        }
      }
    } catch (Exception e) {
      getMetrics().addFailure(monotonicNow() - start);
      String msg = "Cannot get children for \"" + znode + "\": " +
          e.getMessage();
      LOG.error(msg);
      throw new IOException(msg);
    }
    long end = monotonicNow();
    getMetrics().addRead(end - start);
    return new QueryResult<T>(ret, getTime());
  }

  @Override
  public <T extends BaseRecord> boolean putAll(
      List<T> records, boolean update, boolean error) throws IOException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return true;
    }

    // All records should be the same
    T record0 = records.get(0);
    Class<? extends BaseRecord> recordClass = record0.getClass();
    String znode = getZNodeForClass(recordClass);

    long start = monotonicNow();
    boolean status = true;
    for (T record : records) {
      String primaryKey = getPrimaryKey(record);
      String recordZNode = getNodePath(znode, primaryKey);
      byte[] data = serialize(record);
      if (!writeNode(recordZNode, data, update, error)){
        status = false;
      }
    }
    long end = monotonicNow();
    if (status) {
      getMetrics().addWrite(end - start);
    } else {
      getMetrics().addFailure(end - start);
    }
    return status;
  }

  @Override
  public <T extends BaseRecord> int remove(
      Class<T> clazz, Query<T> query) throws IOException {
    verifyDriverReady();
    if (query == null) {
      return 0;
    }

    // Read the current data
    long start = monotonicNow();
    List<T> records = null;
    try {
      QueryResult<T> result = get(clazz);
      records = result.getRecords();
    } catch (IOException ex) {
      LOG.error("Cannot get existing records", ex);
      getMetrics().addFailure(monotonicNow() - start);
      return 0;
    }

    // Check the records to remove
    String znode = getZNodeForClass(clazz);
    List<T> recordsToRemove = filterMultiple(query, records);

    // Remove the records
    int removed = 0;
    for (T existingRecord : recordsToRemove) {
      LOG.info("Removing \"{}\"", existingRecord);
      try {
        String primaryKey = getPrimaryKey(existingRecord);
        String path = getNodePath(znode, primaryKey);
        if (zkManager.delete(path)) {
          removed++;
        } else {
          LOG.error("Did not remove \"{}\"", existingRecord);
        }
      } catch (Exception e) {
        LOG.error("Cannot remove \"{}\"", existingRecord, e);
        getMetrics().addFailure(monotonicNow() - start);
      }
    }
    long end = monotonicNow();
    if (removed > 0) {
      getMetrics().addRemove(end - start);
    }
    return removed;
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz)
      throws IOException {
    long start = monotonicNow();
    boolean status = true;
    String znode = getZNodeForClass(clazz);
    LOG.info("Deleting all children under {}", znode);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        String path = getNodePath(znode, child);
        LOG.info("Deleting {}", path);
        zkManager.delete(path);
      }
    } catch (Exception e) {
      LOG.error("Cannot remove {}: {}", znode, e.getMessage());
      status = false;
    }
    long time = monotonicNow() - start;
    if (status) {
      getMetrics().addRemove(time);
    } else {
      getMetrics().addFailure(time);
    }
    return status;
  }

  private boolean writeNode(
      String znode, byte[] bytes, boolean update, boolean error) {
    try {
      boolean created = zkManager.create(znode);
      if (!update && !created && error) {
        LOG.info("Cannot write record \"{}\", it already exists", znode);
        return false;
      }

      // Write data
      zkManager.setData(znode, bytes, -1);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot write record \"{}\": {}", znode, e.getMessage());
    }
    return false;
  }

  /**
   * Get the ZNode for a class.
   *
   * @param clazz Record class to evaluate.
   * @return The ZNode for the class.
   */
  private <T extends BaseRecord> String getZNodeForClass(Class<T> clazz) {
    String className = getRecordName(clazz);
    return getNodePath(baseZNode, className);
  }

  /**
   * Creates a record from a string returned by ZooKeeper.
   *
   * @param data The data to write.
   * @param stat Stat of the data record to create.
   * @param clazz The data record type to create.
   * @return The created record.
   * @throws IOException
   */
  private <T extends BaseRecord> T createRecord(
      String data, Stat stat, Class<T> clazz) throws IOException {
    T record = newRecord(data, clazz, false);
    record.setDateCreated(stat.getCtime());
    record.setDateModified(stat.getMtime());
    return record;
  }
}
