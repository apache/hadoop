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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreOperationResult;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.zookeeper.data.ACL;
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
 * |--- DISABLE_NAMESERVICE
 */
public class StateStoreZooKeeperImpl extends StateStoreSerializableImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreZooKeeperImpl.class);

  /** Service to get/update zk state. */
  private ThreadPoolExecutor executorService;
  private boolean enableConcurrent;


  /** Directory to store the state store data. */
  private String baseZNode;

  /** Interface to ZooKeeper. */
  private ZKCuratorManager zkManager;
  /** ACLs for ZooKeeper. */
  private List<ACL> zkAcl;


  @Override
  public boolean initDriver() {
    LOG.info("Initializing ZooKeeper connection");

    Configuration conf = getConf();
    baseZNode = conf.get(
        RBFConfigKeys.FEDERATION_STORE_ZK_PARENT_PATH,
        RBFConfigKeys.FEDERATION_STORE_ZK_PARENT_PATH_DEFAULT);
    int numThreads = conf.getInt(
        RBFConfigKeys.FEDERATION_STORE_ZK_ASYNC_MAX_THREADS,
        RBFConfigKeys.FEDERATION_STORE_ZK_ASYNC_MAX_THREADS_DEFAULT);
    enableConcurrent = numThreads > 0;
    if (enableConcurrent) {
      ThreadFactory threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("StateStore ZK Client-%d")
          .build();
      this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
          0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
      LOG.info("Init StateStoreZookeeperImpl by async mode with {} threads.", numThreads);
    } else {
      LOG.info("Init StateStoreZookeeperImpl by sync mode.");
    }
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
      this.zkAcl = ZKCuratorManager.getZKAcls(conf);
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
      zkManager.createRootDirRecursively(checkPath, zkAcl);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot initialize ZK node for {}: {}",
          className, e.getMessage());
      return false;
    }
  }

  @VisibleForTesting
  public void setEnableConcurrent(boolean enableConcurrent) {
    this.enableConcurrent = enableConcurrent;
  }

  @Override
  public void close() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
    }
    if (zkManager  != null) {
      zkManager.close();
    }
  }

  @Override
  public boolean isDriverReady() {
    if (zkManager == null) {
      return false;
    }
    CuratorFramework curator = zkManager.getCurator();
    if (curator == null) {
      return false;
    }
    return curator.getState() == CuratorFrameworkState.STARTED;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    verifyDriverReady();
    long start = monotonicNow();
    List<T> ret = new ArrayList<>();
    String znode = getZNodeForClass(clazz);
    try {
      List<Callable<T>> callables = new ArrayList<>();
      zkManager.getChildren(znode).forEach(c -> callables.add(() -> getRecord(clazz, znode, c)));
      if (enableConcurrent) {
        List<Future<T>> futures = executorService.invokeAll(callables);
        for (Future<T> future : futures) {
          if (future.get() != null) {
            ret.add(future.get());
          }
        }
      } else {
        for (Callable<T> callable : callables) {
          T record = callable.call();
          if (record != null) {
            ret.add(record);
          }
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

  /**
   * Get one data record in the StateStore or delete it if it's corrupted.
   *
   * @param clazz Record class to evaluate.
   * @param znode The ZNode for the class.
   * @param child The child for znode to get.
   * @return The record to get.
   */
  private <T extends BaseRecord> T getRecord(Class<T> clazz, String znode, String child) {
    T record = null;
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
          record = createRecord(data, stat, clazz);
        } catch (IOException e) {
          LOG.error("Cannot create record type \"{}\" from \"{}\": {}",
              clazz.getSimpleName(), data, e.getMessage());
          corrupted = true;
        }
      }

      if (corrupted) {
        LOG.error("Cannot get data for {} at {}, cleaning corrupted data", child, path);
        zkManager.delete(path);
      }
    } catch (Exception e) {
      LOG.error("Cannot get data for {}: {}", child, e.getMessage());
    }
    return record;
  }

  @Override
  public <T extends BaseRecord> StateStoreOperationResult putAll(
      List<T> records, boolean update, boolean error) throws IOException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return StateStoreOperationResult.getDefaultSuccessResult();
    }

    // All records should be the same
    T record0 = records.get(0);
    Class<? extends BaseRecord> recordClass = record0.getClass();
    String znode = getZNodeForClass(recordClass);

    long start = monotonicNow();
    final AtomicBoolean status = new AtomicBoolean(true);
    List<Callable<Void>> callables = new ArrayList<>();
    final List<String> failedRecordsKeys = Collections.synchronizedList(new ArrayList<>());
    records.forEach(record ->
        callables.add(
            () -> {
              String primaryKey = getPrimaryKey(record);
              String recordZNode = getNodePath(znode, primaryKey);
              byte[] data = serialize(record);
              if (!writeNode(recordZNode, data, update, error)) {
                failedRecordsKeys.add(getOriginalPrimaryKey(primaryKey));
                status.set(false);
              }
              return null;
            }
        )
    );
    try {
      if (enableConcurrent) {
        executorService.invokeAll(callables);
      } else {
        for(Callable<Void> callable : callables) {
          callable.call();
        }
      }
    } catch (Exception e) {
      LOG.error("Write record failed : {}", e.getMessage(), e);
      throw new IOException(e);
    }
    long end = monotonicNow();
    if (status.get()) {
      getMetrics().addWrite(end - start);
    } else {
      getMetrics().addFailure(end - start);
    }
    return new StateStoreOperationResult(failedRecordsKeys, status.get());
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
