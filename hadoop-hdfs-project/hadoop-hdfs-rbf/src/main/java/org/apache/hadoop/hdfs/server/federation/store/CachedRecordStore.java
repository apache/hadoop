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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record store that takes care of caching the records in memory.
 *
 * @param <R> Record to store by this interface.
 */
public abstract class CachedRecordStore<R extends BaseRecord>
    extends RecordStore<R> implements StateStoreCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(CachedRecordStore.class);


  /** Prevent loading the cache more than once every 500 ms. */
  private static final long MIN_UPDATE_MS = 500;


  /** Cached entries. */
  private List<R> records = new ArrayList<>();

  /** Time stamp of the cached entries. */
  private long timestamp = -1;

  /** If the cache is initialized. */
  private boolean initialized = false;

  /** Last time the cache was updated. */
  private long lastUpdate = -1;

  /** Lock to access the memory cache. */
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  /** If it should override the expired values when loading the cache. */
  private boolean override = false;


  /**
   * Create a new cached record store.
   *
   * @param clazz Class of the record to store.
   * @param driver State Store driver.
   */
  protected CachedRecordStore(Class<R> clazz, StateStoreDriver driver) {
    this(clazz, driver, false);
  }

  /**
   * Create a new cached record store.
   *
   * @param clazz Class of the record to store.
   * @param driver State Store driver.
   * @param over If the entries should be override if they expire
   */
  protected CachedRecordStore(
      Class<R> clazz, StateStoreDriver driver, boolean over) {
    super(clazz, driver);

    this.override = over;
  }

  /**
   * Check that the cache of the State Store information is available.
   *
   * @throws StateStoreUnavailableException If the cache is not initialized.
   */
  private void checkCacheAvailable() throws StateStoreUnavailableException {
    if (!this.initialized) {
      throw new StateStoreUnavailableException(
          "Cached State Store not initialized, " +
          getRecordClass().getSimpleName() + " records not valid");
    }
  }

  @Override
  public boolean loadCache(boolean force) throws IOException {
    // Prevent loading the cache too frequently
    if (force || isUpdateTime()) {
      List<R> newRecords = null;
      long t = -1;
      try {
        QueryResult<R> result = getDriver().get(getRecordClass());
        newRecords = result.getRecords();
        t = result.getTimestamp();

        // If we have any expired record, update the State Store
        if (this.override) {
          overrideExpiredRecords(result);
        }
      } catch (IOException e) {
        LOG.error("Cannot get \"{}\" records from the State Store",
            getRecordClass().getSimpleName());
        this.initialized = false;
        return false;
      }

      // Update cache atomically
      writeLock.lock();
      try {
        this.records.clear();
        this.records.addAll(newRecords);
        this.timestamp = t;
        this.initialized = true;
      } finally {
        writeLock.unlock();
      }

      // Update the metrics for the cache State Store size
      StateStoreMetrics metrics = getDriver().getMetrics();
      if (metrics != null) {
        String recordName = getRecordClass().getSimpleName();
        metrics.setCacheSize(recordName, this.records.size());
      }

      lastUpdate = Time.monotonicNow();
    }
    return true;
  }

  /**
   * Check if it's time to update the cache. Update it it was never updated.
   *
   * @return If it's time to update this cache.
   */
  private boolean isUpdateTime() {
    return Time.monotonicNow() - lastUpdate > MIN_UPDATE_MS;
  }

  /**
   * Updates the state store with any record overrides we detected, such as an
   * expired state.
   *
   * @param query RecordQueryResult containing the data to be inspected.
   * @throws IOException
   */
  public void overrideExpiredRecords(QueryResult<R> query) throws IOException {
    List<R> commitRecords = new ArrayList<>();
    List<R> newRecords = query.getRecords();
    long currentDriverTime = query.getTimestamp();
    if (newRecords == null || currentDriverTime <= 0) {
      LOG.error("Cannot check overrides for record");
      return;
    }
    for (R record : newRecords) {
      if (record.checkExpired(currentDriverTime)) {
        String recordName = StateStoreUtils.getRecordName(record.getClass());
        LOG.info("Override State Store record {}: {}", recordName, record);
        commitRecords.add(record);
      }
    }
    if (commitRecords.size() > 0) {
      getDriver().putAll(commitRecords, true, false);
    }
  }

  /**
   * Updates the state store with any record overrides we detected, such as an
   * expired state.
   *
   * @param record Record record to be updated.
   * @throws IOException
   */
  public void overrideExpiredRecord(R record) throws IOException {
    List<R> newRecords = Collections.singletonList(record);
    long time = getDriver().getTime();
    QueryResult<R> query = new QueryResult<>(newRecords, time);
    overrideExpiredRecords(query);
  }

  /**
   * Get all the cached records.
   *
   * @return Copy of the cached records.
   * @throws StateStoreUnavailableException If the State store is not available.
   */
  public List<R> getCachedRecords() throws StateStoreUnavailableException {
    checkCacheAvailable();

    List<R> ret = new LinkedList<R>();
    this.readLock.lock();
    try {
      ret.addAll(this.records);
    } finally {
      this.readLock.unlock();
    }
    return ret;
  }

  /**
   * Get all the cached records and the time stamp of the cache.
   *
   * @return Copy of the cached records and the time stamp.
   * @throws StateStoreUnavailableException If the State store is not available.
   */
  protected QueryResult<R> getCachedRecordsAndTimeStamp()
      throws StateStoreUnavailableException {
    checkCacheAvailable();

    this.readLock.lock();
    try {
      return new QueryResult<R>(this.records, this.timestamp);
    } finally {
      this.readLock.unlock();
    }
  }
}
