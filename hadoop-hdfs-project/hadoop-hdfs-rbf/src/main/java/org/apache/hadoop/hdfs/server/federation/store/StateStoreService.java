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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.NullStateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMBean;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.impl.DisabledNameserviceStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.MembershipStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.MountTableStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.impl.RouterStoreImpl;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A service to initialize a
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver} and maintain the connection to the data store. There are
 * multiple state store driver connections supported:
 * <ul>
 * <li>File {@link
 * org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl
 * StateStoreFileImpl}
 * <li>ZooKeeper {@link
 * org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl
 * StateStoreZooKeeperImpl}
 * </ul>
 * <p>
 * The service also supports the dynamic registration of record stores like:
 * <ul>
 * <li>{@link MembershipStore}: state of the Namenodes in the
 * federation.
 * <li>{@link MountTableStore}: Mount table between to subclusters.
 * See {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * <li>{@link RouterStore}: Router state in the federation.
 * <li>{@link DisabledNameserviceStore}: Disabled name services.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StateStoreService extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreService.class);


  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the service. */
  private String identifier;

  /** Driver for the back end connection. */
  private StateStoreDriver driver;

  /** Service to maintain data store connection. */
  private StateStoreConnectionMonitorService monitorService;

  /** StateStore metrics. */
  private StateStoreMetrics metrics;

  /** Supported record stores. */
  private final Map<
      Class<? extends BaseRecord>, RecordStore<? extends BaseRecord>>
          recordStores;

  /** Service to maintain State Store caches. */
  private StateStoreCacheUpdateService cacheUpdater;
  /** Time the cache was last successfully updated. */
  private long cacheLastUpdateTime;
  /** List of internal caches to update. */
  private final List<StateStoreCache> cachesToUpdateInternal;
  /** List of external caches to update. */
  private final List<StateStoreCache> cachesToUpdateExternal;


  public StateStoreService() {
    super(StateStoreService.class.getName());

    // Records and stores supported by this implementation
    this.recordStores = new HashMap<>();

    // Caches to maintain
    this.cachesToUpdateInternal = new ArrayList<>();
    this.cachesToUpdateExternal = new ArrayList<>();
  }

  /**
   * Initialize the State Store and the connection to the back-end.
   *
   * @param config Configuration for the State Store.
   * @throws IOException Cannot create driver for the State Store.
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;

    // Create implementation of State Store
    Class<? extends StateStoreDriver> driverClass = this.conf.getClass(
        RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS_DEFAULT,
        StateStoreDriver.class);
    this.driver = ReflectionUtils.newInstance(driverClass, this.conf);

    if (this.driver == null) {
      throw new IOException("Cannot create driver for the State Store");
    }

    // Add supported record stores
    addRecordStore(MembershipStoreImpl.class);
    addRecordStore(MountTableStoreImpl.class);
    addRecordStore(RouterStoreImpl.class);
    addRecordStore(DisabledNameserviceStoreImpl.class);

    // Check the connection to the State Store periodically
    this.monitorService = new StateStoreConnectionMonitorService(this);
    this.addService(monitorService);

    // Set expirations intervals for each record
    MembershipState.setExpirationMs(conf.getTimeDuration(
        RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS,
        RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    MembershipState.setDeletionMs(conf.getTimeDuration(
        RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_DELETION_MS,
        RBFConfigKeys
            .FEDERATION_STORE_MEMBERSHIP_EXPIRATION_DELETION_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    RouterState.setExpirationMs(conf.getTimeDuration(
        RBFConfigKeys.FEDERATION_STORE_ROUTER_EXPIRATION_MS,
        RBFConfigKeys.FEDERATION_STORE_ROUTER_EXPIRATION_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    RouterState.setDeletionMs(conf.getTimeDuration(
        RBFConfigKeys.FEDERATION_STORE_ROUTER_EXPIRATION_DELETION_MS,
        RBFConfigKeys.FEDERATION_STORE_ROUTER_EXPIRATION_DELETION_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    // Cache update service
    this.cacheUpdater = new StateStoreCacheUpdateService(this);
    addService(this.cacheUpdater);

    if (conf.getBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE,
        RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE_DEFAULT)) {
      // Create metrics for the State Store
      this.metrics = StateStoreMetrics.create(conf);

      // Adding JMX interface
      try {
        StandardMBean bean = new StandardMBean(metrics, StateStoreMBean.class);
        ObjectName registeredObject =
            MBeans.register("Router", "StateStore", bean);
        LOG.info("Registered StateStoreMBean: {}", registeredObject);
      } catch (NotCompliantMBeanException e) {
        throw new RuntimeException("Bad StateStoreMBean setup", e);
      } catch (MetricsException e) {
        LOG.error("Failed to register State Store bean {}", e.getMessage());
      }
    } else {
      LOG.info("State Store metrics not enabled");
      this.metrics = new NullStateStoreMetrics();
    }

    super.serviceInit(this.conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    loadDriver();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeDriver();

    if (metrics != null) {
      metrics.shutdown();
      metrics = null;
    }

    super.serviceStop();
  }

  /**
   * Add a record store to the State Store. It includes adding the store, the
   * supported record and the cache management.
   *
   * @param <T> Type of the records stored.
   * @param clazz Class of the record store to track.
   * @return New record store.
   * @throws ReflectiveOperationException
   */
  private <T extends RecordStore<?>> void addRecordStore(
      final Class<T> clazz) throws ReflectiveOperationException {

    assert this.getServiceState() == STATE.INITED :
        "Cannot add record to the State Store once started";

    T recordStore = RecordStore.newInstance(clazz, this.getDriver());
    Class<? extends BaseRecord> recordClass = recordStore.getRecordClass();
    this.recordStores.put(recordClass, recordStore);

    // Subscribe for cache updates
    if (recordStore instanceof StateStoreCache) {
      StateStoreCache cachedRecordStore = (StateStoreCache) recordStore;
      this.cachesToUpdateInternal.add(cachedRecordStore);
    }
  }

  /**
   * Get the record store in this State Store for a given interface.
   *
   * @param recordStoreClass Class of the record store.
   * @return Registered record store or null if not found.
   */
  public <T extends RecordStore<?>> T getRegisteredRecordStore(
      final Class<T> recordStoreClass) {
    for (RecordStore<? extends BaseRecord> recordStore :
        this.recordStores.values()) {
      if (recordStoreClass.isInstance(recordStore)) {
        @SuppressWarnings("unchecked")
        T recordStoreChecked = (T) recordStore;
        return recordStoreChecked;
      }
    }
    return null;
  }

  /**
   * List of records supported by this State Store.
   *
   * @return List of supported record classes.
   */
  public Collection<Class<? extends BaseRecord>> getSupportedRecords() {
    return this.recordStores.keySet();
  }

  /**
   * Load the State Store driver. If successful, refresh cached data tables.
   */
  public void loadDriver() {
    synchronized (this.driver) {
      if (!isDriverReady()) {
        String driverName = this.driver.getClass().getSimpleName();
        if (this.driver.init(
            conf, getIdentifier(), getSupportedRecords(), metrics)) {
          LOG.info("Connection to the State Store driver {} is open and ready",
              driverName);
          this.refreshCaches();
        } else {
          LOG.error("Cannot initialize State Store driver {}", driverName);
        }
      }
    }
  }

  /**
   * Check if the driver is ready to be used.
   *
   * @return If the driver is ready.
   */
  public boolean isDriverReady() {
    return this.driver.isDriverReady();
  }

  /**
   * Manually shuts down the driver.
   *
   * @throws Exception If the driver cannot be closed.
   */
  @VisibleForTesting
  public void closeDriver() throws Exception {
    if (this.driver != null) {
      this.driver.close();
    }
  }

  /**
   * Get the state store driver.
   *
   * @return State store driver.
   */
  public StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Fetch a unique identifier for this state store instance. Typically it is
   * the address of the router.
   *
   * @return Unique identifier for this store.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Set a unique synchronization identifier for this store.
   *
   * @param id Unique identifier, typically the router's RPC address.
   */
  public void setIdentifier(String id) {
    this.identifier = id;
  }

  //
  // Cached state store data
  //
  /**
   * The last time the state store cache was fully updated.
   *
   * @return Timestamp.
   */
  public long getCacheUpdateTime() {
    return this.cacheLastUpdateTime;
  }

  /**
   * Stops the cache update service.
   */
  @VisibleForTesting
  public void stopCacheUpdateService() {
    if (this.cacheUpdater != null) {
      this.cacheUpdater.stop();
      removeService(this.cacheUpdater);
      this.cacheUpdater = null;
    }
  }

  /**
   * Register a cached record store for automatic periodic cache updates.
   *
   * @param client Client to the state store.
   */
  public void registerCacheExternal(StateStoreCache client) {
    this.cachesToUpdateExternal.add(client);
  }

  /**
   * Refresh the cache with information from the State Store. Called
   * periodically by the CacheUpdateService to maintain data caches and
   * versions.
   */
  public void refreshCaches() {
    refreshCaches(false);
  }

  /**
   * Refresh the cache with information from the State Store. Called
   * periodically by the CacheUpdateService to maintain data caches and
   * versions.
   * @param force If we force the refresh.
   */
  public void refreshCaches(boolean force) {
    boolean success = true;
    if (isDriverReady()) {
      List<StateStoreCache> cachesToUpdate = new LinkedList<>();
      cachesToUpdate.addAll(cachesToUpdateInternal);
      cachesToUpdate.addAll(cachesToUpdateExternal);
      for (StateStoreCache cachedStore : cachesToUpdate) {
        String cacheName = cachedStore.getClass().getSimpleName();
        boolean result = false;
        try {
          result = cachedStore.loadCache(force);
        } catch (IOException e) {
          LOG.error("Error updating cache for {}", cacheName, e);
          result = false;
        }
        if (!result) {
          success = false;
          LOG.error("Cache update failed for cache {}", cacheName);
        }
      }
    } else {
      success = false;
      LOG.info("Skipping State Store cache update, driver is not ready.");
    }
    if (success) {
      // Uses local time, not driver time.
      this.cacheLastUpdateTime = Time.now();
    }
  }

  /**
   * Update the cache for a specific record store.
   *
   * @param clazz Class of the record store.
   * @return If the cached was loaded.
   * @throws IOException if the cache update failed.
   */
  public boolean loadCache(final Class<?> clazz) throws IOException {
    return loadCache(clazz, false);
  }

  /**
   * Update the cache for a specific record store.
   *
   * @param clazz Class of the record store.
   * @param force Force the update ignoring cached periods.
   * @return If the cached was loaded.
   * @throws IOException if the cache update failed.
   */
  public boolean loadCache(Class<?> clazz, boolean force) throws IOException {
    List<StateStoreCache> cachesToUpdate =
        new LinkedList<StateStoreCache>();
    cachesToUpdate.addAll(this.cachesToUpdateInternal);
    cachesToUpdate.addAll(this.cachesToUpdateExternal);
    for (StateStoreCache cachedStore : cachesToUpdate) {
      if (clazz.isInstance(cachedStore)) {
        return cachedStore.loadCache(force);
      }
    }
    throw new IOException("Registered cache was not found for " + clazz);
  }

  /**
   * Get the metrics for the State Store.
   *
   * @return State Store metrics.
   */
  public StateStoreMetrics getMetrics() {
    return metrics;
  }

}