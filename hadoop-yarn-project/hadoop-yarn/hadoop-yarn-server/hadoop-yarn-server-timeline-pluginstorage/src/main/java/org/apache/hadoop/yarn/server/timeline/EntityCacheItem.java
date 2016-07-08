/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timeline;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache item for timeline server v1.5 reader cache. Each cache item has a
 * TimelineStore that can be filled with data within one entity group.
 */
public class EntityCacheItem {
  private static final Logger LOG
      = LoggerFactory.getLogger(EntityCacheItem.class);

  private TimelineStore store;
  private TimelineEntityGroupId groupId;
  private EntityGroupFSTimelineStore.AppLogs appLogs;
  private long lastRefresh;
  private Configuration config;
  private int refCount = 0;
  private static AtomicInteger activeStores = new AtomicInteger(0);

  public EntityCacheItem(TimelineEntityGroupId gId, Configuration config) {
    this.groupId = gId;
    this.config = config;
  }

  /**
   * @return The application log associated to this cache item, may be null.
   */
  public synchronized EntityGroupFSTimelineStore.AppLogs getAppLogs() {
    return this.appLogs;
  }

  /**
   * Set the application logs to this cache item. The entity group should be
   * associated with this application.
   *
   * @param incomingAppLogs Application logs this cache item mapped to
   */
  public synchronized void setAppLogs(
      EntityGroupFSTimelineStore.AppLogs incomingAppLogs) {
    this.appLogs = incomingAppLogs;
  }

  /**
   * @return The timeline store, either loaded or unloaded, of this cache item.
   * This method will not hold the storage from being reclaimed.
   */
  public synchronized TimelineStore getStore() {
    return store;
  }

  /**
   * @return The number of currently active stores in all CacheItems.
   */
  public static int getActiveStores() {
    return activeStores.get();
  }

  /**
   * Refresh this cache item if it needs refresh. This will enforce an appLogs
   * rescan and then load new data. The refresh process is synchronized with
   * other operations on the same cache item.
   *
   * @param aclManager ACL manager for the timeline storage
   * @param metrics Metrics to trace the status of the entity group store
   * @return a {@link org.apache.hadoop.yarn.server.timeline.TimelineStore}
   *         object filled with all entities in the group.
   * @throws IOException
   */
  public synchronized TimelineStore refreshCache(TimelineACLsManager aclManager,
      EntityGroupFSTimelineStoreMetrics metrics) throws IOException {
    if (needRefresh()) {
      long startTime = Time.monotonicNow();
      // If an application is not finished, we only update summary logs (and put
      // new entities into summary storage).
      // Otherwise, since the application is done, we can update detail logs.
      if (!appLogs.isDone()) {
        appLogs.parseSummaryLogs();
      } else if (appLogs.getDetailLogs().isEmpty()) {
        appLogs.scanForLogs();
      }
      if (!appLogs.getDetailLogs().isEmpty()) {
        if (store == null) {
          activeStores.getAndIncrement();
          store = new LevelDBCacheTimelineStore(groupId.toString(),
              "LeveldbCache." + groupId);
          store.init(config);
          store.start();
        } else {
          // Store is not null, the refresh is triggered by stale storage.
          metrics.incrCacheStaleRefreshes();
        }
        try (TimelineDataManager tdm =
                new TimelineDataManager(store, aclManager)) {
          tdm.init(config);
          tdm.start();
          // Load data from appLogs to tdm
          appLogs.loadDetailLog(tdm, groupId);
        }
      }
      updateRefreshTimeToNow();
      metrics.addCacheRefreshTime(Time.monotonicNow() - startTime);
    } else {
      LOG.debug("Cache new enough, skip refreshing");
      metrics.incrNoRefreshCacheRead();
    }
    return store;
  }

  /**
   * Increase the number of references to this cache item by 1.
   */
  public synchronized void incrRefs() {
    refCount++;
  }

  /**
   * Unregister a reader. Try to release the cache if the reader to current
   * cache reaches 0.
   *
   * @return true if the cache has been released, otherwise false
   */
  public synchronized boolean tryRelease() {
    refCount--;
    // Only reclaim the storage if there is no reader.
    if (refCount > 0) {
      LOG.debug("{} references left for cached group {}, skipping the release",
          refCount, groupId);
      return false;
    }
    forceRelease();
    return true;
  }

  /**
   * Force releasing the cache item for the given group id, even though there
   * may be active references.
   */
  public synchronized void forceRelease() {
    try {
      if (store != null) {
        store.close();
      }
    } catch (IOException e) {
      LOG.warn("Error closing timeline store", e);
    }
    store = null;
    activeStores.getAndDecrement();
    refCount = 0;
    // reset offsets so next time logs are re-parsed
    for (LogInfo log : appLogs.getDetailLogs()) {
      if (log.getFilename().contains(groupId.toString())) {
        log.setOffset(0);
      }
    }
    LOG.debug("Cache for group {} released. ", groupId);
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  synchronized int getRefCount() {
    return refCount;
  }

  private boolean needRefresh() {
    return (Time.monotonicNow() - lastRefresh > 10000);
  }

  private void updateRefreshTimeToNow() {
    this.lastRefresh = Time.monotonicNow();
  }
}
