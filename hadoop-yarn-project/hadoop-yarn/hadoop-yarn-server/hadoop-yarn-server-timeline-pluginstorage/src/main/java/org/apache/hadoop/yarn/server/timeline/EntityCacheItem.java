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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Cache item for timeline server v1.5 reader cache. Each cache item has a
 * TimelineStore that can be filled with data within one entity group.
 */
public class EntityCacheItem {
  private static final Logger LOG
      = LoggerFactory.getLogger(EntityCacheItem.class);

  private TimelineStore store;
  private EntityGroupFSTimelineStore.AppLogs appLogs;
  private long lastRefresh;
  private Configuration config;
  private FileSystem fs;

  public EntityCacheItem(Configuration config, FileSystem fs) {
    this.config = config;
    this.fs = fs;
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
   * @param incomingAppLogs
   */
  public synchronized void setAppLogs(
      EntityGroupFSTimelineStore.AppLogs incomingAppLogs) {
    this.appLogs = incomingAppLogs;
  }

  /**
   * @return The timeline store, either loaded or unloaded, of this cache item.
   */
  public synchronized TimelineStore getStore() {
    return store;
  }

  /**
   * Refresh this cache item if it needs refresh. This will enforce an appLogs
   * rescan and then load new data. The refresh process is synchronized with
   * other operations on the same cache item.
   *
   * @param groupId
   * @param aclManager
   * @param jsonFactory
   * @param objMapper
   * @return a {@link org.apache.hadoop.yarn.server.timeline.TimelineStore}
   *         object filled with all entities in the group.
   * @throws IOException
   */
  public synchronized TimelineStore refreshCache(TimelineEntityGroupId groupId,
      TimelineACLsManager aclManager, JsonFactory jsonFactory,
      ObjectMapper objMapper) throws IOException {
    if (needRefresh()) {
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
          store = new MemoryTimelineStore();
          store.init(config);
          store.start();
        }
        TimelineDataManager tdm = new TimelineDataManager(store,
            aclManager);
        tdm.init(config);
        tdm.start();
        List<LogInfo> removeList = new ArrayList<LogInfo>();
        for (LogInfo log : appLogs.getDetailLogs()) {
          LOG.debug("Try refresh logs for {}", log.getFilename());
          // Only refresh the log that matches the cache id
          if (log.matchesGroupId(groupId)) {
            Path appDirPath = appLogs.getAppDirPath();
            if (fs.exists(log.getPath(appDirPath))) {
              LOG.debug("Refresh logs for cache id {}", groupId);
              log.parseForStore(tdm, appDirPath, appLogs.isDone(), jsonFactory,
                  objMapper, fs);
            } else {
              // The log may have been removed, remove the log
              removeList.add(log);
              LOG.info("File {} no longer exists, remove it from log list",
                  log.getPath(appDirPath));
            }
          }
        }
        appLogs.getDetailLogs().removeAll(removeList);
        tdm.close();
      }
      updateRefreshTimeToNow();
    } else {
      LOG.debug("Cache new enough, skip refreshing");
    }
    return store;
  }

  /**
   * Release the cache item for the given group id.
   *
   * @param groupId
   */
  public synchronized void releaseCache(TimelineEntityGroupId groupId) {
    try {
      if (store != null) {
        store.close();
      }
    } catch (IOException e) {
      LOG.warn("Error closing timeline store", e);
    }
    store = null;
    // reset offsets so next time logs are re-parsed
    for (LogInfo log : appLogs.getDetailLogs()) {
      if (log.getFilename().contains(groupId.toString())) {
        log.setOffset(0);
      }
    }
  }

  private boolean needRefresh() {
    return (Time.monotonicNow() - lastRefresh > 10000);
  }

  private void updateRefreshTimeToNow() {
    this.lastRefresh = Time.monotonicNow();
  }
}
