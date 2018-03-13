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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.CachedRecordStore;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.RecordStore;
import org.apache.hadoop.hdfs.server.federation.store.RouterStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically update the Router current state in the State Store.
 */
public class RouterHeartbeatService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterHeartbeatService.class);

  /** Router we are hearbeating. */
  private final Router router;

  /**
   * Create a new Router heartbeat service.
   *
   * @param router Router to heartbeat.
   */
  public RouterHeartbeatService(Router router) {
    super(RouterHeartbeatService.class.getSimpleName());
    this.router = router;
  }

  /**
   * Trigger the update of the Router state asynchronously.
   */
  protected void updateStateAsync() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        updateStateStore();
      }
    }, "Router Heartbeat Async");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Update the state of the Router in the State Store.
   */
  private synchronized void updateStateStore() {
    String routerId = router.getRouterId();
    if (routerId == null) {
      LOG.error("Cannot heartbeat for router: unknown router id");
      return;
    }
    RouterStore routerStore = router.getRouterStateManager();
    if (routerStore != null) {
      try {
        RouterState record = RouterState.newInstance(
            routerId, router.getStartTime(), router.getRouterState());
        StateStoreVersion stateStoreVersion = StateStoreVersion.newInstance(
            getStateStoreVersion(MembershipStore.class),
            getStateStoreVersion(MountTableStore.class));
        record.setStateStoreVersion(stateStoreVersion);
        RouterHeartbeatRequest request =
            RouterHeartbeatRequest.newInstance(record);
        RouterHeartbeatResponse response = routerStore.routerHeartbeat(request);
        if (!response.getStatus()) {
          LOG.warn("Cannot heartbeat router {}", routerId);
        } else {
          LOG.debug("Router heartbeat for router {}", routerId);
        }
      } catch (IOException e) {
        LOG.error("Cannot heartbeat router {}: {}", routerId, e.getMessage());
      }
    } else {
      LOG.warn("Cannot heartbeat router {}: State Store unavailable", routerId);
    }
  }

  /**
   * Get the version of the data in the State Store.
   *
   * @param clazz Class in the State Store.
   * @return Version of the data.
   */
  private <R extends BaseRecord, S extends RecordStore<R>>
      long getStateStoreVersion(final Class<S> clazz) {
    long version = -1;
    try {
      StateStoreService stateStore = router.getStateStore();
      S recordStore = stateStore.getRegisteredRecordStore(clazz);
      if (recordStore != null) {
        if (recordStore instanceof CachedRecordStore) {
          CachedRecordStore<R> cachedRecordStore =
              (CachedRecordStore<R>) recordStore;
          List<R> records = cachedRecordStore.getCachedRecords();
          for (BaseRecord record : records) {
            if (record.getDateModified() > version) {
              version = record.getDateModified();
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Cannot get version for {}: {}", clazz, e.getMessage());
    }
    return version;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    long interval = conf.getTimeDuration(
        DFSConfigKeys.DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS,
        DFSConfigKeys.DFS_ROUTER_HEARTBEAT_STATE_INTERVAL_MS_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.setIntervalMs(interval);

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    updateStateStore();
  }
}