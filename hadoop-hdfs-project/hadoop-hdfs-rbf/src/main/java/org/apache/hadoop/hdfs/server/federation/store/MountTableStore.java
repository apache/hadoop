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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.router.MountTableRefresherService;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaManager;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Management API for the HDFS mount table information stored in
 * {@link org.apache.hadoop.hdfs.server.federation.store.records.MountTable
 * MountTable} records. The mount table contains entries that map a particular
 * global namespace path one or more HDFS nameservices (NN) + target path. It is
 * possible to map mount locations for root folders, directories or individual
 * files.
 * <p>
 * Once fetched from the
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver}, MountTable records are cached in a tree for faster access.
 * Each path in the global namespace is mapped to a nameserivce ID and local
 * path upon request. The cache is periodically updated by the @{link
 * StateStoreCacheUpdateService}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class MountTableStore extends CachedRecordStore<MountTable>
    implements MountTableManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableStore.class);
  private MountTableRefresherService refreshService;
  /** Router quota manager to update quota usage in mount table. */
  private RouterQuotaManager quotaManager;
  public MountTableStore(StateStoreDriver driver) {
    super(MountTable.class, driver);
  }

  public void setRefreshService(MountTableRefresherService refreshService) {
    this.refreshService = refreshService;
  }

  public void setQuotaManager(RouterQuotaManager quotaManager) {
    this.quotaManager = quotaManager;
  }

  public RouterQuotaManager getQuotaManager() {
    return quotaManager;
  }

  /**
   * Update mount table cache of this router as well as all other routers.
   */
  protected void updateCacheAllRouters() {
    if (refreshService != null) {
      try {
        refreshService.refresh();
      } catch (StateStoreUnavailableException e) {
        LOG.error("Cannot refresh mount table: state store not available", e);
      }
    }
  }

}