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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically update the {@link RouterQuotaUsage}
 * cached information in the {@link Router} and update corresponding
 * mount table in State Store.
 */
public class RouterQuotaUpdateService extends PeriodicService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterQuotaUpdateService.class);

  private MountTableStore mountTableStore;
  private RouterRpcServer rpcServer;
  /** Router using this Service. */
  private final Router router;
  /** Router Quota manager. */
  private RouterQuotaManager quotaManager;

  public RouterQuotaUpdateService(final Router router) throws IOException {
    super(RouterQuotaUpdateService.class.getName());
    this.router = router;
    this.rpcServer = router.getRpcServer();
    this.quotaManager = router.getQuotaManager();

    if (this.quotaManager == null) {
      throw new IOException("Router quota manager is not initialized.");
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.setIntervalMs(conf.getTimeDuration(
        DFSConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL,
        DFSConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS));

    super.serviceInit(conf);
  }

  @Override
  protected void periodicInvoke() {
    LOG.debug("Start to update quota cache.");
    try {
      List<MountTable> updateMountTables = new LinkedList<>();
      List<MountTable> mountTables = getQuotaSetMountTables();
      for (MountTable entry : mountTables) {
        String src = entry.getSourcePath();
        RouterQuotaUsage oldQuota = entry.getQuota();
        long nsQuota = oldQuota.getQuota();
        long ssQuota = oldQuota.getSpaceQuota();
        // Call RouterRpcServer#getQuotaUsage for getting current quota usage.
        QuotaUsage currentQuotaUsage = this.rpcServer.getQuotaModule()
            .getQuotaUsage(src);
        // If quota is not set in some subclusters under federation path,
        // set quota for this path.
        if (currentQuotaUsage.getQuota() == HdfsConstants.QUOTA_DONT_SET) {
          this.rpcServer.setQuota(src, nsQuota, ssQuota, null);
        }

        RouterQuotaUsage newQuota = generateNewQuota(oldQuota,
            currentQuotaUsage);
        this.quotaManager.put(src, newQuota);
        entry.setQuota(newQuota);

        // only update mount tables which quota was changed
        if (!oldQuota.equals(newQuota)) {
          updateMountTables.add(entry);

          LOG.debug(
              "Update quota usage entity of path: {}, nsCount: {},"
                  + " nsQuota: {}, ssCount: {}, ssQuota: {}.",
              src, newQuota.getFileAndDirectoryCount(),
              newQuota.getQuota(), newQuota.getSpaceConsumed(),
              newQuota.getSpaceQuota());
        }
      }

      updateMountTableEntries(updateMountTables);
    } catch (IOException e) {
      LOG.error("Quota cache updated error.", e);
    }
  }

  /**
   * Get mount table store management interface.
   * @return MountTableStore instance.
   * @throws IOException
   */
  private MountTableStore getMountTableStore() throws IOException {
    if (this.mountTableStore == null) {
      this.mountTableStore = router.getStateStore().getRegisteredRecordStore(
          MountTableStore.class);
      if (this.mountTableStore == null) {
        throw new IOException("Mount table state store is not available.");
      }
    }
    return this.mountTableStore;
  }

  /**
   * Get all the existing mount tables.
   * @return List of mount tables.
   * @throws IOException
   */
  private List<MountTable> getMountTableEntries() throws IOException {
    // scan mount tables from root path
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance("/");
    GetMountTableEntriesResponse getResponse = getMountTableStore()
        .getMountTableEntries(getRequest);
    return getResponse.getEntries();
  }

  /**
   * Get mount tables which quota was set.
   * During this time, the quota usage cache will also be updated by
   * quota manager:
   * 1. Stale paths (entries) will be removed.
   * 2. Existing entries will be override and updated.
   * @return List of mount tables which quota was set.
   * @throws IOException
   */
  private List<MountTable> getQuotaSetMountTables() throws IOException {
    List<MountTable> mountTables = getMountTableEntries();
    Set<String> stalePaths = new HashSet<>();
    for (String path : this.quotaManager.getAll()) {
      stalePaths.add(path);
    }

    List<MountTable> neededMountTables = new LinkedList<>();
    for (MountTable entry : mountTables) {
      // select mount tables which is quota set
      if (isQuotaSet(entry)) {
        neededMountTables.add(entry);
      }

      // update mount table entries info in quota cache
      String src = entry.getSourcePath();
      this.quotaManager.put(src, entry.getQuota());
      stalePaths.remove(src);
    }

    // remove stale paths that currently cached
    for (String stalePath : stalePaths) {
      this.quotaManager.remove(stalePath);
    }

    return neededMountTables;
  }

  /**
   * Check if the quota was set in given MountTable.
   * @param mountTable Mount table entry.
   */
  private boolean isQuotaSet(MountTable mountTable) {
    if (mountTable != null) {
      return this.quotaManager.isQuotaSet(mountTable.getQuota());
    }
    return false;
  }

  /**
   * Generate a new quota based on old quota and current quota usage value.
   * @param oldQuota Old quota stored in State Store.
   * @param currentQuotaUsage Current quota usage value queried from
   *        subcluster.
   * @return A new RouterQuotaUsage.
   */
  private RouterQuotaUsage generateNewQuota(RouterQuotaUsage oldQuota,
      QuotaUsage currentQuotaUsage) {
    RouterQuotaUsage newQuota = new RouterQuotaUsage.Builder()
        .fileAndDirectoryCount(currentQuotaUsage.getFileAndDirectoryCount())
        .quota(oldQuota.getQuota())
        .spaceConsumed(currentQuotaUsage.getSpaceConsumed())
        .spaceQuota(oldQuota.getSpaceQuota()).build();
    return newQuota;
  }

  /**
   * Write out updated mount table entries into State Store.
   * @param updateMountTables Mount tables to be updated.
   * @throws IOException
   */
  private void updateMountTableEntries(List<MountTable> updateMountTables)
      throws IOException {
    for (MountTable entry : updateMountTables) {
      UpdateMountTableEntryRequest updateRequest = UpdateMountTableEntryRequest
          .newInstance(entry);
      getMountTableStore().updateMountTableEntry(updateRequest);
    }
  }
}
