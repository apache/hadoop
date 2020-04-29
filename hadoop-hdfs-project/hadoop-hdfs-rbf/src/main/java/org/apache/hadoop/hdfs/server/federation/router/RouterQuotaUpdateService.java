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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to periodically update the {@link RouterQuotaUsage}
 * cached information in the {@link Router}.
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
        RBFConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL,
        RBFConfigKeys.DFS_ROUTER_QUOTA_CACHE_UPATE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS));

    super.serviceInit(conf);
  }

  @Override
  protected void periodicInvoke() {
    LOG.debug("Start to update quota cache.");
    try {
      List<MountTable> mountTables = getQuotaSetMountTables();
      Map<RemoteLocation, QuotaUsage> remoteQuotaUsage = new HashMap<>();
      for (MountTable entry : mountTables) {
        String src = entry.getSourcePath();
        RouterQuotaUsage oldQuota = entry.getQuota();
        long nsQuota = oldQuota.getQuota();
        long ssQuota = oldQuota.getSpaceQuota();
        long[] typeQuota = new long[StorageType.values().length];
        Quota.eachByStorageType(
            t -> typeQuota[t.ordinal()] = oldQuota.getTypeQuota(t));

        QuotaUsage currentQuotaUsage = null;

        // Check whether destination path exists in filesystem. When the
        // mtime is zero, the destination is not present and reset the usage.
        // This is because mount table does not have mtime.
        // For other mount entry get current quota usage
        HdfsFileStatus ret = this.rpcServer.getFileInfo(src);
        if (ret == null || ret.getModificationTime() == 0) {
          long[] zeroConsume = new long[StorageType.values().length];
          currentQuotaUsage =
              new RouterQuotaUsage.Builder().fileAndDirectoryCount(0)
                  .quota(nsQuota).spaceConsumed(0).spaceQuota(ssQuota)
                  .typeConsumed(zeroConsume)
                  .typeQuota(typeQuota).build();
        } else {
          // Call RouterRpcServer#getQuotaUsage for getting current quota usage.
          // If any exception occurs catch it and proceed with other entries.
          try {
            Quota quotaModule = this.rpcServer.getQuotaModule();
            Map<RemoteLocation, QuotaUsage> usageMap =
                quotaModule.getEachQuotaUsage(src);
            currentQuotaUsage = quotaModule.aggregateQuota(src, usageMap);
            remoteQuotaUsage.putAll(usageMap);
          } catch (IOException ioe) {
            LOG.error("Unable to get quota usage for " + src, ioe);
            continue;
          }
        }

        RouterQuotaUsage newQuota = generateNewQuota(oldQuota,
            currentQuotaUsage);
        this.quotaManager.put(src, newQuota);
        entry.setQuota(newQuota);
      }

      // Fix inconsistent quota.
      for (Entry<RemoteLocation, QuotaUsage> en : remoteQuotaUsage
          .entrySet()) {
        RemoteLocation remoteLocation = en.getKey();
        QuotaUsage currentQuota = en.getValue();
        fixGlobalQuota(remoteLocation, currentQuota);
      }
    } catch (IOException e) {
      LOG.error("Quota cache updated error.", e);
    }
  }

  private void fixGlobalQuota(RemoteLocation location, QuotaUsage remoteQuota)
      throws IOException {
    QuotaUsage gQuota =
        this.rpcServer.getQuotaModule().getGlobalQuota(location.getSrc());
    if (remoteQuota.getQuota() != gQuota.getQuota()
        || remoteQuota.getSpaceQuota() != gQuota.getSpaceQuota()) {
      this.rpcServer.getQuotaModule()
          .setQuotaInternal(location.getSrc(), Arrays.asList(location),
              gQuota.getQuota(), gQuota.getSpaceQuota(), null);
      LOG.info("[Fix Quota] src={} dst={} oldQuota={}/{} newQuota={}/{}",
          location.getSrc(), location, remoteQuota.getQuota(),
          remoteQuota.getSpaceQuota(), gQuota.getQuota(),
          gQuota.getSpaceQuota());
    }
    for (StorageType t : StorageType.values()) {
      if (remoteQuota.getTypeQuota(t) != gQuota.getTypeQuota(t)) {
        this.rpcServer.getQuotaModule()
            .setQuotaInternal(location.getSrc(), Arrays.asList(location),
                HdfsConstants.QUOTA_DONT_SET, gQuota.getTypeQuota(t), t);
        LOG.info("[Fix Quota] src={} dst={} type={} oldQuota={} newQuota={}",
            location.getSrc(), location, t, remoteQuota.getTypeQuota(t),
            gQuota.getTypeQuota(t));
      }
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
    Set<String> allPaths = this.quotaManager.getAll();
    Set<String> stalePaths = new HashSet<>(allPaths);

    List<MountTable> neededMountTables = new LinkedList<>();
    for (MountTable entry : mountTables) {
      // select mount tables which is quota set
      if (isQuotaSet(entry)) {
        neededMountTables.add(entry);
      }

      // update mount table entries info in quota cache
      String src = entry.getSourcePath();
      this.quotaManager.updateQuota(src, entry.getQuota());
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
    RouterQuotaUsage.Builder newQuotaBuilder = new RouterQuotaUsage.Builder()
        .fileAndDirectoryCount(currentQuotaUsage.getFileAndDirectoryCount())
        .quota(oldQuota.getQuota())
        .spaceConsumed(currentQuotaUsage.getSpaceConsumed())
        .spaceQuota(oldQuota.getSpaceQuota());
    Quota.eachByStorageType(t -> {
      newQuotaBuilder.typeQuota(t, oldQuota.getTypeQuota(t));
      newQuotaBuilder.typeConsumed(t, currentQuotaUsage.getTypeConsumed(t));
    });
    return newQuotaBuilder.build();
  }
}
