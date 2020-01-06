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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import static org.apache.hadoop.hdfs.DFSUtil.isParentEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterPermissionChecker;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Time;
import static org.apache.hadoop.hdfs.server.federation.router.Quota.eachByStorageType;

/**
 * Implementation of the {@link MountTableStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MountTableStoreImpl extends MountTableStore {

  public MountTableStoreImpl(StateStoreDriver driver) {
    super(driver);
  }

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(mountTable, FsAction.WRITE);
      }
      mountTable.validate();
    }

    boolean status = getDriver().put(mountTable, false, true);
    AddMountTableEntryResponse response =
        AddMountTableEntryResponse.newInstance();
    response.setStatus(status);
    updateCacheAllRouters();
    return response;
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(mountTable, FsAction.WRITE);
      }
      mountTable.validate();
    }

    boolean status = getDriver().put(mountTable, true, true);
    UpdateMountTableEntryResponse response =
        UpdateMountTableEntryResponse.newInstance();
    response.setStatus(status);
    updateCacheAllRouters();
    return response;
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException {
    final String srcPath = request.getSrcPath();
    final MountTable partial = MountTable.newInstance();
    partial.setSourcePath(srcPath);
    final Query<MountTable> query = new Query<>(partial);
    final MountTable deleteEntry = getDriver().get(getRecordClass(), query);

    boolean status = false;
    if (deleteEntry != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(deleteEntry, FsAction.WRITE);
      }
      status = getDriver().remove(deleteEntry);
    }

    RemoveMountTableEntryResponse response =
        RemoveMountTableEntryResponse.newInstance();
    response.setStatus(status);
    updateCacheAllRouters();
    return response;
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException {
    RouterPermissionChecker pc =
        RouterAdminServer.getPermissionChecker();
    // Get all values from the cache
    List<MountTable> records = getCachedRecords();

    // Sort and filter
    Collections.sort(records, MountTable.SOURCE_COMPARATOR);
    String reqSrcPath = request.getSrcPath();
    if (reqSrcPath != null && !reqSrcPath.isEmpty()) {
      // Return only entries beneath this path
      Iterator<MountTable> it = records.iterator();
      while (it.hasNext()) {
        MountTable record = it.next();
        String srcPath = record.getSourcePath();
        if (!isParentEntry(srcPath, reqSrcPath)) {
          it.remove();
        } else if (pc != null) {
          // do the READ permission check
          try {
            pc.checkPermission(record, FsAction.READ);
          } catch (AccessControlException ignored) {
            // Remove this mount table entry if it cannot
            // be accessed by current user.
            it.remove();
          }
        }
        // If quota manager is not null, update quota usage from quota cache.
        if (this.getQuotaManager() != null) {
          RouterQuotaUsage quota =
              this.getQuotaManager().getQuotaUsage(record.getSourcePath());
          if (quota != null) {
            RouterQuotaUsage oldquota = record.getQuota();
            RouterQuotaUsage.Builder builder = new RouterQuotaUsage.Builder()
                .fileAndDirectoryCount(quota.getFileAndDirectoryCount())
                .quota(oldquota.getQuota())
                .spaceConsumed(quota.getSpaceConsumed())
                .spaceQuota(oldquota.getSpaceQuota());
            eachByStorageType(t -> {
              builder.typeQuota(t, oldquota.getTypeQuota(t));
              builder.typeConsumed(t, quota.getTypeConsumed(t));
            });
            record.setQuota(builder.build());
          }
        }
      }
    }

    GetMountTableEntriesResponse response =
        GetMountTableEntriesResponse.newInstance();
    response.setEntries(records);
    response.setTimestamp(Time.now());
    return response;
  }

  @Override
  public RefreshMountTableEntriesResponse refreshMountTableEntries(
      RefreshMountTableEntriesRequest request) throws IOException {
    // Because this refresh is done through admin API, it should always be force
    // refresh.
    boolean result = loadCache(true);
    RefreshMountTableEntriesResponse response =
        RefreshMountTableEntriesResponse.newInstance();
    response.setResult(result);
    return response;
  }

  @Override
  public GetDestinationResponse getDestination(
      GetDestinationRequest request) throws IOException {
    throw new UnsupportedOperationException("Requires the RouterRpcServer");
  }
}