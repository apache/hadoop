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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterPermissionChecker;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreOperationResult;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntriesResponse;
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

  /**
   * Whether a mount table entry can be accessed by the current context.
   *
   * @param src mount entry being accessed
   * @param action type of action being performed on the mount entry
   * @throws AccessControlException if mount table cannot be accessed
   */
  private void checkMountTableEntryPermission(String src, FsAction action)
      throws IOException {
    final MountTable partial = MountTable.newInstance();
    partial.setSourcePath(src);
    final Query<MountTable> query = new Query<>(partial);
    final MountTable entry = getDriver().get(getRecordClass(), query);
    if (entry != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(entry, action);
      }
    }
  }

  /**
   * Check parent path permission recursively. It needs WRITE permission
   * of the nearest parent entry and other EXECUTE permission.
   * @param src mount entry being checked
   * @throws AccessControlException if mount table cannot be accessed
   */
  private void checkMountTablePermission(final String src) throws IOException {
    String parent = src.substring(0, src.lastIndexOf(Path.SEPARATOR));
    checkMountTableEntryPermission(parent, FsAction.WRITE);
    while (!parent.isEmpty()) {
      parent = parent.substring(0, parent.lastIndexOf(Path.SEPARATOR));
      checkMountTableEntryPermission(parent, FsAction.EXECUTE);
    }
  }

  /**
   * When add mount table entry, it needs WRITE permission of the nearest parent
   * entry if exist, and EXECUTE permission of other ancestor entries.
   * @param request add mount table entry request
   * @return add mount table entry response
   * @throws IOException if mount table cannot be accessed
   */
  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      mountTable.validate();
      final String src = mountTable.getSourcePath();
      checkMountTablePermission(src);
      boolean status = getDriver().put(mountTable, false, true);
      AddMountTableEntryResponse response =
          AddMountTableEntryResponse.newInstance();
      response.setStatus(status);
      if (status) {
        updateCacheAllRouters();
      }
      return response;
    } else {
      AddMountTableEntryResponse response =
          AddMountTableEntryResponse.newInstance();
      response.setStatus(false);
      return response;
    }
  }

  @Override
  public AddMountTableEntriesResponse addMountTableEntries(AddMountTableEntriesRequest request)
      throws IOException {
    List<MountTable> mountTables = request.getEntries();
    if (mountTables == null || mountTables.size() == 0) {
      AddMountTableEntriesResponse response = AddMountTableEntriesResponse.newInstance();
      response.setStatus(false);
      response.setFailedRecordsKeys(Collections.emptyList());
      return response;
    }
    for (MountTable mountTable : mountTables) {
      mountTable.validate();
      final String src = mountTable.getSourcePath();
      checkMountTablePermission(src);
    }
    StateStoreOperationResult result = getDriver().putAll(mountTables, false, true);
    boolean status = result.isOperationSuccessful();
    AddMountTableEntriesResponse response = AddMountTableEntriesResponse.newInstance();
    response.setStatus(status);
    response.setFailedRecordsKeys(result.getFailedRecordsKeys());
    if (status) {
      updateCacheAllRouters();
    }
    return response;
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      mountTable.validate();
      final String srcPath = mountTable.getSourcePath();
      checkMountTableEntryPermission(srcPath, FsAction.WRITE);
      boolean status = getDriver().put(mountTable, true, true);
      UpdateMountTableEntryResponse response =
          UpdateMountTableEntryResponse.newInstance();
      response.setStatus(status);
      if (status) {
        updateCacheAllRouters();
      }
      return response;
    } else {
      UpdateMountTableEntryResponse response =
          UpdateMountTableEntryResponse.newInstance();
      response.setStatus(false);
      return response;
    }
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
    if (status) {
      updateCacheAllRouters();
    }
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