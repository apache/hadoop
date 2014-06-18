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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;

public class NMMemoryStateStoreService extends NMStateStoreService {
  private Map<TrackerKey, TrackerState> trackerStates;
  private Map<Integer, DeletionServiceDeleteTaskProto> deleteTasks;

  public NMMemoryStateStoreService() {
    super(NMMemoryStateStoreService.class.getName());
  }

  private LocalResourceTrackerState loadTrackerState(TrackerState ts) {
    LocalResourceTrackerState result = new LocalResourceTrackerState();
    result.localizedResources.addAll(ts.localizedResources.values());
    for (Map.Entry<Path, LocalResourceProto> entry :
         ts.inProgressMap.entrySet()) {
      result.inProgressResources.put(entry.getValue(), entry.getKey());
    }
    return result;
  }

  private TrackerState getTrackerState(TrackerKey key) {
    TrackerState ts = trackerStates.get(key);
    if (ts == null) {
      ts = new TrackerState();
      trackerStates.put(key, ts);
    }
    return ts;
  }

  @Override
  public synchronized RecoveredLocalizationState loadLocalizationState() {
    RecoveredLocalizationState result = new RecoveredLocalizationState();
    for (Map.Entry<TrackerKey, TrackerState> e : trackerStates.entrySet()) {
      TrackerKey tk = e.getKey();
      TrackerState ts = e.getValue();
      // check what kind of tracker state we have and recover appropriately
      // public trackers have user == null
      // private trackers have a valid user but appId == null
      // app-specific trackers have a valid user and valid appId
      if (tk.user == null) {
        result.publicTrackerState = loadTrackerState(ts);
      } else {
        RecoveredUserResources rur = result.userResources.get(tk.user);
        if (rur == null) {
          rur = new RecoveredUserResources();
          result.userResources.put(tk.user, rur);
        }
        if (tk.appId == null) {
          rur.privateTrackerState = loadTrackerState(ts);
        } else {
          rur.appTrackerStates.put(tk.appId, loadTrackerState(ts));
        }
      }
    }
    return result;
  }

  @Override
  public synchronized void startResourceLocalization(String user,
      ApplicationId appId, LocalResourceProto proto, Path localPath) {
    TrackerState ts = getTrackerState(new TrackerKey(user, appId));
    ts.inProgressMap.put(localPath, proto);
  }

  @Override
  public synchronized void finishResourceLocalization(String user,
      ApplicationId appId, LocalizedResourceProto proto) {
    TrackerState ts = getTrackerState(new TrackerKey(user, appId));
    Path localPath = new Path(proto.getLocalPath());
    ts.inProgressMap.remove(localPath);
    ts.localizedResources.put(localPath, proto);
  }

  @Override
  public synchronized void removeLocalizedResource(String user,
      ApplicationId appId, Path localPath) {
    TrackerState ts = trackerStates.get(new TrackerKey(user, appId));
    if (ts != null) {
      ts.inProgressMap.remove(localPath);
      ts.localizedResources.remove(localPath);
    }
  }

  @Override
  protected void initStorage(Configuration conf) {
    trackerStates = new HashMap<TrackerKey, TrackerState>();
    deleteTasks = new HashMap<Integer, DeletionServiceDeleteTaskProto>();
  }

  @Override
  protected void startStorage() {
  }

  @Override
  protected void closeStorage() {
  }


  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState result =
        new RecoveredDeletionServiceState();
    result.tasks = new ArrayList<DeletionServiceDeleteTaskProto>(
        deleteTasks.values());
    return result;
  }

  @Override
  public synchronized void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
    deleteTasks.put(taskId, taskProto);
  }

  @Override
  public synchronized void removeDeletionTask(int taskId) throws IOException {
    deleteTasks.remove(taskId);
  }


  private static class TrackerState {
    Map<Path, LocalResourceProto> inProgressMap =
        new HashMap<Path, LocalResourceProto>();
    Map<Path, LocalizedResourceProto> localizedResources =
        new HashMap<Path, LocalizedResourceProto>();
  }

  private static class TrackerKey {
    String user;
    ApplicationId appId;

    public TrackerKey(String user, ApplicationId appId) {
      this.user = user;
      this.appId = appId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((appId == null) ? 0 : appId.hashCode());
      result = prime * result + ((user == null) ? 0 : user.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof TrackerKey))
        return false;
      TrackerKey other = (TrackerKey) obj;
      if (appId == null) {
        if (other.appId != null)
          return false;
      } else if (!appId.equals(other.appId))
        return false;
      if (user == null) {
        if (other.user != null)
          return false;
      } else if (!user.equals(other.user))
        return false;
      return true;
    }
  }
}
