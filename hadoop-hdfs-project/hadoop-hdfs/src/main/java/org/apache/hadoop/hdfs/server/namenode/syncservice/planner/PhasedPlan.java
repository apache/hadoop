/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.CREATE_DIRS;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.CREATE_FILES;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.DELETES;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.FINISHED;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan.Phases.RENAMES_TO_FINAL;

public class PhasedPlan {

  private List<SyncTask> renameToTemporaryName;
  private List<SyncTask> deleteMetadataSyncTasks;
  private List<SyncTask> renameToFinalName;
  private List<SyncTask> createDirSyncTasks;
  private List<SyncTask> createFileSyncTasks;

  public PhasedPlan(List<SyncTask> renameToTemporaryName,
      List<SyncTask> deleteMetadataSyncTasks,
      List<SyncTask> renameToFinalName,
      List<SyncTask> createDirSyncTasks,
      List<SyncTask> createFileSyncTasks) {
    this.renameToTemporaryName = renameToTemporaryName;
    this.deleteMetadataSyncTasks = deleteMetadataSyncTasks;
    this.renameToFinalName = renameToFinalName;
    this.createDirSyncTasks = createDirSyncTasks;
    this.createFileSyncTasks = createFileSyncTasks;
  }

  public static PhasedPlan empty() {
    return new PhasedPlan(Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
  }

  private static List<SyncTask> popList(List<SyncTask> memberList) {
    List<SyncTask> returnList = Lists.newArrayList(memberList);
    memberList.clear();
    return returnList;
  }

  @VisibleForTesting
  List<SyncTask> peekRenameToTemporaryName() {
    return renameToTemporaryName;
  }

  @VisibleForTesting
  List<SyncTask> peekDeleteMetadataSyncTasks() {
    return deleteMetadataSyncTasks;
  }

  @VisibleForTesting
  List<SyncTask> peekRenameToFinalName() {
    return renameToFinalName;
  }

  @VisibleForTesting
  List<SyncTask> peekCreateDirSyncTasks() {
    return createDirSyncTasks;
  }

  @VisibleForTesting
  List<SyncTask> peekCreateFileSyncTasks() {
    return createFileSyncTasks;
  }


  /**
   * TODO Popping the lists might make our phases enum useless, in hindsight.
   * Consider this. Need to find something for the decision whether we are in the
   * create files phase and need to create a multipart plan when we do that.
   */
  public List<SyncTask> popNextSchedulableWork(Phases currentPhase) {
    switch (currentPhase) {
    case RENAMES_TO_TEMP:
      return popList(renameToTemporaryName);
    case CREATE_DIRS:
      return popList(this.createDirSyncTasks);
    case CREATE_FILES:
      return popList(this.createFileSyncTasks);
    case RENAMES_TO_FINAL:
      return popList(this.renameToFinalName);
    case DELETES:
      return popList(this.deleteMetadataSyncTasks);
    default:
      return Collections.emptyList();
    }
  }

  public boolean isEmpty() {
    return this.createFileSyncTasks.isEmpty() &&
        this.createDirSyncTasks.isEmpty() &&
        this.deleteMetadataSyncTasks.isEmpty() &&
        this.renameToFinalName.isEmpty() &&
        this.renameToTemporaryName.isEmpty();
  }

  public boolean hasNoDownstreamTasksLeft(Phases currentPhase) {
    switch (currentPhase) {
    case RENAMES_TO_TEMP:
      return this.renameToTemporaryName.isEmpty() && hasNoDownstreamTasksLeft(DELETES);
    case DELETES:
      return this.deleteMetadataSyncTasks.isEmpty() && hasNoDownstreamTasksLeft(RENAMES_TO_FINAL);
    case RENAMES_TO_FINAL:
      return this.renameToFinalName.isEmpty() && hasNoDownstreamTasksLeft(CREATE_DIRS);
    case CREATE_DIRS:
      return this.createDirSyncTasks.isEmpty() && hasNoDownstreamTasksLeft(CREATE_FILES);
    case CREATE_FILES:
      return this.createFileSyncTasks.isEmpty() && hasNoDownstreamTasksLeft(FINISHED);
    case FINISHED:
      return true;
    default:
      throw new IllegalArgumentException("Illegal phase " + currentPhase);
    }
  }

  public void filter(Path pathInAliasMap) {
    this.createFileSyncTasks =
        this.createFileSyncTasks
            .stream()
            .filter(syncTask -> syncTask.getUri().getPath().equals(pathInAliasMap.toString()))
            .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "PhasedPlan{" +
        "renameToTemporaryName=" + renameToTemporaryName +
        ", deleteMetadataSyncTasks=" + deleteMetadataSyncTasks +
        ", renameToFinalName=" + renameToFinalName +
        ", createDirSyncTasks=" + createDirSyncTasks +
        ", createFileSyncTasks=" + createFileSyncTasks +
        '}';
  }

  public enum Phases {
    NOT_STARTED,
    RENAMES_TO_TEMP,
    DELETES,
    RENAMES_TO_FINAL,
    CREATE_DIRS,
    CREATE_FILES,
    FINISHED;

    public Phases next() {
      switch (this) {
      case NOT_STARTED:
        return RENAMES_TO_TEMP;
      case RENAMES_TO_TEMP:
        return DELETES;
      case DELETES:
        return RENAMES_TO_FINAL;
      case RENAMES_TO_FINAL:
        return CREATE_DIRS;
      case CREATE_DIRS:
        return CREATE_FILES;
      case CREATE_FILES:
        return FINISHED;
      default:
        return FINISHED;
      }
    }
  }
}
