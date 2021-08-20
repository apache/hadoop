/**
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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;

import java.util.List;

/**
 * Maintain metadata sync task list & block sync task list to be scheduled.
 */
public final class SchedulableSyncPhase {

  private final List<MetadataSyncTask> metadataSyncTaskList;
  private final List<BlockSyncTask> blockSyncTaskList;

  private SchedulableSyncPhase(List<MetadataSyncTask> metadataSyncTaskList,
      List<BlockSyncTask> blockSyncTaskList) {
    this.metadataSyncTaskList = metadataSyncTaskList;
    this.blockSyncTaskList = blockSyncTaskList;
  }

  public static SchedulableSyncPhase empty() {
    return new SchedulableSyncPhase(Lists.newArrayList(),
        Lists.newArrayList());
  }

  public static SchedulableSyncPhase create(List<MetadataSyncTask>
      metadataSyncTaskList, List<BlockSyncTask> blockSyncTaskList) {
    return new SchedulableSyncPhase(metadataSyncTaskList, blockSyncTaskList);
  }

  public static SchedulableSyncPhase createBlock(List<BlockSyncTask>
      blockSyncTasks) {
    return new SchedulableSyncPhase(Lists.newArrayList(), blockSyncTasks);
  }

  public static SchedulableSyncPhase createMeta(List<MetadataSyncTask>
      metadataSyncTasks) {
    return new SchedulableSyncPhase(metadataSyncTasks, Lists.newArrayList());
  }

  public List<BlockSyncTask> getBlockSyncTaskList() {
    return blockSyncTaskList;
  }

  public List<MetadataSyncTask> getMetadataSyncTaskList() {
    return metadataSyncTaskList;
  }

  public void append(SchedulableSyncPhase multipartSyncPhase) {
    this.metadataSyncTaskList.addAll(multipartSyncPhase.metadataSyncTaskList);
    this.blockSyncTaskList.addAll(multipartSyncPhase.blockSyncTaskList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchedulableSyncPhase that = (SchedulableSyncPhase) o;
    return
        this.metadataSyncTaskList.containsAll(that.metadataSyncTaskList) &&
            that.metadataSyncTaskList.containsAll(this.metadataSyncTaskList) &&
            this.blockSyncTaskList.containsAll(that.blockSyncTaskList) &&
            that.blockSyncTaskList.containsAll(this.blockSyncTaskList);
  }

  @Override
  public int hashCode() {
    int result = metadataSyncTaskList.hashCode();
    result = 31 * result + blockSyncTaskList.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SchedulableSyncPhase{" +
        "metadataSyncTaskList=" + metadataSyncTaskList +
        ", blockSyncTaskList=" + blockSyncTaskList +
        '}';
  }

  public boolean isEmpty() {
    return metadataSyncTaskList.isEmpty() && blockSyncTaskList.isEmpty();
  }
}
