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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;

import java.util.Collections;
import java.util.List;

/**
 * Sync tasks for file & dir.
 */
public class FileAndDirsSyncTasks {

  List<SyncTask> fileTasks;
  List<SyncTask> dirTasks;

  public FileAndDirsSyncTasks() {
    this.fileTasks = Lists.newArrayList();
    this.dirTasks = Lists.newArrayList();
  }

  public void addFileSync(SyncTask fileCreate) {
    this.fileTasks.add(fileCreate);
  }

  public void addDirSync(SyncTask dirCreate) {
    this.dirTasks.add(dirCreate);
  }

  public void addAllFileSync(List<SyncTask> fileCreates) {
    this.fileTasks.addAll(fileCreates);
  }

  public List<SyncTask> getDirTasks() {
    return dirTasks;
  }

  public List<SyncTask> getFileTasks() {
    return fileTasks;
  }

  public List<SyncTask> getAllTasks() {
    List<SyncTask> allTasks = Lists.newArrayList();
    allTasks.addAll(dirTasks);
    allTasks.addAll(fileTasks);
    return allTasks;
  }

  public List<SyncTask> getAllTasksForDelete() {
    List<SyncTask> allTasks = Lists.newArrayList();
    Collections.reverse(fileTasks);
    Collections.reverse(dirTasks);
    allTasks.addAll(fileTasks);
    allTasks.addAll(dirTasks);
    return allTasks;
  }

  public void append(FileAndDirsSyncTasks add) {
    this.fileTasks.addAll(add.fileTasks);
    this.dirTasks.addAll(add.dirTasks);
  }
}
