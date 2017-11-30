/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DeletionTasks are supplied to the {@link DeletionService} for deletion.
 */
public abstract class DeletionTask implements Runnable {

  static final Logger LOG =
       LoggerFactory.getLogger(DeletionTask.class);

  public static final int INVALID_TASK_ID = -1;

  private int taskId;
  private String user;
  private DeletionTaskType deletionTaskType;
  private DeletionService deletionService;
  private final AtomicInteger numberOfPendingPredecessorTasks;
  private final Set<DeletionTask> successorTaskSet;
  // By default all tasks will start as success=true; however if any of
  // the dependent task fails then it will be marked as false in
  // deletionTaskFinished().
  private boolean success;

  /**
   * Deletion task with taskId and default values.
   *
   * @param taskId              the ID of the task, if previously set.
   * @param deletionService     the {@link DeletionService}.
   * @param user                the user associated with the delete.
   * @param deletionTaskType    the {@link DeletionTaskType}.
   */
  public DeletionTask(int taskId, DeletionService deletionService, String user,
      DeletionTaskType deletionTaskType) {
    this(taskId, deletionService, user, new AtomicInteger(0),
        new HashSet<DeletionTask>(), deletionTaskType);
  }

  /**
   * Deletion task with taskId and user supplied values.
   *
   * @param taskId              the ID of the task, if previously set.
   * @param deletionService     the {@link DeletionService}.
   * @param user                the user associated with the delete.
   * @param numberOfPendingPredecessorTasks  Number of pending tasks.
   * @param successorTaskSet    the list of successor DeletionTasks
   * @param deletionTaskType    the {@link DeletionTaskType}.
   */
  public DeletionTask(int taskId, DeletionService deletionService, String user,
      AtomicInteger numberOfPendingPredecessorTasks,
      Set<DeletionTask> successorTaskSet, DeletionTaskType deletionTaskType) {
    this.taskId = taskId;
    this.deletionService = deletionService;
    this.user = user;
    this.numberOfPendingPredecessorTasks = numberOfPendingPredecessorTasks;
    this.successorTaskSet = successorTaskSet;
    this.deletionTaskType = deletionTaskType;
    success = true;
  }

  /**
   * Get the taskId for the DeletionTask.
   *
   * @return the taskId.
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * Set the taskId for the DeletionTask.
   *
   * @param taskId the taskId.
   */
  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  /**
   * The the user assoicated with the DeletionTask.
   *
   * @return the user name.
   */
  public String getUser() {
    return user;
  }

  /**
   * Get the {@link DeletionService} for this DeletionTask.
   *
   * @return the {@link DeletionService}.
   */
  public DeletionService getDeletionService() {
    return deletionService;
  }

  /**
   * Get the {@link DeletionTaskType} for this DeletionTask.
   *
   * @return the {@link DeletionTaskType}.
   */
  public DeletionTaskType getDeletionTaskType() {
    return deletionTaskType;
  }

  /**
   * Set the DeletionTask run status.
   *
   * @param success the status of the running DeletionTask.
   */
  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * Return the DeletionTask run status.
   *
   * @return the status of the running DeletionTask.
   */
  public synchronized boolean getSucess() {
    return this.success;
  }

  /**
   * Return the list of successor tasks for the DeletionTask.
   *
   * @return the list of successor tasks.
   */
  public synchronized DeletionTask[] getSuccessorTasks() {
    DeletionTask[] successors = new DeletionTask[successorTaskSet.size()];
    return successorTaskSet.toArray(successors);
  }

  /**
   * Convert the DeletionTask to the Protobuf representation for storing in the
   * state store and recovery.
   *
   * @return the protobuf representation of the DeletionTask.
   */
  public abstract DeletionServiceDeleteTaskProto convertDeletionTaskToProto();

  /**
   * Add a dependent DeletionTask.
   *
   * If there is a task dependency between say tasks 1,2,3 such that
   * task2 and task3 can be started only after task1 then we should define
   * task2 and task3 as successor tasks for task1.
   * Note:- Task dependency should be defined prior to calling delete.
   *
   * @param successorTask the DeletionTask the depends on this DeletionTask.
   */
  public synchronized void addDeletionTaskDependency(
      DeletionTask successorTask) {
    if (successorTaskSet.add(successorTask)) {
      successorTask.incrementAndGetPendingPredecessorTasks();
    }
  }

  /**
   * Increments and returns pending predecessor task count.
   *
   * @return the number of pending predecessor DeletionTasks.
   */
  public int incrementAndGetPendingPredecessorTasks() {
    return numberOfPendingPredecessorTasks.incrementAndGet();
  }

  /**
   * Decrements and returns pending predecessor task count.
   *
   * @return the number of pending predecessor DeletionTasks.
   */
  public int decrementAndGetPendingPredecessorTasks() {
    return numberOfPendingPredecessorTasks.decrementAndGet();
  }

  /**
   * Removes the DeletionTask from the state store and validates that successor
   * tasks have been scheduled and completed.
   *
   * This is called when:
   * 1) Current deletion task ran and finished.
   * 2) When directly called by predecessor task if one of the
   * dependent tasks of it has failed marking its success = false.
   */
  synchronized void deletionTaskFinished() {
    try {
      NMStateStoreService stateStore = deletionService.getStateStore();
      stateStore.removeDeletionTask(taskId);
    } catch (IOException e) {
      LOG.error("Unable to remove deletion task " + taskId
          + " from state store", e);
    }
    Iterator<DeletionTask> successorTaskI = this.successorTaskSet.iterator();
    while (successorTaskI.hasNext()) {
      DeletionTask successorTask = successorTaskI.next();
      if (!success) {
        successorTask.setSuccess(success);
      }
      int count = successorTask.decrementAndGetPendingPredecessorTasks();
      if (count == 0) {
        if (successorTask.getSucess()) {
          successorTask.deletionService.delete(successorTask);
        } else {
          successorTask.deletionTaskFinished();
        }
      }
    }
  }

  /**
   * Return the Protobuf builder with the base DeletionTask attributes.
   *
   * @return pre-populated Buidler with the base attributes.
   */
  DeletionServiceDeleteTaskProto.Builder getBaseDeletionTaskProtoBuilder() {
    DeletionServiceDeleteTaskProto.Builder builder =
        DeletionServiceDeleteTaskProto.newBuilder();
    builder.setId(getTaskId());
    if (getUser() != null) {
      builder.setUser(getUser());
    }
    builder.setDeletionTime(System.currentTimeMillis() +
        TimeUnit.MILLISECONDS.convert(getDeletionService().getDebugDelay(),
            TimeUnit.SECONDS));
    for (DeletionTask successor : getSuccessorTasks()) {
      builder.addSuccessorIds(successor.getTaskId());
    }
    return builder;
  }
}
