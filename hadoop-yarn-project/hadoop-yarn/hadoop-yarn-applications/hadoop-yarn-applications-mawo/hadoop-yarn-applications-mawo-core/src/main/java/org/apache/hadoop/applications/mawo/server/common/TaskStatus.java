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

package org.apache.hadoop.applications.mawo.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.applications.mawo.server.worker.WorkerId;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines TaskStatus for MaWo app.
 */
public class TaskStatus implements Writable, Cloneable {

  /**
   * Set logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(TaskStatus.class);

  /**
   * TaskId is a unique task identifier.
   */
  private TaskId taskId = new TaskId();
  /**
   * epoch time for a task starttime.
   */
  private long startTime;
  /**
   * epoch time for a task endtime.
   */
  private long endTime;
  /**
   * Unique worker identifier.
   */
  private WorkerId workerId = new WorkerId();
  /**
   * Task exit code.
   */
  private int exitCode = -1;
  /**
   * Task cmd.
   */
  private String taskCMD;
  /**
   * Task type.
   */
  private String taskType;

  /**
   * Possible Task States.
   */
  public enum State {
    /**
     * INIT State refers to Task initialization.
     */
    INIT,
    /**
     * RUNNING State refers to Task in Running state.
     */

    RUNNING,
    /**
     * SUCCEEDED State is assigned when task finishes successfully.
     */
    SUCCEEDED,
    /**
     * FAILED State is assigned when task fails.
     */
    FAILED,
    /**
     * KILLED State refers to when a task is killed.
     */
    KILLED,
    /**
     * EXPIRE State refers to when a task is expired.
     */
    EXPIRE
  }

  /**
   * Current Task state.
   */
  private volatile State runState;

  /**
   * Task status constructor.
   */
  public TaskStatus() {
  }

  /**
   * Task status constructor with workerId, TaskId, TaskCmd, TaskType.
   * @param localworkerId : Worker ID
   * @param localtaskId : Task ID
   * @param localtaskCMD : Task command line
   * @param localtaskType : Type of Task
   */
  public TaskStatus(final WorkerId localworkerId, final TaskId localtaskId,
                    final String localtaskCMD, final String localtaskType) {
    this(localworkerId, localtaskId,
        TaskStatus.State.INIT, localtaskCMD,
        localtaskType);
  }

  /**
   * Task status constructor with workerId, TaskId,
   * TaskCmd, TaskType and Run State.
   * @param localworkerId : Worker Id
   * @param localtaskId : Task Id
   * @param localrunState : Task run State
   * @param localtaskCMD : Task cmd
   * @param localtaskType : Task type
   */
  public TaskStatus(final WorkerId localworkerId, final TaskId localtaskId,
      final State localrunState, final String localtaskCMD,
      final String localtaskType) {
    setWorkerId(localworkerId);
    setTaskId(localtaskId);
    setRunState(localrunState);
    setTaskCMD(localtaskCMD);
    setTaskType(localtaskType);
  }

  /**
   * Get status of a Task.
   * @return Status of a Task
   */
  public final State getRunState() {
    return runState;
  }

  /**
   * Update status of a Task.
   * @param localrunState : Status of a Task
   */
  public final void setRunState(final State localrunState) {
    this.runState = localrunState;
  }

  /**
   * Set exitcode of a Task.
   * @param localexitCode : Exitcode of a Task
   */
  public final void setExitCode(final int localexitCode) {
    this.exitCode = localexitCode;
  }

  /**
   * Get exitcode of a Task.
   * @return exitCode of Task
   */
  public final int getExitCode() {
    return exitCode;
  }

  /**
   * Set Task cmd of a Task.
   * @param localcmd : command line which need to be executed
   */
  public final void setTaskCMD(final String localcmd) {
    this.taskCMD = localcmd;
  }

  /**
   * Get Task cmd of a Task.
   * @return TaskCmd : command line which need to be executed
   */
  public final String getTaskCMD() {
    return taskCMD;
  }

  /**
   * Set Task Type.
   * @param localtaskType : TaskType such as SimpleTask, NullTask
   */
  public final void setTaskType(final String localtaskType) {
    this.taskType = localtaskType;
  }

  /**
   * Get Task Type.
   * @return TaskType : TaskType such as SimpleTask, NullTask
   */
  public final String getTaskType() {
    return taskType;
  }

  /**
   * Get Task Id.
   * @return TaskId : Task identifier
   */
  public final TaskId getTaskId() {
    return taskId;
  }

  /**
   * Set TaskId.
   * @param localtaskId : Task identifier
   */
  public final void setTaskId(final TaskId localtaskId) {
    if (localtaskId != null) {
      this.taskId = localtaskId;
    }
  }

  /**
   * Set staus of a Task.
   * @param localtaskId : TaskId of a task
   * @param localrunState : Run state of a task
   */
  public final void setTaskState(final TaskId localtaskId,
      final State localrunState) {
    setTaskId(localtaskId);
    setRunState(localrunState);
  }

  /**
   * Get Task status of a Task.
   * @param localtaskId : Task Id
   * @return TaskStatus for valid Task otherwise Null
   */
  public final State getTaskState(final TaskId localtaskId) {
    if (localtaskId.equals(this.taskId)) {
      return getRunState();
    } else {
      return null;
    }
  }

  /**
   * Get starttime of a Task.
   * @return StartTime of Task
   */
  public final long getStartTime() {
    return startTime;
  }

  /**
   * Set current time as start time of a Task.
   */
  public final void setStartTime() {
    this.startTime = getCurrentTime();
    LOG.debug("Start Time for " + this.taskId + " is " + this.startTime);
  }

  /**
   * Set task start time to a specific time value.
   * @param time : epoch timestamp
   */
  private void setStartTime(final long time) {
    this.startTime = time;
  }

  /**
   * Get task end time.
   * @return End time of task.
   */
  public final long getEndTime() {
    return endTime;
  }

  /**
   * Set task end time to current time.
   */
  public final void setEndTime() {
    this.setEndTime(getCurrentTime());
  }

  /**
   * Set task end time to a specific time value.
   * @param time : epoch timestamp
   */
  private void setEndTime(final long time) {
    this.endTime = time;
    LOG.debug("End Time for " + this.taskId + " is " + this.endTime);
  }

  /**
   * Get current time in milliseconds.
   * @return Current time in milliseconds
   */
  private long getCurrentTime() {
    return System.currentTimeMillis();
  }

  /** {@inheritDoc} */
  public final void write(final DataOutput dataOutput) throws IOException {
    workerId.write(dataOutput);
    taskId.write(dataOutput);
    WritableUtils.writeEnum(dataOutput, runState);
    WritableUtils.writeVLong(dataOutput, getStartTime());
    WritableUtils.writeVLong(dataOutput, getEndTime());
    WritableUtils.writeString(dataOutput, taskCMD);
    WritableUtils.writeString(dataOutput, taskType);
    WritableUtils.writeVInt(dataOutput, exitCode);
  }


  /** {@inheritDoc} */
  public final void readFields(final DataInput dataInput) throws IOException {
    workerId.readFields(dataInput);
    taskId.readFields(dataInput);
    setRunState(WritableUtils.readEnum(dataInput, State.class));
    setStartTime(WritableUtils.readVLong(dataInput));
    setEndTime(WritableUtils.readVLong(dataInput));
    setTaskCMD(WritableUtils.readString(dataInput));
    setTaskType(WritableUtils.readString(dataInput));
    setExitCode(WritableUtils.readVInt(dataInput));
  }

  /**
   * Get workerId.
   * @return workerId : Worker identifier
   */
  public final WorkerId getWorkerId() {
    return workerId;
  }

  /**
   * Set WorkerId.
   * @param localworkerId : Worker identifier
   */
  public final void setWorkerId(final WorkerId localworkerId) {
    this.workerId = localworkerId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    TaskStatus other = (TaskStatus) obj;
    return (getWorkerId().equals(other.getWorkerId()) &&
        getTaskId().equals(other.getTaskId()) &&
        getRunState().equals(other.getRunState()) &&
        getStartTime() == other.getStartTime() &&
        getEndTime() == other.getEndTime() &&
        getTaskCMD().equals(other.getTaskCMD()) &&
        getTaskType().equals(other.getTaskType()) &&
        getExitCode() == other.getExitCode());
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.append(getWorkerId())
        .append(getTaskId())
        .append(getRunState())
        .append(getStartTime())
        .append(getEndTime())
        .append(getTaskCMD())
        .append(getTaskType())
        .append(getExitCode());
    return builder.hashCode();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
