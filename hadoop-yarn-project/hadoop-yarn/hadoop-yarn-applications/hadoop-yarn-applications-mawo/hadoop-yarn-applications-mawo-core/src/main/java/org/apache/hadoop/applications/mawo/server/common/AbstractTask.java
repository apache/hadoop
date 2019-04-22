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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Abstract class for MaWo Task.
 */
public abstract class AbstractTask implements Task {
  /**
   * Task identifier.
   */
  private TaskId taskID = new TaskId();
  /**
   * Task environment.
   */
  private Map<String, String> environment = new HashMap<String, String>();
  /**
   * Command which need to be executed as Task.
   */
  private String taskCmd;
  /**
   * Type of task.
   */
  private TaskType taskType;
  /**
   * Task timeout.
   */
  private long timeout;
  /**
   * logger for abstract class.
   */
  static final Logger LOG = LoggerFactory.getLogger(AbstractTask.class);

  /**
   * AbstractTask constructor.
   */
  public AbstractTask() {
  }

  /**
   * AbstrackTask constructor.
   * @param taskId : Task identifier
   * @param localenvironment : Task environment vars
   * @param taskCMD : Cmd to run
   * @param localtimeout : Task timeout in seconds
   */

  public AbstractTask(final TaskId taskId,
      final Map<String, String> localenvironment,
      final String taskCMD, final long localtimeout) {
    this();
    setTaskId(taskId);
    setEnvironment(localenvironment);
    setTaskCmd(taskCMD);
    setTimeout(localtimeout);
    LOG.info("Created Task - type: " + this.taskType + ", TaskId: "
        + this.taskID.toString() + ", cmd: '" + taskCMD + "' Timeout: "
        + timeout);
  }

  /**
   * Get environment for a Task.
   * @return environment of a Task
   */
  @Override
  public final Map<String, String> getEnvironment() {
    return environment;
  }

  /**
   * Set environment for a Task.
   * @param localenvironment : Map of environment vars
   */
  @Override
  public final void setEnvironment(final Map<String, String> localenvironment) {
    this.environment = localenvironment;
  }

  /**
   * Get TaskCmd for a Task.
   * @return TaskCMD: Its a task command line such as sleep 10
   */
  @Override
  public final String getTaskCmd() {
    return taskCmd;
  }

  /**
   * Set TaskCmd for a Task.
   * @param taskCMD : Task command line
   */
  @Override
  public final void setTaskCmd(final String taskCMD) {
    this.taskCmd = taskCMD;
  }

  /**
   * Get TaskId for a Task.
   * @return TaskID: Task command line
   */
  @Override
  public final TaskId getTaskId() {
    return taskID;
  }

  /**
   * Set Task Id.
   * @param taskId : Task Identifier
   */
  @Override
  public final void setTaskId(final TaskId taskId) {
    if (taskId != null) {
      this.taskID = taskId;
    }
  }

  /**
   * Get TaskType for a Task.
   * @return TaskType: Type of Task
   */
  @Override
  public final TaskType getTaskType() {
    return taskType;
  }

  /**
   * Set TaskType for a Task.
   * @param type Simple or Composite Task
   */
  public final void setTaskType(final TaskType type) {
    this.taskType = type;
  }

  /**
   * Get Timeout for a Task.
   * @return timeout in seconds
   */
  @Override
  public final long getTimeout() {
    return this.timeout;
  }

  /**
   * Set Task Timeout in seconds.
   * @param taskTimeout : Timeout in seconds
   */
  @Override
  public final void setTimeout(final long taskTimeout) {
    this.timeout = taskTimeout;
  }

  /**
   * Write Task.
   * @param out : dataoutout object.
   * @throws IOException : Throws IO exception if any error occurs.
   */
  @Override
  public final void write(final DataOutput out) throws IOException {
    taskID.write(out);
    int environmentSize = 0;
    if (environment == null) {
      environmentSize = 0;
    } else {
      environmentSize = environment.size();
    }
    new IntWritable(environmentSize).write(out);
    if (environmentSize != 0) {
      for (Entry<String, String> envEntry : environment.entrySet()) {
        new Text(envEntry.getKey()).write(out);
        new Text(envEntry.getValue()).write(out);
      }
    }
    Text taskCmdText;
    if (taskCmd == null) {
      taskCmdText = new Text("");
    } else {
      taskCmdText = new Text(taskCmd);
    }
    taskCmdText.write(out);
    WritableUtils.writeEnum(out, taskType);
    WritableUtils.writeVLong(out, timeout);
  }

  /**
   * Read Fields from file.
   * @param in : datainput object.
   * @throws IOException : Throws IOException in case of error.
   */
  @Override
  public final void readFields(final DataInput in) throws IOException {
    this.taskID = new TaskId();
    taskID.readFields(in);
    IntWritable envSize = new IntWritable(0);
    envSize.readFields(in);
    for (int i = 0; i < envSize.get(); i++) {
      Text key = new Text();
      Text value = new Text();
      key.readFields(in);
      value.readFields(in);
      environment.put(key.toString(), value.toString());
    }
    Text taskCmdText = new Text();
    taskCmdText.readFields(in);
    taskCmd = taskCmdText.toString();
    taskType = WritableUtils.readEnum(in, TaskType.class);
    timeout = WritableUtils.readVLong(in);
  }

  /**
   * ToString.
   * @return String representation of Task
   */
  @Override
  public final String toString() {
    return "TaskId: " + this.taskID.toString() + ", TaskType: " + this.taskType
        + ", cmd: '" + taskCmd + "'";
  }
}
