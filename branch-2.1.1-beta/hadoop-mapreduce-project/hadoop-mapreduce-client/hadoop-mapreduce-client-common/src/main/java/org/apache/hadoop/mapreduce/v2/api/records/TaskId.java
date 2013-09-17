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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.text.NumberFormat;

/**
 * <p>
 * <code>TaskId</code> represents the unique identifier for a Map or Reduce
 * Task.
 * </p>
 * 
 * <p>
 * TaskId consists of 3 parts. First part is <code>JobId</code>, that this Task
 * belongs to. Second part of the TaskId is either 'm' or 'r' representing
 * whether the task is a map task or a reduce task. And the third part is the
 * task number.
 * </p>
 */
public abstract class TaskId implements Comparable<TaskId> {

  /**
   * @return the associated <code>JobId</code>
   */
  public abstract JobId getJobId();

  /**
   * @return the type of the task - MAP/REDUCE
   */
  public abstract TaskType getTaskType();

  /**
   * @return the task number.
   */
  public abstract int getId();

  public abstract void setJobId(JobId jobId);

  public abstract void setTaskType(TaskType taskType);

  public abstract void setId(int id);

  protected static final String TASK = "task";

  static final ThreadLocal<NumberFormat> taskIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result = prime * result + getJobId().hashCode();
    result = prime * result + getTaskType().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TaskId other = (TaskId) obj;
    if (getId() != other.getId())
      return false;
    if (!getJobId().equals(other.getJobId()))
      return false;
    if (getTaskType() != other.getTaskType())
      return false;
    return true;
  }
      
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(TASK);
    JobId jobId = getJobId();
    builder.append("_").append(jobId.getAppId().getClusterTimestamp());
    builder.append("_").append(
        JobId.jobIdFormat.get().format(jobId.getAppId().getId()));
    builder.append("_");
    builder.append(getTaskType() == TaskType.MAP ? "m" : "r").append("_");
    builder.append(taskIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(TaskId other) {
    int jobIdComp = this.getJobId().compareTo(other.getJobId());
    if (jobIdComp == 0) {
      if (this.getTaskType() == other.getTaskType()) {
        return this.getId() - other.getId();
      } else {
        return this.getTaskType().compareTo(other.getTaskType());
      }
    } else {
      return jobIdComp;
    }
  }
}