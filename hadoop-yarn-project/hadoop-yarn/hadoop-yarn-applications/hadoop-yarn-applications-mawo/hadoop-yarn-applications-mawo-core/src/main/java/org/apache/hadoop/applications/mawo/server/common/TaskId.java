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

import org.apache.hadoop.applications.mawo.server.master.job.JobId;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Defines TaskId for MaWo app.
 */
public class TaskId implements Writable {

  /**
   * MaWo TaskIds prefix.
   */
  static final String TASK_ID_PREFIX = "mawo_task_";

  /**
   * MaWo Job ID.
   */
  private JobId jobId = new JobId();
  /**
   * Mawo TaskId.
   */
  private long taskId;

  /**
   * TaskId constructor.
   */
  public TaskId() {
  }

  /**
   * TaskId constructor with jobId and taskId.
   * @param localjobId : Job identifier
   * @param id : Task identifier
   */
  public TaskId(final JobId localjobId, final int id) {
    this.jobId = localjobId;
    this.taskId = id;
  }

  /**
   * Getter method for jobId.
   * @return JobID: Job identifier
   */
  public final int getJobId() {
    return jobId.getID();
  }

  /**
   * Getter method for TaskID.
   * @return TaskId: Task identifier
   */
  public final long getId() {
    return taskId;
  }

  /**
   * Print method for TaskId.
   * @return : Full TaskId which is TaskId_prefix + jobId + _ + TaskId
   */
  public final String toString() {
    return TASK_ID_PREFIX + jobId.getID() + "_" + taskId;
  }

  @Override
  /**
   * Hashcode method for TaskId.
   */
  public final int hashCode() {
    final int prime = 31;
    final int bits = 32;
    int result = 1;
    int jobHash = 0;
    if (jobId == null) {
      jobHash = 0;
    } else {
      jobHash = jobId.hashCode();
    }
    result = prime * result + jobHash;
    result = prime * result + (int) (taskId ^ (taskId >>> bits));
    return result;
  }

  @Override
  /**
   * Equal method override for TaskId.
   */
  public final boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    TaskId other = (TaskId) obj;
    if (jobId == null) {
      if (other.jobId != null) {
        return false;
      }
    } else if (!jobId.equals(other.jobId)) {
      return false;
    }
    if (taskId != other.taskId) {
      return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  public final void write(final DataOutput out) throws IOException {
    jobId.write(out);
    WritableUtils.writeVLong(out, taskId);
  }

  /** {@inheritDoc} */
  public final void readFields(final DataInput in) throws IOException {
    jobId = new JobId();
    jobId.readFields(in);
    this.taskId = WritableUtils.readVLong(in);
  }

}
