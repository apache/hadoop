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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "task")
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskInfo {

  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected float progress;
  protected String id;
  protected TaskState state;
  protected String type;
  protected String successfulAttempt;
  protected String status;

  @XmlTransient
  int taskNum;

  @XmlTransient
  TaskAttempt successful;

  public TaskInfo() {
  }

  public TaskInfo(Task task) {
    TaskType ttype = task.getType();
    this.type = ttype.toString();
    TaskReport report = task.getReport();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.state = report.getTaskState();
    this.elapsedTime = Times.elapsed(this.startTime, this.finishTime,
      this.state == TaskState.RUNNING);
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.progress = report.getProgress() * 100;
    this.status =  report.getStatus();
    this.id = MRApps.toString(task.getID());
    this.taskNum = task.getID().getId();
    this.successful = getSuccessfulAttempt(task);
    if (successful != null) {
      this.successfulAttempt = MRApps.toString(successful.getID());
    } else {
      this.successfulAttempt = "";
    }
  }

  public float getProgress() {
    return this.progress;
  }

  public String getState() {
    return this.state.toString();
  }

  public String getId() {
    return this.id;
  }

  public int getTaskNum() {
    return this.taskNum;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public String getSuccessfulAttempt() {
    return this.successfulAttempt;
  }

  public TaskAttempt getSuccessful() {
    return this.successful;
  }

  private TaskAttempt getSuccessfulAttempt(Task task) {
    for (TaskAttempt attempt : task.getAttempts().values()) {
      if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
        return attempt;
      }
    }
    return null;
  }

  public String getStatus() {
    return status;
  }
}
