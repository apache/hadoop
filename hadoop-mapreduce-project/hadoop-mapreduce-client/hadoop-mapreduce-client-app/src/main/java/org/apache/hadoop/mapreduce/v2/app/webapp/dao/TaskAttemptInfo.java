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

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "taskAttempt")
@XmlSeeAlso({ ReduceTaskAttemptInfo.class })
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskAttemptInfo {

  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected float progress;
  protected String id;
  protected String rack;
  protected TaskAttemptState state;
  protected String status;
  protected String nodeHttpAddress;
  protected String diagnostics;
  protected String type;
  protected String assignedContainerId;

  @XmlTransient
  protected ContainerId assignedContainer;

  public TaskAttemptInfo() {
  }

  public TaskAttemptInfo(TaskAttempt ta, Boolean isRunning) {
    this(ta, TaskType.MAP, isRunning);
  }

  public TaskAttemptInfo(TaskAttempt ta, TaskType type, Boolean isRunning) {
    final TaskAttemptReport report = ta.getReport();
    this.type = type.toString();
    this.id = MRApps.toString(ta.getID());
    this.nodeHttpAddress = ta.getNodeHttpAddress();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.assignedContainer = report.getContainerId();
    if (assignedContainer != null) {
      this.assignedContainerId = assignedContainer.toString();
    }
    this.progress = report.getProgress() * 100;
    this.status = report.getStateString();
    this.state = report.getTaskAttemptState();
    this.elapsedTime = Times
        .elapsed(this.startTime, this.finishTime, isRunning);
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.diagnostics = report.getDiagnosticInfo();
    this.rack = ta.getNodeRackName();
  }

  public String getAssignedContainerIdStr() {
    return this.assignedContainerId;
  }

  public ContainerId getAssignedContainerId() {
    return this.assignedContainer;
  }

  public String getState() {
    return this.state.toString();
  }

  public String getStatus() {
    return status;
  }

  public String getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public float getProgress() {
    return this.progress;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public String getNode() {
    return this.nodeHttpAddress;
  }

  public String getRack() {
    return this.rack;
  }

  public String getNote() {
    return this.diagnostics;
  }

}
