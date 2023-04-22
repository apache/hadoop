/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;

import java.util.List;

public abstract class AMDefinition {
  protected int jobCount;
  protected String amType;
  protected String user;
  protected String queue;
  protected long jobStartTime;
  protected long jobFinishTime;
  protected List<ContainerSimulator> taskContainers;
  protected Resource amResource;
  protected String labelExpression;
  protected String oldAppId;

  public AMDefinition(AmDefinitionBuilder builder) {
    this.jobStartTime = builder.jobStartTime;
    this.jobFinishTime = builder.jobFinishTime;
    this.amType = builder.amType;
    this.taskContainers = builder.taskContainers;
    this.labelExpression = builder.labelExpression;
    this.user = builder.user;
    this.amResource = builder.amResource;
    this.queue = builder.queue;
    this.jobCount = builder.jobCount;
    this.oldAppId = builder.jobId;
  }

  public String getAmType() {
    return amType;
  }

  public String getUser() {
    return user;
  }

  public String getOldAppId() {
    return oldAppId;
  }

  public long getJobStartTime() {
    return jobStartTime;
  }

  public long getJobFinishTime() {
    return jobFinishTime;
  }

  public List<ContainerSimulator> getTaskContainers() {
    return taskContainers;
  }

  public Resource getAmResource() {
    return amResource;
  }

  public String getLabelExpression() {
    return labelExpression;
  }

  public String getQueue() {
    return queue;
  }

  public int getJobCount() {
    return jobCount;
  }


  public abstract static class AmDefinitionBuilder {
    private static final String DEFAULT_USER = "default";

    protected int jobCount = 1;
    protected String amType = AMDefinitionFactory.DEFAULT_JOB_TYPE;
    protected String user = DEFAULT_USER;
    protected String queue;
    protected String jobId;
    protected long jobStartTime;
    protected long jobFinishTime;
    protected List<ContainerSimulator> taskContainers;
    protected Resource amResource;
    protected String labelExpression = null;

  }
}
