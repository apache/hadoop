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

import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.tools.rumen.datatypes.UserName;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.sls.AMDefinitionFactory.adjustTimeValuesToBaselineTime;

public class AMDefinitionRumen extends AMDefinition {
  public final static int DEFAULT_MAPPER_PRIORITY = 20;
  private final static int DEFAULT_REDUCER_PRIORITY = 10;

  public AMDefinitionRumen(AmDefinitionBuilder builder) {
    super(builder);
  }

  public static List<ContainerSimulator> getTaskContainers(LoggedJob job,
      SLSRunner slsRunner) throws YarnException {
    List<ContainerSimulator> containerList = new ArrayList<>();

    TaskContainerDefinition.Builder builder =
        TaskContainerDefinition.Builder.create()
            .withCount(1)
            .withResource(slsRunner.getDefaultContainerResource())
            .withExecutionType(ExecutionType.GUARANTEED)
            .withAllocationId(-1)
            .withRequestDelay(0);

    // mapper
    for (LoggedTask mapTask : job.getMapTasks()) {
      if (mapTask.getAttempts().size() == 0) {
        throw new YarnException("Invalid map task, no attempt for a mapper!");
      }
      LoggedTaskAttempt taskAttempt =
          mapTask.getAttempts().get(mapTask.getAttempts().size() - 1);
      TaskContainerDefinition containerDef = builder
          .withHostname(taskAttempt.getHostName().getValue())
          .withDuration(taskAttempt.getFinishTime() -
              taskAttempt.getStartTime())
          .withPriority(DEFAULT_MAPPER_PRIORITY)
          .withType("map")
          .build();
      containerList.add(
          ContainerSimulator.createFromTaskContainerDefinition(containerDef));
    }

    // reducer
    for (LoggedTask reduceTask : job.getReduceTasks()) {
      if (reduceTask.getAttempts().size() == 0) {
        throw new YarnException(
            "Invalid reduce task, no attempt for a reducer!");
      }
      LoggedTaskAttempt taskAttempt =
          reduceTask.getAttempts().get(reduceTask.getAttempts().size() - 1);
      TaskContainerDefinition containerDef = builder
          .withHostname(taskAttempt.getHostName().getValue())
          .withDuration(taskAttempt.getFinishTime() -
              taskAttempt.getStartTime())
          .withPriority(DEFAULT_REDUCER_PRIORITY)
          .withType("reduce")
          .build();
      containerList.add(
          ContainerSimulator.createFromTaskContainerDefinition(containerDef));
    }

    return containerList;
  }


  public static final class Builder extends AmDefinitionBuilder {
    private long baselineTimeMs;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder withAmType(String amType) {
      this.amType = amType;
      return this;
    }

    public Builder withUser(UserName user) {
      if (user != null) {
        this.user = user.getValue();
      }
      return this;
    }

    public Builder withQueue(String queue) {
      this.queue = queue;
      return this;
    }

    public Builder withJobId(String oldJobId) {
      this.jobId = oldJobId;
      return this;
    }

    public Builder withJobStartTime(long time) {
      this.jobStartTime = time;
      return this;
    }

    public Builder withJobFinishTime(long time) {
      this.jobFinishTime = time;
      return this;
    }

    public Builder withBaseLineTimeMs(long baselineTimeMs) {
      this.baselineTimeMs = baselineTimeMs;
      return this;
    }

    public Builder withLabelExpression(String expr) {
      this.labelExpression = expr;
      return this;
    }

    public AMDefinitionRumen.Builder withTaskContainers(
        List<ContainerSimulator> taskContainers) {
      this.taskContainers = taskContainers;
      return this;
    }

    public AMDefinitionRumen.Builder withAmResource(Resource amResource) {
      this.amResource = amResource;
      return this;
    }

    public AMDefinitionRumen build() {
      AMDefinitionRumen amDef = new AMDefinitionRumen(this);

      if (baselineTimeMs == 0) {
        baselineTimeMs = jobStartTime;
      }
      adjustTimeValuesToBaselineTime(amDef, this, baselineTimeMs);
      return amDef;
    }

  }
}
