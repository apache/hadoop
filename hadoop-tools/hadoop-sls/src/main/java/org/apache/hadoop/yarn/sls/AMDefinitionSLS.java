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
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AMDefinitionSLS extends AMDefinition {
  public AMDefinitionSLS(AmDefinitionBuilder builder) {
    super(builder);
  }

  public String getQueue() {
    return queue;
  }

  public static List<ContainerSimulator> getTaskContainers(Map<String, ?> jsonJob,
      SLSRunner slsRunner) throws YarnException {
    List<Map<String, String>> tasks = (List) jsonJob.get(SLSConfiguration.JOB_TASKS);
    if (tasks == null || tasks.size() == 0) {
      throw new YarnException("No task for the job!");
    }

    List<ContainerSimulator> containers = new ArrayList<>();
    for (Map<String, String> jsonTask : tasks) {
      TaskContainerDefinition containerDef =
          TaskContainerDefinition.Builder.create()
              .withCount(jsonTask, SLSConfiguration.COUNT)
              .withHostname(jsonTask.get(SLSConfiguration.TASK_HOST))
              .withDuration(jsonTask, SLSConfiguration.TASK_DURATION_MS)
              .withDurationLegacy(jsonTask, SLSConfiguration.DURATION_MS)
              .withTaskStart(jsonTask, SLSConfiguration.TASK_START_MS)
              .withTaskFinish(jsonTask, SLSConfiguration.TASK_END_MS)
              .withResource(getResourceForContainer(jsonTask, slsRunner))
              .withPriority(jsonTask, SLSConfiguration.TASK_PRIORITY)
              .withType(jsonTask, SLSConfiguration.TASK_TYPE)
              .withExecutionType(jsonTask, SLSConfiguration.TASK_EXECUTION_TYPE)
              .withAllocationId(jsonTask, SLSConfiguration.TASK_ALLOCATION_ID)
              .withRequestDelay(jsonTask, SLSConfiguration.TASK_REQUEST_DELAY)
              .build();

      for (int i = 0; i < containerDef.getCount(); i++) {
        containers.add(ContainerSimulator.
            createFromTaskContainerDefinition(containerDef));
      }
    }
    return containers;
  }

  private static Resource getResourceForContainer(Map<String, String> jsonTask,
      SLSRunner slsRunner) {
    Resource res = slsRunner.getDefaultContainerResource();
    ResourceInformation[] infors = ResourceUtils.getResourceTypesArray();
    for (ResourceInformation info : infors) {
      if (jsonTask.containsKey(SLSConfiguration.TASK_PREFIX + info.getName())) {
        long value = Long.parseLong(
            jsonTask.get(SLSConfiguration.TASK_PREFIX + info.getName()));
        res.setResourceValue(info.getName(), value);
      }
    }
    return res;
  }

  public static final class Builder extends AmDefinitionBuilder {
    private final Map<String, String> jsonJob;

    private Builder(Map<String, String> jsonJob) {
      this.jsonJob = jsonJob;
    }

    public static Builder create(Map<String, String> jsonJob) {
      return new Builder(jsonJob);
    }

    public Builder withAmType(String key) {
      if (jsonJob.containsKey(key)) {
        String amType = jsonJob.get(key);
        if (amType != null) {
          this.amType = amType;
        }
      }
      return this;
    }

    public Builder withUser(String key) {
      if (jsonJob.containsKey(key)) {
        String user = jsonJob.get(key);
        if (user != null) {
          this.user = user;
        }
      }
      return this;
    }

    public Builder withQueue(String key) {
      if (jsonJob.containsKey(key)) {
        this.queue = jsonJob.get(key);
      }
      return this;
    }

    public Builder withJobId(String key) {
      if (jsonJob.containsKey(key)) {
        this.jobId = jsonJob.get(key);
      }
      return this;
    }

    public Builder withJobCount(String key) {
      if (jsonJob.containsKey(key)) {
        jobCount = Integer.parseInt(jsonJob.get(key));
        jobCount = Math.max(jobCount, 1);
      }
      return this;
    }

    public Builder withJobStartTime(String key) {
      if (jsonJob.containsKey(key)) {
        this.jobStartTime = Long.parseLong(jsonJob.get(key));
      }
      return this;
    }

    public Builder withJobFinishTime(String key) {
      if (jsonJob.containsKey(key)) {
        this.jobFinishTime = Long.parseLong(jsonJob.get(key));
      }
      return this;
    }

    public Builder withLabelExpression(String key) {
      if (jsonJob.containsKey(key)) {
        this.labelExpression = jsonJob.get(key);
      }
      return this;
    }

    public AMDefinitionSLS.Builder withTaskContainers(
        List<ContainerSimulator> taskContainers) {
      this.taskContainers = taskContainers;
      return this;
    }

    public AMDefinitionSLS.Builder withAmResource(Resource amResource) {
      this.amResource = amResource;
      return this;
    }

    public AMDefinitionSLS build() {
      AMDefinitionSLS amDef = new AMDefinitionSLS(this);
      // Job id is generated automatically if this job configuration allows
      // multiple job instances
      if (jobCount > 1) {
        amDef.oldAppId = null;
      } else {
        amDef.oldAppId = jobId;
      }
      amDef.jobCount = jobCount;
      return amDef;
    }
  }

}
