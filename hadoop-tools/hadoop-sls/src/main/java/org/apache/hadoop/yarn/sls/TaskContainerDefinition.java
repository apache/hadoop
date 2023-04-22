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

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import java.util.Map;

import static org.apache.hadoop.yarn.sls.AMDefinitionRumen.DEFAULT_MAPPER_PRIORITY;

public class TaskContainerDefinition {
  private long duration;
  private Resource resource;
  private int priority;
  private String type;
  private int count;
  private ExecutionType executionType;
  private long allocationId = -1;
  private long requestDelay = 0;
  private String hostname;

  public long getDuration() {
    return duration;
  }

  public Resource getResource() {
    return resource;
  }

  public int getPriority() {
    return priority;
  }

  public String getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

  public ExecutionType getExecutionType() {
    return executionType;
  }

  public long getAllocationId() {
    return allocationId;
  }

  public long getRequestDelay() {
    return requestDelay;
  }

  public String getHostname() {
    return hostname;
  }

  public static final class Builder {
    private long duration = -1;
    private long durationLegacy = -1;
    private long taskStart = -1;
    private long taskFinish = -1;
    private Resource resource;
    private int priority = DEFAULT_MAPPER_PRIORITY;
    private String type = "map";
    private int count = 1;
    private ExecutionType executionType = ExecutionType.GUARANTEED;
    private long allocationId = -1;
    private long requestDelay = 0;
    private String hostname;

    public static Builder create() {
      return new Builder();
    }

    public Builder withDuration(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.duration = Integer.parseInt(jsonTask.get(key));
      }
      return this;
    }

    public Builder withDuration(long duration) {
      this.duration = duration;
      return this;
    }

    /**
     * Also support "duration.ms" for backward compatibility.
     * @param jsonTask the json representation of the task.
     * @param key The json key.
     * @return the builder
     */
    public Builder withDurationLegacy(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.durationLegacy = Integer.parseInt(jsonTask.get(key));
      }
      return this;
    }

    public Builder withTaskStart(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.taskStart = Long.parseLong(jsonTask.get(key));
      }
      return this;
    }

    public Builder withTaskFinish(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.taskFinish = Long.parseLong(jsonTask.get(key));
      }
      return this;
    }

    public Builder withResource(Resource resource) {
      this.resource = resource;
      return this;
    }

    public Builder withPriority(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.priority = Integer.parseInt(jsonTask.get(key));
      }
      return this;
    }

    public Builder withPriority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder withType(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.type = jsonTask.get(key);
      }
      return this;
    }

    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    public Builder withCount(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        count = Integer.parseInt(jsonTask.get(key));
        count = Math.max(count, 1);
      }
      return this;
    }

    public Builder withCount(int count) {
      this.count = count;
      return this;
    }

    public Builder withExecutionType(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.executionType = ExecutionType.valueOf(jsonTask.get(key));
      }
      return this;
    }

    public Builder withExecutionType(ExecutionType executionType) {
      this.executionType = executionType;
      return this;
    }

    public Builder withAllocationId(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        this.allocationId = Long.parseLong(jsonTask.get(key));
      }
      return this;
    }

    public Builder withAllocationId(long allocationId) {
      this.allocationId = allocationId;
      return this;
    }

    public Builder withRequestDelay(Map<String, String> jsonTask, String key) {
      if (jsonTask.containsKey(key)) {
        requestDelay = Long.parseLong(jsonTask.get(key));
        requestDelay = Math.max(requestDelay, 0);
      }
      return this;
    }

    public Builder withRequestDelay(long requestDelay) {
      this.requestDelay = requestDelay;
      return this;
    }

    public Builder withHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public TaskContainerDefinition build() throws YarnException {
      TaskContainerDefinition taskContainerDef =
          new TaskContainerDefinition();
      taskContainerDef.duration = validateAndGetDuration(this);
      taskContainerDef.resource = this.resource;
      taskContainerDef.type = this.type;
      taskContainerDef.requestDelay = this.requestDelay;
      taskContainerDef.priority = this.priority;
      taskContainerDef.count = this.count;
      taskContainerDef.allocationId = this.allocationId;
      taskContainerDef.executionType = this.executionType;
      taskContainerDef.hostname = this.hostname;
      return taskContainerDef;
    }

    private long validateAndGetDuration(Builder builder) throws YarnException {
      long duration = 0;

      if (builder.duration != -1) {
        duration = builder.duration;
      } else if (builder.durationLegacy != -1) {
        duration = builder.durationLegacy;
      } else if (builder.taskStart != -1 && builder.taskFinish != -1) {
        duration = builder.taskFinish - builder.taskStart;
      }

      if (duration <= 0) {
        throw new YarnException("Duration of a task shouldn't be less or equal"
            + " to 0!");
      }
      return duration;
    }
  }
}
