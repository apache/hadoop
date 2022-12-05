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

package org.apache.hadoop.yarn.sls.scheduler;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.sls.TaskContainerDefinition;

@Private
@Unstable
public class ContainerSimulator implements Delayed {
  private ContainerId id;
  private Resource resource;
  private long endTime;
  // life time (ms)
  private long lifeTime;
  // time(ms) after which container would be requested by AM
  private long requestDelay;
  private String hostname;
  private int priority;
  private String type;
  private ExecutionType executionType = ExecutionType.GUARANTEED;
  private long allocationId;

  /**
   * Invoked when AM schedules containers to allocate.
   * @param def The task's definition object.
   * @return ContainerSimulator object
   */
  public static ContainerSimulator createFromTaskContainerDefinition(
      TaskContainerDefinition def) {
    return new ContainerSimulator(def.getResource(), def.getDuration(),
        def.getHostname(), def.getPriority(), def.getType(),
        def.getExecutionType(), def.getAllocationId(), def.getRequestDelay());
  }

  /**
   * Invoked when AM schedules containers to allocate.
   */
  @SuppressWarnings("checkstyle:parameternumber")
  private ContainerSimulator(Resource resource, long lifeTime,
      String hostname, int priority, String type, ExecutionType executionType,
      long allocationId, long requestDelay) {
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.hostname = hostname;
    this.priority = priority;
    this.type = type;
    this.executionType = executionType;
    this.requestDelay = requestDelay;
    this.allocationId = allocationId;
  }

  /**
   * Invoked when NM schedules containers to run.
   */
  public ContainerSimulator(ContainerId id, Resource resource, long endTime,
      long lifeTime, long allocationId) {
    this.id = id;
    this.resource = resource;
    this.endTime = endTime;
    this.lifeTime = lifeTime;
    this.allocationId = allocationId;
  }
  
  public Resource getResource() {
    return resource;
  }
  
  public ContainerId getId() {
    return id;
  }

  @Override
  public int compareTo(Delayed o) {
    if (!(o instanceof ContainerSimulator)) {
      throw new IllegalArgumentException(
              "Parameter must be a ContainerSimulator instance");
    }
    ContainerSimulator other = (ContainerSimulator) o;
    return (int) Math.signum(endTime - other.endTime);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(endTime - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
  }
  
  public long getLifeTime() {
    return lifeTime;
  }
  
  public String getHostname() {
    return hostname;
  }
  
  public long getEndTime() {
    return endTime;
  }
  
  public int getPriority() {
    return priority;
  }
  
  public String getType() {
    return type;
  }
  
  public void setPriority(int p) {
    priority = p;
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
}
