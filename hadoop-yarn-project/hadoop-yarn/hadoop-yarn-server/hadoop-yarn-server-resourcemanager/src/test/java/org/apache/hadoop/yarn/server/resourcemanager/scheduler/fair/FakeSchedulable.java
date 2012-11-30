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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.util.Records;

/**
 * Dummy implementation of Schedulable for unit testing.
 */
public class FakeSchedulable extends Schedulable {
  private Resource demand;
  private Resource usage;
  private Resource minShare;
  private double weight;
  private Priority priority;
  private long startTime;
  
  public FakeSchedulable() {
    this(0, 0, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int demand) {
    this(demand, 0, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int demand, int minShare) {
    this(demand, minShare, 1, 0, 0, 0);
  }
  
  public FakeSchedulable(int demand, int minShare, double weight) {
    this(demand, minShare, weight, 0, 0, 0);
  }
  
  public FakeSchedulable(int demand, int minShare, double weight, int fairShare, int usage,
      long startTime) {
    this(Resources.createResource(demand), Resources.createResource(minShare), weight, 
        Resources.createResource(fairShare), Resources.createResource(usage), startTime);
  }
  
  public FakeSchedulable(Resource demand, Resource minShare, double weight, Resource fairShare,
      Resource usage, long startTime) {
    this.demand = demand;
    this.minShare = minShare;
    this.weight = weight;
    setFairShare(fairShare);
    this.usage = usage;
    this.priority = Records.newRecord(Priority.class);
    this.startTime = startTime;
  }
  
  @Override
  public Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    return null;
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public String getName() {
    return "FakeSchedulable" + this.hashCode();
  }

  @Override
  public Priority getPriority() {
    return priority;
  }

  @Override
  public Resource getResourceUsage() {
    return usage;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }
  
  @Override
  public double getWeight() {
    return weight;
  }
  
  @Override
  public Resource getMinShare() {
    return minShare;
  }

  @Override
  public void updateDemand() {}
}
