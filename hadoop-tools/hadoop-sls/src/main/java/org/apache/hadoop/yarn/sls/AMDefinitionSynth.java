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

import static org.apache.hadoop.yarn.sls.AMDefinitionFactory.adjustTimeValuesToBaselineTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;

public class AMDefinitionSynth extends AMDefinition {
  public AMDefinitionSynth(AmDefinitionBuilder builder) {
    super(builder);
  }

  public static List<ContainerSimulator> getTaskContainers(
      SynthJob job, SLSRunner slsRunner) throws YarnException {
    List<ContainerSimulator> containerList = new ArrayList<>();
    ArrayList<NodeId> keyAsArray = new ArrayList<>(
        slsRunner.getNmMap().keySet());
    Random rand = new Random(slsRunner.getStjp().getSeed());

    for (SynthJob.SynthTask task : job.getTasks()) {
      RMNode node = getRandomNode(slsRunner, keyAsArray, rand);
      TaskContainerDefinition containerDef =
          TaskContainerDefinition.Builder.create()
              .withCount(1)
              .withHostname("/" + node.getRackName() + "/" + node.getHostName())
              .withDuration(task.getTime())
              .withResource(Resource
                  .newInstance((int) task.getMemory(), (int) task.getVcores()))
              .withPriority(task.getPriority())
              .withType(task.getType())
              .withExecutionType(task.getExecutionType())
              .withAllocationId(-1)
              .withRequestDelay(0)
              .build();
      containerList.add(
          ContainerSimulator.createFromTaskContainerDefinition(containerDef));
    }

    return containerList;
  }

  private static RMNode getRandomNode(SLSRunner slsRunner,
      ArrayList<NodeId> keyAsArray, Random rand) {
    int randomIndex = rand.nextInt(keyAsArray.size());
    return slsRunner.getNmMap().get(keyAsArray.get(randomIndex)).getNode();
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

    public Builder withUser(String user) {
      if (user != null) {
        this.user = user;
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

    public AMDefinitionSynth.Builder withLabelExpression(String expr) {
      this.labelExpression = expr;
      return this;
    }

    public AMDefinitionSynth.Builder withTaskContainers(
        List<ContainerSimulator> taskContainers) {
      this.taskContainers = taskContainers;
      return this;
    }

    public AMDefinitionSynth.Builder withAmResource(Resource amResource) {
      this.amResource = amResource;
      return this;
    }

    public AMDefinitionSynth build() {
      AMDefinitionSynth amDef = new AMDefinitionSynth(this);

      if (baselineTimeMs == 0) {
        baselineTimeMs = jobStartTime;
      }
      adjustTimeValuesToBaselineTime(amDef, this, baselineTimeMs);
      return amDef;
    }
  }

}
