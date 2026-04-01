/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer.planner;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.test.SampleStep;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNodePlan {

  @Test
  public void testNodePlan() throws IOException {
    NodePlan nodePlan = new NodePlan("datanode1234", 1234);
    MoveStep moveStep = new MoveStep();
    moveStep.setBandwidth(12345);
    moveStep.setBytesToMove(98765);
    moveStep.setIdealStorage(1.234);
    moveStep.setMaxDiskErrors(4567);
    moveStep.setVolumeSetID("id1234");
    nodePlan.addStep(moveStep);
    String json = nodePlan.toJson();
    assertThat(NodePlan.parseJson(json)).isNotNull();
  }

  @Test
  public void testNodePlanWithDisallowedStep() throws Exception {
    NodePlan nodePlan = new NodePlan("datanode1234", 1234);
    Step sampleStep = new SampleStep();
    sampleStep.setBandwidth(12345);
    sampleStep.setMaxDiskErrors(4567);
    nodePlan.addStep(sampleStep);
    assertNodePlanInvalid(nodePlan);
  }

  @Test
  public void testNodePlanWithSecondStepDisallowed() throws Exception {
    NodePlan nodePlan = new NodePlan("datanode1234", 1234);
    MoveStep moveStep = new MoveStep();
    moveStep.setBandwidth(12345);
    moveStep.setBytesToMove(98765);
    moveStep.setIdealStorage(1.234);
    moveStep.setMaxDiskErrors(4567);
    moveStep.setVolumeSetID("id1234");
    nodePlan.addStep(moveStep);
    Step sampleStep = new SampleStep();
    sampleStep.setBandwidth(12345);
    sampleStep.setMaxDiskErrors(4567);
    nodePlan.addStep(sampleStep);
    assertNodePlanInvalid(nodePlan);
  }

  @Test
  public void testNodePlanWithNestedDisallowedStep() throws Exception {
    NodePlan nodePlan = new NodePlan("datanode1234", 1234);
    NodePlan nodePlan2 = new NodePlan("datanode9876", 9876);
    SampleStep sampleStep = new SampleStep();
    sampleStep.setBandwidth(12345);
    sampleStep.setMaxDiskErrors(4567);
    nodePlan2.addStep(sampleStep);
    NestedStep nestedStep = new NestedStep(nodePlan2);
    nestedStep.setBandwidth(1234);
    nestedStep.setMaxDiskErrors(456);
    nodePlan.addStep(nestedStep);
    assertNodePlanInvalid(nodePlan);
  }

  private void assertNodePlanInvalid(final NodePlan nodePlan) throws Exception {
    LambdaTestUtils.intercept(
        IOException.class,
        "Invalid @class value in NodePlan JSON",
        () -> NodePlan.parseJson(nodePlan.toJson()));
  }

  private static class NestedStep implements Step {
    @JsonProperty
    private NodePlan nodePlan;

    NestedStep() {
      // needed to make Jackson deserialization easier
    }

    NestedStep(NodePlan nodePlan) {
      this.nodePlan = nodePlan;
    }

    NodePlan getNodePlan() {
      return nodePlan;
    }

    @Override
    public long getBytesToMove() {
      return 0;
    }

    @Override
    public DiskBalancerVolume getDestinationVolume() {
      return null;
    }

    @Override
    public double getIdealStorage() {
      return 0;
    }

    @Override
    public DiskBalancerVolume getSourceVolume() {
      return null;
    }

    @Override
    public String getVolumeSetID() {
      return "";
    }

    @Override
    public String getSizeString(long size) {
      return "";
    }

    @Override
    public long getMaxDiskErrors() {
      return 0;
    }

    @Override
    public long getTolerancePercent() {
      return 0;
    }

    @Override
    public long getBandwidth() {
      return 0;
    }

    @Override
    public void setTolerancePercent(long tolerancePercent) {

    }

    @Override
    public void setBandwidth(long bandwidth) {

    }

    @Override
    public void setMaxDiskErrors(long maxDiskErrors) {

    }
  }
}
