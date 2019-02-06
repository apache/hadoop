/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.common.helpers;

import java.util.Comparator;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;

/**
 * Class wraps ozone container info.
 */
public class ContainerWithPipeline implements Comparator<ContainerWithPipeline>,
    Comparable<ContainerWithPipeline> {

  private final ContainerInfo containerInfo;
  private final Pipeline pipeline;

  public ContainerWithPipeline(ContainerInfo containerInfo, Pipeline pipeline) {
    this.containerInfo = containerInfo;
    this.pipeline = pipeline;
  }

  public ContainerInfo getContainerInfo() {
    return containerInfo;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public static ContainerWithPipeline fromProtobuf(
      HddsProtos.ContainerWithPipeline allocatedContainer)
      throws UnknownPipelineStateException {
    return new ContainerWithPipeline(
        ContainerInfo.fromProtobuf(allocatedContainer.getContainerInfo()),
        Pipeline.getFromProtobuf(allocatedContainer.getPipeline()));
  }

  public HddsProtos.ContainerWithPipeline getProtobuf()
      throws UnknownPipelineStateException {
    HddsProtos.ContainerWithPipeline.Builder builder =
        HddsProtos.ContainerWithPipeline.newBuilder();
    builder.setContainerInfo(getContainerInfo().getProtobuf())
        .setPipeline(getPipeline().getProtobufMessage());

    return builder.build();
  }


  @Override
  public String toString() {
    return containerInfo.toString() + " | " + pipeline.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerWithPipeline that = (ContainerWithPipeline) o;

    return new EqualsBuilder()
        .append(getContainerInfo(), that.getContainerInfo())
        .append(getPipeline(), that.getPipeline())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(11, 811)
        .append(getContainerInfo())
        .append(getPipeline())
        .toHashCode();
  }

  /**
   * Compares its two arguments for order.  Returns a negative integer, zero, or
   * a positive integer as the first argument is less than, equal to, or greater
   * than the second.<p>
   *
   * @param o1 the first object to be compared.
   * @param o2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer as the first
   * argument is less than, equal to, or greater than the second.
   * @throws NullPointerException if an argument is null and this comparator
   *                              does not permit null arguments
   * @throws ClassCastException   if the arguments' types prevent them from
   *                              being compared by this comparator.
   */
  @Override
  public int compare(ContainerWithPipeline o1, ContainerWithPipeline o2) {
    return o1.getContainerInfo().compareTo(o2.getContainerInfo());
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less than,
   * equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   * less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(ContainerWithPipeline o) {
    return this.compare(this, o);
  }

}
