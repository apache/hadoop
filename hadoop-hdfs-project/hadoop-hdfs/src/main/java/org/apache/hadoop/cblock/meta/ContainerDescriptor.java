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
package org.apache.hadoop.cblock.meta;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.cblock.protocol.proto.CBlockClientServerProtocolProtos;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

/**
 *
 * The internal representation of a container maintained by CBlock server.
 * Include enough information to exactly identify a container for read/write
 * operation.
 *
 * NOTE that this class is work-in-progress. Depends on HDFS-7240 container
 * implementation. Currently only to allow testing.
 */
public class ContainerDescriptor {
  private final String containerID;
  // the index of this container with in a volume
  // on creation, there may be no way to know the index of the container
  // as it is a volume specific information
  private int containerIndex;
  private Pipeline pipeline;

  public ContainerDescriptor(String containerID) {
    this.containerID = containerID;
  }

  public ContainerDescriptor(String containerID, int containerIndex) {
    this.containerID = containerID;
    this.containerIndex = containerIndex;
  }

  public void setContainerIndex(int idx) {
    this.containerIndex = idx;
  }

  public String getContainerID() {
    return containerID;
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public int getContainerIndex() {
    return containerIndex;
  }

  public long getUtilization() {
    return 0;
  }

  public CBlockClientServerProtocolProtos.ContainerIDProto toProtobuf() {
    CBlockClientServerProtocolProtos.ContainerIDProto.Builder builder =
        CBlockClientServerProtocolProtos.ContainerIDProto.newBuilder();
    builder.setContainerID(containerID);
    builder.setIndex(containerIndex);
    if (pipeline != null) {
      builder.setPipeline(pipeline.getProtobufMessage());
    }
    return builder.build();
  }

  public static ContainerDescriptor fromProtobuf(byte[] data)
      throws InvalidProtocolBufferException {
    CBlockClientServerProtocolProtos.ContainerIDProto id =
        CBlockClientServerProtocolProtos.ContainerIDProto.parseFrom(data);
    return new ContainerDescriptor(id.getContainerID(),
        (int)id.getIndex());
  }

  @Override
  public int hashCode() {
    return containerID.hashCode()*37 + containerIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (o != null && o instanceof ContainerDescriptor) {
      ContainerDescriptor other = (ContainerDescriptor)o;
      return containerID.equals(other.containerID) &&
          containerIndex == other.containerIndex;
    }
    return false;
  }
}
