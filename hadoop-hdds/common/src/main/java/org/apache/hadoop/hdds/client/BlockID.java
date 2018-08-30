/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.client;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

/**
 * BlockID of ozone (containerID  localID).
 */
public class BlockID {
  private long containerID;
  private long localID;

  public BlockID(long containerID, long localID) {
    this.containerID = containerID;
    this.localID = localID;
  }

  public long getContainerID() {
    return containerID;
  }

  public long getLocalID() {
    return localID;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).
        append("containerID", containerID).
        append("localID", localID).
        toString();
  }

  public HddsProtos.BlockID getProtobuf() {
    return HddsProtos.BlockID.newBuilder().
        setContainerID(containerID).setLocalID(localID).build();
  }

  public static BlockID getFromProtobuf(HddsProtos.BlockID blockID) {
    return new BlockID(blockID.getContainerID(),
        blockID.getLocalID());
  }

  public ContainerProtos.DatanodeBlockID getDatanodeBlockIDProtobuf() {
    return ContainerProtos.DatanodeBlockID.newBuilder().
        setContainerID(containerID).setLocalID(localID).build();
  }

  public static BlockID getFromProtobuf(
      ContainerProtos.DatanodeBlockID blockID) {
    return new BlockID(blockID.getContainerID(),
        blockID.getLocalID());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockID blockID = (BlockID) o;
    return containerID == blockID.containerID && localID == blockID.localID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerID, localID);
  }
}
