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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

/**
 * BlockID of Ozone (containerID + localID + blockCommitSequenceId).
 */

public class BlockID {

  private ContainerBlockID containerBlockID;
  private long blockCommitSequenceId;

  public BlockID(long containerID, long localID) {
    this(containerID, localID, 0);
  }

  private BlockID(long containerID, long localID, long bcsID) {
    containerBlockID = new ContainerBlockID(containerID, localID);
    blockCommitSequenceId = bcsID;
  }

  public BlockID(ContainerBlockID containerBlockID) {
    this(containerBlockID, 0);
  }

  private BlockID(ContainerBlockID containerBlockID, long bcsId) {
    this.containerBlockID = containerBlockID;
    blockCommitSequenceId = bcsId;
  }

  public long getContainerID() {
    return containerBlockID.getContainerID();
  }

  public long getLocalID() {
    return containerBlockID.getLocalID();
  }

  public long getBlockCommitSequenceId() {
    return blockCommitSequenceId;
  }

  public void setBlockCommitSequenceId(long blockCommitSequenceId) {
    this.blockCommitSequenceId = blockCommitSequenceId;
  }

  public ContainerBlockID getContainerBlockID() {
    return containerBlockID;
  }

  public void setContainerBlockID(ContainerBlockID containerBlockID) {
    this.containerBlockID = containerBlockID;
  }

  @Override
  public String toString() {
    return new StringBuilder().append(getContainerBlockID().toString())
        .append(" bcsId: ")
        .append(blockCommitSequenceId)
        .toString();
  }

  public ContainerProtos.DatanodeBlockID getDatanodeBlockIDProtobuf() {
    return ContainerProtos.DatanodeBlockID.newBuilder().
        setContainerID(containerBlockID.getContainerID())
        .setLocalID(containerBlockID.getLocalID())
        .setBlockCommitSequenceId(blockCommitSequenceId).build();
  }

  public static BlockID getFromProtobuf(
      ContainerProtos.DatanodeBlockID blockID) {
    return new BlockID(blockID.getContainerID(),
        blockID.getLocalID(), blockID.getBlockCommitSequenceId());
  }

  public HddsProtos.BlockID getProtobuf() {
    return HddsProtos.BlockID.newBuilder()
        .setContainerBlockID(containerBlockID.getProtobuf())
        .setBlockCommitSequenceId(blockCommitSequenceId).build();
  }

  public static BlockID getFromProtobuf(HddsProtos.BlockID blockID) {
    return new BlockID(
        ContainerBlockID.getFromProtobuf(blockID.getContainerBlockID()),
        blockID.getBlockCommitSequenceId());
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
    return containerBlockID.equals(blockID.getContainerBlockID())
        && blockCommitSequenceId == blockID.getBlockCommitSequenceId();
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(containerBlockID.getContainerID(), containerBlockID.getLocalID(),
            blockCommitSequenceId);
  }
}
