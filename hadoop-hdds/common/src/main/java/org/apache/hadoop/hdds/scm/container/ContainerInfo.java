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
package org.apache.hadoop.hdds.scm.container;

import static java.lang.Math.max;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.util.Time;

/**
 * Class wraps ozone container info.
 */
public class ContainerInfo implements Comparator<ContainerInfo>,
    Comparable<ContainerInfo>, Externalizable {

  private static final ObjectWriter WRITER;
  private static final String SERIALIZATION_ERROR_MSG = "Java serialization not"
      + " supported. Use protobuf instead.";

  static {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper
        .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
    WRITER = mapper.writer();
  }

  private HddsProtos.LifeCycleState state;
  @JsonIgnore
  private PipelineID pipelineID;
  private ReplicationFactor replicationFactor;
  private ReplicationType replicationType;
  private long usedBytes;
  private long numberOfKeys;
  private long lastUsed;
  // The wall-clock ms since the epoch at which the current state enters.
  private long stateEnterTime;
  private String owner;
  private long containerID;
  private long deleteTransactionId;
  // The sequenceId of a close container cannot change, and all the
  // container replica should have the same sequenceId.
  private long sequenceId;

  /**
   * Allows you to maintain private data on ContainerInfo. This is not
   * serialized via protobuf, just allows us to maintain some private data.
   */
  @JsonIgnore
  private byte[] data;

  @SuppressWarnings("parameternumber")
  ContainerInfo(
      long containerID,
      HddsProtos.LifeCycleState state,
      PipelineID pipelineID,
      long usedBytes,
      long numberOfKeys,
      long stateEnterTime,
      String owner,
      long deleteTransactionId,
      long sequenceId,
      ReplicationFactor replicationFactor,
      ReplicationType repType) {
    this.containerID = containerID;
    this.pipelineID = pipelineID;
    this.usedBytes = usedBytes;
    this.numberOfKeys = numberOfKeys;
    this.lastUsed = Time.monotonicNow();
    this.state = state;
    this.stateEnterTime = stateEnterTime;
    this.owner = owner;
    this.deleteTransactionId = deleteTransactionId;
    this.sequenceId = sequenceId;
    this.replicationFactor = replicationFactor;
    this.replicationType = repType;
  }

  /**
   * Needed for serialization findbugs.
   */
  public ContainerInfo() {
  }

  public static ContainerInfo fromProtobuf(HddsProtos.ContainerInfoProto info) {
    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    return builder.setPipelineID(
        PipelineID.getFromProtobuf(info.getPipelineID()))
        .setUsedBytes(info.getUsedBytes())
        .setNumberOfKeys(info.getNumberOfKeys())
        .setState(info.getState())
        .setStateEnterTime(info.getStateEnterTime())
        .setOwner(info.getOwner())
        .setContainerID(info.getContainerID())
        .setDeleteTransactionId(info.getDeleteTransactionId())
        .setReplicationFactor(info.getReplicationFactor())
        .setReplicationType(info.getReplicationType())
        .build();
  }

  public long getContainerID() {
    return containerID;
  }

  public HddsProtos.LifeCycleState getState() {
    return state;
  }

  public void setState(HddsProtos.LifeCycleState state) {
    this.state = state;
  }

  public long getStateEnterTime() {
    return stateEnterTime;
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public void setUsedBytes(long value) {
    usedBytes = value;
  }

  public long getNumberOfKeys() {
    return numberOfKeys;
  }

  public void setNumberOfKeys(long value) {
    numberOfKeys = value;
  }

  public long getDeleteTransactionId() {
    return deleteTransactionId;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public void updateDeleteTransactionId(long transactionId) {
    deleteTransactionId = max(transactionId, deleteTransactionId);
  }

  public void updateSequenceId(long sequenceID) {
    assert (isOpen() || state == HddsProtos.LifeCycleState.QUASI_CLOSED);
    sequenceId = max(sequenceID, sequenceId);
  }

  public ContainerID containerID() {
    return new ContainerID(getContainerID());
  }

  /**
   * Gets the last used time from SCM's perspective.
   *
   * @return time in milliseconds.
   */
  public long getLastUsed() {
    return lastUsed;
  }

  public ReplicationType getReplicationType() {
    return replicationType;
  }

  public void updateLastUsedTime() {
    lastUsed = Time.monotonicNow();
  }

  public HddsProtos.ContainerInfoProto getProtobuf() {
    HddsProtos.ContainerInfoProto.Builder builder =
        HddsProtos.ContainerInfoProto.newBuilder();
    Preconditions.checkState(containerID > 0);
    return builder.setContainerID(getContainerID())
        .setUsedBytes(getUsedBytes())
        .setNumberOfKeys(getNumberOfKeys()).setState(getState())
        .setStateEnterTime(getStateEnterTime()).setContainerID(getContainerID())
        .setDeleteTransactionId(getDeleteTransactionId())
        .setPipelineID(getPipelineID().getProtobuf())
        .setReplicationFactor(getReplicationFactor())
        .setReplicationType(getReplicationType())
        .setOwner(getOwner())
        .build();
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  @Override
  public String toString() {
    return "ContainerInfo{"
        + "id=" + containerID
        + ", state=" + state
        + ", pipelineID=" + pipelineID
        + ", stateEnterTime=" + stateEnterTime
        + ", owner=" + owner
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerInfo that = (ContainerInfo) o;

    return new EqualsBuilder()
        .append(getContainerID(), that.getContainerID())

        // TODO : Fix this later. If we add these factors some tests fail.
        // So Commenting this to continue and will enforce this with
        // Changes in pipeline where we remove Container Name to
        // SCMContainerinfo from Pipeline.
        // .append(pipeline.getFactor(), that.pipeline.getFactor())
        // .append(pipeline.getType(), that.pipeline.getType())
        .append(owner, that.owner)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(11, 811)
        .append(getContainerID())
        .append(getOwner())
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
  public int compare(ContainerInfo o1, ContainerInfo o2) {
    return Long.compare(o1.getLastUsed(), o2.getLastUsed());
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
  public int compareTo(ContainerInfo o) {
    return this.compare(this, o);
  }

  /**
   * Returns a JSON string of this object.
   *
   * @return String - json string
   * @throws IOException
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * Returns private data that is set on this containerInfo.
   *
   * @return blob, the user can interpret it any way they like.
   */
  public byte[] getData() {
    if (this.data != null) {
      return Arrays.copyOf(this.data, this.data.length);
    } else {
      return null;
    }
  }

  /**
   * Set private data on ContainerInfo object.
   *
   * @param data -- private data.
   */
  public void setData(byte[] data) {
    if (data != null) {
      this.data = Arrays.copyOf(data, data.length);
    }
  }

  /**
   * Throws IOException as default java serialization is not supported. Use
   * serialization via protobuf instead.
   *
   * @param out the stream to write the object to
   * @throws IOException Includes any I/O exceptions that may occur
   * @serialData Overriding methods should use this tag to describe
   * the data layout of this Externalizable object.
   * List the sequence of element types and, if possible,
   * relate the element to a public/protected field and/or
   * method of this Externalizable class.
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    throw new IOException(SERIALIZATION_ERROR_MSG);
  }

  /**
   * Throws IOException as default java serialization is not supported. Use
   * serialization via protobuf instead.
   *
   * @param in the stream to read data from in order to restore the object
   * @throws IOException            if I/O errors occur
   * @throws ClassNotFoundException If the class for an object being
   *                                restored cannot be found.
   */
  @Override
  public void readExternal(ObjectInput in)
      throws IOException, ClassNotFoundException {
    throw new IOException(SERIALIZATION_ERROR_MSG);
  }

  /**
   * Builder class for ContainerInfo.
   */
  public static class Builder {
    private HddsProtos.LifeCycleState state;
    private long used;
    private long keys;
    private long stateEnterTime;
    private String owner;
    private long containerID;
    private long deleteTransactionId;
    private long sequenceId;
    private PipelineID pipelineID;
    private ReplicationFactor replicationFactor;
    private ReplicationType replicationType;

    public Builder setReplicationType(
        ReplicationType repType) {
      this.replicationType = repType;
      return this;
    }

    public Builder setPipelineID(PipelineID pipelineId) {
      this.pipelineID = pipelineId;
      return this;
    }

    public Builder setReplicationFactor(ReplicationFactor repFactor) {
      this.replicationFactor = repFactor;
      return this;
    }

    public Builder setContainerID(long id) {
      Preconditions.checkState(id >= 0);
      this.containerID = id;
      return this;
    }

    public Builder setState(HddsProtos.LifeCycleState lifeCycleState) {
      this.state = lifeCycleState;
      return this;
    }

    public Builder setUsedBytes(long bytesUsed) {
      this.used = bytesUsed;
      return this;
    }

    public Builder setNumberOfKeys(long keyCount) {
      this.keys = keyCount;
      return this;
    }

    public Builder setStateEnterTime(long time) {
      this.stateEnterTime = time;
      return this;
    }

    public Builder setOwner(String containerOwner) {
      this.owner = containerOwner;
      return this;
    }

    public Builder setDeleteTransactionId(long deleteTransactionID) {
      this.deleteTransactionId = deleteTransactionID;
      return this;
    }

    public Builder setSequenceId(long sequenceID) {
      this.sequenceId = sequenceID;
      return this;
    }

    public ContainerInfo build() {
      return new ContainerInfo(containerID, state, pipelineID,
          used, keys, stateEnterTime, owner, deleteTransactionId,
          sequenceId, replicationFactor, replicationType);
    }
  }

  /**
   * Check if a container is in open state, this will check if the
   * container is either open or closing state. Any containers in these states
   * is managed as an open container by SCM.
   */
  public boolean isOpen() {
    return state == HddsProtos.LifeCycleState.OPEN
        || state == HddsProtos.LifeCycleState.CLOSING;
  }
}
