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
package org.apache.hadoop.hdfs.protocol;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;

import java.io.Serializable;

/**
 * A policy about how to write/read/code an erasure coding file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ErasureCodingPolicy implements Serializable {

  private static final long serialVersionUID = 0x0079fe4e;

  private String name;
  private final ECSchema schema;
  private final int cellSize;
  private byte id;
  private ErasureCodingPolicyState state;


  public ErasureCodingPolicy(String name, ECSchema schema,
      int cellSize, byte id, ErasureCodingPolicyState state) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(cellSize > 0, "cellSize must be positive");
    Preconditions.checkArgument(cellSize % 1024 == 0,
        "cellSize must be 1024 aligned");
    this.name = name;
    this.schema = schema;
    this.cellSize = cellSize;
    this.id = id;
    this.state = state;
  }

  public ErasureCodingPolicy(String name, ECSchema schema, int cellSize,
      byte id) {
    this(name, schema, cellSize, id, ErasureCodingPolicyState.DISABLED);
  }

  public ErasureCodingPolicy(ECSchema schema, int cellSize, byte id) {
    this(composePolicyName(schema, cellSize), schema, cellSize, id,
        ErasureCodingPolicyState.DISABLED);
  }

  public ErasureCodingPolicy(ECSchema schema, int cellSize) {
    this(composePolicyName(schema, cellSize), schema, cellSize, (byte) -1,
        ErasureCodingPolicyState.DISABLED);
  }

  public static String composePolicyName(ECSchema schema, int cellSize) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(cellSize > 0, "cellSize must be positive");
    Preconditions.checkArgument(cellSize % 1024 == 0,
        "cellSize must be 1024 aligned");
    return schema.getCodecName().toUpperCase() + "-" +
        schema.getNumDataUnits() + "-" + schema.getNumParityUnits() +
        "-" + cellSize / 1024 + "k";
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ECSchema getSchema() {
    return schema;
  }

  public int getCellSize() {
    return cellSize;
  }

  public int getNumDataUnits() {
    return schema.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return schema.getNumParityUnits();
  }

  public String getCodecName() {
    return schema.getCodecName();
  }

  public byte getId() {
    return id;
  }

  public void setId(byte id) {
    this.id = id;
  }


  public boolean isReplicationPolicy() {
    return (id == ErasureCodeConstants.REPLICATION_POLICY_ID);
  }

  public ErasureCodingPolicyState getState() {
    return state;
  }

  public void setState(ErasureCodingPolicyState state) {
    this.state = state;
  }

  public boolean isSystemPolicy() {
    return (this.id < ErasureCodeConstants.USER_DEFINED_POLICY_START_ID);
  }

  public boolean isEnabled() {
    return (this.state == ErasureCodingPolicyState.ENABLED);
  }

  public boolean isDisabled() {
    return (this.state == ErasureCodingPolicyState.DISABLED);
  }

  public boolean isRemoved() {
    return (this.state == ErasureCodingPolicyState.REMOVED);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o.getClass() != getClass()) {
      return false;
    }
    ErasureCodingPolicy rhs = (ErasureCodingPolicy) o;
    return new EqualsBuilder()
        .append(name, rhs.name)
        .append(schema, rhs.schema)
        .append(cellSize, rhs.cellSize)
        .append(id, rhs.id)
        .append(state, rhs.state)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(303855623, 582626729)
        .append(name)
        .append(schema)
        .append(cellSize)
        .append(id)
        .append(state)
        .toHashCode();
  }

  @Override
  public String toString() {
    return "ErasureCodingPolicy=[" + "Name=" + name + ", "
        + "Schema=[" + schema.toString() + "], "
        + "CellSize=" + cellSize + ", "
        + "Id=" + id + ", "
        + "State=" + state.toString()
        + "]";
  }
}
