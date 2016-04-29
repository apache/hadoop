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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * A policy about how to write/read/code an erasure coding file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ErasureCodingPolicy {

  private final String name;
  private final ECSchema schema;
  private final int cellSize;
  private final byte id;

  public ErasureCodingPolicy(String name, ECSchema schema,
      int cellSize, byte id) {
    this.name = name;
    this.schema = schema;
    this.cellSize = cellSize;
    this.id = id;
  }

  public ErasureCodingPolicy(ECSchema schema, int cellSize, byte id) {
    this(composePolicyName(schema, cellSize), schema, cellSize, id);
  }

  private static String composePolicyName(ECSchema schema, int cellSize) {
    assert cellSize % 1024 == 0;
    return schema.getCodecName().toUpperCase() + "-" +
        schema.getNumDataUnits() + "-" + schema.getNumParityUnits() +
        "-" + cellSize / 1024 + "k";
  }

  public String getName() {
    return name;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErasureCodingPolicy that = (ErasureCodingPolicy) o;

    return that.getName().equals(name) &&
        that.getCellSize() == cellSize &&
        that.getSchema().equals(schema);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + schema.hashCode();
    result = 31 * result + cellSize;
    return result;
  }

  @Override
  public String toString() {
    return "ErasureCodingPolicy=[" + "Name=" + name + ", "
        + "Schema=[" + schema.toString() + "], "
        + "CellSize=" + cellSize + " " + "]";
  }
}
