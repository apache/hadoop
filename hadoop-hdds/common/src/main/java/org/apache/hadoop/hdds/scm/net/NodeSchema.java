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
package org.apache.hadoop.hdds.scm.net;

import org.apache.hadoop.HadoopIllegalArgumentException;

import java.util.List;

/**
 * Network topology schema to housekeeper relevant information.
 */
public final class NodeSchema {
  /**
   * Network topology layer type enum definition.
   */
  public enum LayerType{
    ROOT("Root", NetConstants.INNER_NODE_COST_DEFAULT),
    INNER_NODE("InnerNode", NetConstants.INNER_NODE_COST_DEFAULT),
    LEAF_NODE("Leaf", NetConstants.NODE_COST_DEFAULT);

    private final String description;
    // default cost
    private final int cost;

    LayerType(String description, int cost) {
      this.description = description;
      this.cost = cost;
    }

    @Override
    public String toString() {
      return description;
    }

    public int getCost(){
      return cost;
    }
    public static LayerType getType(String typeStr) {
      for (LayerType type: LayerType.values()) {
        if (typeStr.equalsIgnoreCase(type.toString())) {
          return type;
        }
      }
      return null;
    }
  }

  // default cost
  private int cost;
  // layer Type, mandatory property
  private LayerType type;
  // default name, can be null or ""
  private String defaultName;
  // layer prefix, can be null or ""
  private String prefix;
  // sublayer
  private List<NodeSchema> sublayer;

  /**
   * Builder for NodeSchema.
   */
  public static class Builder {
    private int cost = -1;
    private LayerType type;
    private String defaultName;
    private String prefix;

    public Builder setCost(int nodeCost) {
      this.cost = nodeCost;
      return this;
    }

    public Builder setPrefix(String nodePrefix) {
      this.prefix = nodePrefix;
      return this;
    }

    public Builder setType(LayerType nodeType) {
      this.type = nodeType;
      return this;
    }

    public Builder setDefaultName(String nodeDefaultName) {
      this.defaultName = nodeDefaultName;
      return this;
    }

    public NodeSchema build() {
      if (type == null) {
        throw new HadoopIllegalArgumentException("Type is mandatory for a " +
            "network topology node layer definition");
      }
      if (cost == -1) {
        cost = type.getCost();
      }
      return new NodeSchema(type, cost, prefix, defaultName);
    }
  }

  /**
   * Constructor.
   * @param type layer type
   * @param cost layer's default cost
   * @param prefix layer's prefix
   * @param defaultName layer's default name is if specified
   */
  public NodeSchema(LayerType type, int cost, String prefix,
      String defaultName) {
    this.type = type;
    this.cost = cost;
    this.prefix = prefix;
    this.defaultName = defaultName;
  }

  /**
   * Constructor. This constructor is only used when build NodeSchema from
   * YAML file.
   */
  public NodeSchema() {
    this.type = LayerType.INNER_NODE;
  }

  public boolean matchPrefix(String name) {
    if (name == null || name.isEmpty() || prefix == null || prefix.isEmpty()) {
      return false;
    }
    return name.trim().toLowerCase().startsWith(prefix.toLowerCase());
  }

  public LayerType getType() {
    return this.type;
  }

  public void setType(LayerType type) {
    this.type = type;
  }

  public String getPrefix() {
    return this.prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getDefaultName() {
    return this.defaultName;
  }

  public void setDefaultName(String name) {
    this.defaultName = name;
  }

  public int getCost() {
    return this.cost;
  }
  public void setCost(int cost) {
    this.cost = cost;
  }

  public void setSublayer(List<NodeSchema> sublayer) {
    this.sublayer = sublayer;
  }

  public List<NodeSchema> getSublayer() {
    return sublayer;
  }
}