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
package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * Node Attribute Info describes a NodeAttribute.
 * </p>
 */
@Public
@Unstable
public abstract class NodeAttributeInfo {

  public static NodeAttributeInfo newInstance(NodeAttribute nodeAttribute) {
    return newInstance(nodeAttribute.getAttributeKey(),
        nodeAttribute.getAttributeType());
  }

  public static NodeAttributeInfo newInstance(NodeAttributeKey nodeAttributeKey,
      NodeAttributeType attributeType) {
    NodeAttributeInfo nodeAttribute =
        Records.newRecord(NodeAttributeInfo.class);
    nodeAttribute.setAttributeKey(nodeAttributeKey);
    nodeAttribute.setAttributeType(attributeType);
    return nodeAttribute;
  }

  @Public
  @Unstable
  public abstract NodeAttributeKey getAttributeKey();

  @Public
  @Unstable
  public abstract void setAttributeKey(NodeAttributeKey attributeKey);

  @Public
  @Unstable
  public abstract NodeAttributeType getAttributeType();

  @Public
  @Unstable
  public abstract void setAttributeType(NodeAttributeType attributeType);
}
