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
 * Node Attribute is a kind of a label which represents one of the
 * attribute/feature of a Node. Its different from node partition label as
 * resource guarantees across the queues will not be maintained for these type
 * of labels.
 * </p>
 * <p>
 * A given Node can be mapped with any kind of attribute, few examples are
 * HAS_SSD=true, JAVA_VERSION=JDK1.8, OS_TYPE=WINDOWS.
 * </p>
 * <p>
 * Its not compulsory for all the attributes to have value, empty string is the
 * default value of the <code>NodeAttributeType.STRING</code>
 * </p>
 * <p>
 * Node Attribute Prefix is used as namespace to segregate the attributes.
 * </p>
 */
@Public
@Unstable
public abstract class NodeAttribute {

  public static final String PREFIX_DISTRIBUTED = "nm.yarn.io";
  public static final String PREFIX_CENTRALIZED = "rm.yarn.io";

  public static NodeAttribute newInstance(String attributeName,
      NodeAttributeType attributeType, String attributeValue) {
    return newInstance(PREFIX_CENTRALIZED, attributeName, attributeType,
        attributeValue);
  }

  public static NodeAttribute newInstance(String attributePrefix,
      String attributeName, NodeAttributeType attributeType,
      String attributeValue) {
    NodeAttribute nodeAttribute = Records.newRecord(NodeAttribute.class);
    NodeAttributeKey nodeAttributeKey =
        NodeAttributeKey.newInstance(attributePrefix, attributeName);
    nodeAttribute.setAttributeKey(nodeAttributeKey);
    nodeAttribute.setAttributeType(attributeType);
    nodeAttribute.setAttributeValue(attributeValue);
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
  public abstract String getAttributeValue();

  @Public
  @Unstable
  public abstract void setAttributeValue(String attributeValue);

  @Public
  @Unstable
  public abstract NodeAttributeType getAttributeType();

  @Public
  @Unstable
  public abstract void setAttributeType(NodeAttributeType attributeType);
}
