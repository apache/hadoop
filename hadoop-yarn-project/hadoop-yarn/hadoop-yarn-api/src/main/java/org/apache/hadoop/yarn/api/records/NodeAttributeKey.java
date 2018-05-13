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
 * Node AttributeKey uniquely identifies a given Node Attribute. Node Attribute
 * is identified based on attribute prefix and name.
 * </p>
 * <p>
 * Node Attribute Prefix is used as namespace to segregate the attributes.
 * </p>
 */
@Public
@Unstable
public abstract class NodeAttributeKey {

  public static NodeAttributeKey newInstance(String attributeName) {
    return newInstance(NodeAttribute.PREFIX_CENTRALIZED, attributeName);
  }

  public static NodeAttributeKey newInstance(String attributePrefix,
      String attributeName) {
    NodeAttributeKey nodeAttributeKey =
        Records.newRecord(NodeAttributeKey.class);
    nodeAttributeKey.setAttributePrefix(attributePrefix);
    nodeAttributeKey.setAttributeName(attributeName);
    return nodeAttributeKey;
  }

  @Public
  @Unstable
  public abstract String getAttributePrefix();

  @Public
  @Unstable
  public abstract void setAttributePrefix(String attributePrefix);

  @Public
  @Unstable
  public abstract String getAttributeName();

  @Public
  @Unstable
  public abstract void setAttributeName(String attributeName);
}
