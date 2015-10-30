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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class NodeLabel implements Comparable<NodeLabel> {

  /**
   * Default node label partition used for displaying.
   */
  @Private
  @Unstable
  public static final String DEFAULT_NODE_LABEL_PARTITION = "<DEFAULT_PARTITION>";

  /**
   * Node Label expression not set .
   */
  @Private
  @Unstable
  public static final String NODE_LABEL_EXPRESSION_NOT_SET = "<Not set>";

  /**
   * By default, node label is exclusive or not
   */
  @Private
  @Unstable
  public static final boolean DEFAULT_NODE_LABEL_EXCLUSIVITY = true;

  @Private
  @Unstable
  public static NodeLabel newInstance(String name) {
    return newInstance(name, DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }

  @Private
  @Unstable
  public static NodeLabel newInstance(String name, boolean isExclusive) {
    NodeLabel request = Records.newRecord(NodeLabel.class);
    request.setName(name);
    request.setExclusivity(isExclusive);
    return request;
  }

  @Public
  @Stable
  public abstract String getName();

  @Private
  @Unstable
  public abstract void setName(String name);

  @Public
  @Stable
  public abstract boolean isExclusive();

  @Private
  @Unstable
  public abstract void setExclusivity(boolean isExclusive);

  @Override
  public int compareTo(NodeLabel other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeLabel) {
      NodeLabel nl = (NodeLabel) obj;
      return nl.getName().equals(getName())
          && nl.isExclusive() == isExclusive();
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<");
    sb.append(getName());
    sb.append(":exclusivity=");
    sb.append(isExclusive());
    sb.append(">");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return (getName().hashCode() << 16) + (isExclusive() ? 1 : 0);
  }
}
