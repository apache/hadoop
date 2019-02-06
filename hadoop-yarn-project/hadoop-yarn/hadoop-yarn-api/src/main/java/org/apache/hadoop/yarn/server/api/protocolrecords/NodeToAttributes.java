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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.util.Records;

/**
 * Represents a mapping of Node id to list of attributes.
 */
@Public
@Unstable
public abstract class NodeToAttributes {

  public static NodeToAttributes newInstance(String node,
      List<NodeAttribute> attributes) {
    NodeToAttributes nodeIdToAttributes =
        Records.newRecord(NodeToAttributes.class);
    nodeIdToAttributes.setNode(node);
    nodeIdToAttributes.setNodeAttributes(attributes);
    return nodeIdToAttributes;
  }

  @Public
  @Unstable
  public abstract String getNode();

  @Public
  @Unstable
  public abstract void setNode(String node);

  @Public
  @Unstable
  public abstract List<NodeAttribute> getNodeAttributes();

  @Public
  @Unstable
  public abstract void setNodeAttributes(List<NodeAttribute> attributes);
}
