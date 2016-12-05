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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class GetClusterNodeLabelsResponse {
  /**
   * Creates a new instance.
   *
   * @param labels Node labels
   * @return response
   * @deprecated Use {@link #newInstance(List)} instead.
   */
  @Deprecated
  public static GetClusterNodeLabelsResponse newInstance(Set<String> labels) {
    List<NodeLabel> list = new ArrayList<>();
    for (String label : labels) {
      list.add(NodeLabel.newInstance(label));
    }
    return newInstance(list);
  }

  public static GetClusterNodeLabelsResponse newInstance(List<NodeLabel> labels) {
    GetClusterNodeLabelsResponse response =
        Records.newRecord(GetClusterNodeLabelsResponse.class);
    response.setNodeLabelList(labels);
    return response;
  }

  public abstract void setNodeLabelList(List<NodeLabel> labels);

  public abstract List<NodeLabel> getNodeLabelList();

  /**
   * Set node labels to the response.
   *
   * @param labels Node labels
   * @deprecated Use {@link #setNodeLabelList(List)} instead.
   */
  @Deprecated
  public abstract void setNodeLabels(Set<String> labels);

  /**
   * Get node labels of the response.
   *
   * @return Node labels
   * @deprecated Use {@link #getNodeLabelList()} instead.
   */
  @Deprecated
  public abstract Set<String> getNodeLabels();
}
