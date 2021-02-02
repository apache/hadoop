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

package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;
import java.util.StringJoiner;

/**
 * {@code NodeHealthDetails} is a summary of the overall health score
 * of the node.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>
 *     In depth analysis of the health of the node. Even if the node is healthy
 *     it gives out a score based on node resources.
 *   </li>
 *   <li>
 *     Holds a map of information about the node resources.
 *     Example: SSD, HDD etc.
 *   </li>
 * </ul>
 *
 */
public abstract class NodeHealthDetails {

  @Private
  public static NodeHealthDetails newInstance(Integer overallScore,
      Map<String, Integer> nodeResourceScore) {
    NodeHealthDetails nodeHealthDetails = Records.newRecord(
        NodeHealthDetails.class);
    nodeHealthDetails.setOverallScore(overallScore);
    nodeHealthDetails.setNodeResourceScores(nodeResourceScore);
    return nodeHealthDetails;
  }

  @Private
  public static NodeHealthDetails newInstance(Integer overallScore) {
    NodeHealthDetails nodeHealthDetails = Records.newRecord(
        NodeHealthDetails.class);
    nodeHealthDetails.setOverallScore(overallScore);
    return nodeHealthDetails;
  }

  /**
   * Set the overall score of the node. This score is derived from node
   * resources score.
   * @param overallScore
   */
  @Private
  public abstract void setOverallScore(Integer overallScore);

  /**
   * Holds a Map of the resources and its scores.
   * @param nodeResourceScores
   */
  @Private
  public abstract void setNodeResourceScores(
      Map<String, Integer> nodeResourceScores);

  /**
   * @return the score of the node.
   */
  @Private
  public abstract Integer getOverallScore();

  /**
   * @return Scores of each resources in the node.
   */
  @Private
  public abstract Map<String, Integer> getNodeResourceScores();

  @Override
  public String toString() {
    StringJoiner healthDetailsString = new StringJoiner(",", "[", "]");
    healthDetailsString.add("Overall Score = " + this.getOverallScore());
    this.getNodeResourceScores().forEach((key, value) ->
        healthDetailsString.add(key + " = " + value));
    return healthDetailsString.toString();
  }

}
