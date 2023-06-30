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

import org.apache.hadoop.yarn.util.Records;

import java.util.HashMap;
import java.util.Map;

public abstract class QueueSubClusterWeight {

  private String queue = "*";
  private Map<String, Double> amRmPolicyWeights = new HashMap<>();
  private Map<String, Double>  routerPolicyWeights = new HashMap<>();
  private Double headroomAlpha = 1.0;

  public static QueueSubClusterWeight newInstance(String queue,
      Map<String, Double> amRmPolicyWeights, Map<String, Double> routerPolicyWeights,
      Double headroomAlpha) {
    QueueSubClusterWeight nodeAttribute = Records.newRecord(QueueSubClusterWeight.class);
    nodeAttribute.setQueue(queue);
    nodeAttribute.setAmrmPolicyWeights(amRmPolicyWeights);
    nodeAttribute.setRouterPolicyWeights(routerPolicyWeights);
    nodeAttribute.setHeadroomAlpha(headroomAlpha);
    return nodeAttribute;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public void setAmrmPolicyWeights(Map<String, Double> pAmRmPolicyWeights) {
    this.amRmPolicyWeights = pAmRmPolicyWeights;
  }

  public void setRouterPolicyWeights(Map<String, Double> pRouterPolicyWeights) {
    this.routerPolicyWeights = pRouterPolicyWeights;
  }

  public void setHeadroomAlpha(Double headroomAlpha) {
    this.headroomAlpha = headroomAlpha;
  }
}
