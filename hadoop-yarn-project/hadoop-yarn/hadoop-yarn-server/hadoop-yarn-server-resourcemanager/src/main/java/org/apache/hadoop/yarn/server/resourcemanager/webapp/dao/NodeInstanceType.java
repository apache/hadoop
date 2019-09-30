/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "nodeInstanceType")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInstanceType {

  public String getModelName() {
    return modelName;
  }

  public CustomResourceInfo getCapacity() {
    return capacity;
  }

  protected String modelName;
  protected CustomResourceInfo capacity;

  public NodeInstanceType() {}

  public NodeInstanceType(String name, Resource res) {
    this.capacity = new CustomResourceInfo(res);
    this.modelName = name;
  }


  /**
   * Find the smaller size instance type that can satisfy the resource.
   * The philosophy behind this design is the more instance,
   * the more opportunity to scale down and better cost-saving. And the bigger
   * instance, the more risk it will have orphan task that costs more.
   *
   * @param res The resource size to meet
   * @param allType The list of instance types
   * @param rc The resource calculator used
   * @return int array. First element is the index of instance type. Second is
   *         the minimumBuckets. If index is -1, it means no instance types can
   *         meet the resource size
   * */
  public static int[] getSuitableInstanceType(Resource res,
      List<NodeInstanceType> allType, ResourceCalculator rc) {
    // Assume it's better that a instance with resource approximate
    // to the requested container resource
    int[] ret = new int[2];
    int buckets = 0;
    int minimumBuckets = Integer.MAX_VALUE;
    int bestInstanceIndex = -1;
    for (int i = 0; i < allType.size(); i++) {
      buckets = (int)rc.computeAvailableContainers(allType.get(i)
          .getCapacity().getResource(), res);
      if (buckets > 0 && buckets < minimumBuckets) {
        minimumBuckets = buckets;
        bestInstanceIndex = i;
      }
    }
    ret[0] = bestInstanceIndex;
    ret[1] = minimumBuckets;
    return ret;
  }

  public String toStr(int count) {
    return "name:" + this.modelName + "; count:" + count;
  }

}
