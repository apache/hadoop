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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

public class PartitionedQueueComparator implements Comparator<CSQueue> {
  private String partitionToLookAt = null;
  
  public void setPartitionToLookAt(String partitionToLookAt) {
    this.partitionToLookAt = partitionToLookAt;
  }
  

  @Override
  public int compare(CSQueue q1, CSQueue q2) {
    /*
     * 1. Check accessible to given partition, if one queue accessible and
     * the other not, accessible queue goes first.
     */
    boolean q1Accessible =
        q1.getAccessibleNodeLabels().contains(partitionToLookAt)
            || q1.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY);
    boolean q2Accessible =
        q2.getAccessibleNodeLabels().contains(partitionToLookAt)
            || q2.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY);
    if (q1Accessible && !q2Accessible) {
      return -1;
    } else if (!q1Accessible && q2Accessible) {
      return 1;
    }

    /*
     * 
     * 2. When two queue has same accessibility, check who will go first:
     * Now we simply compare their used resource on the partition to lookAt
     */
    float used1 = q1.getQueueCapacities().getUsedCapacity(partitionToLookAt);
    float used2 = q2.getQueueCapacities().getUsedCapacity(partitionToLookAt);
    if (Math.abs(used1 - used2) < 1e-6) {
      // When used capacity is same, compare their guaranteed-capacity
      float cap1 = q1.getQueueCapacities().getCapacity(partitionToLookAt);
      float cap2 = q2.getQueueCapacities().getCapacity(partitionToLookAt);
      
      // when cap1 == cap2, we will compare queue's name
      if (Math.abs(cap1 - cap2) < 1e-6) {
        return q1.getQueueName().compareTo(q2.getQueueName());
      }
      return Float.compare(cap2, cap1);
    }
    
    return Float.compare(used1, used2);
  }
}
