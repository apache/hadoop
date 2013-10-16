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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

class CSQueueUtils {
  
  final static float EPSILON = 0.0001f;
  
  public static void checkMaxCapacity(String queueName, 
      float capacity, float maximumCapacity) {
    if (maximumCapacity < 0.0f || maximumCapacity > 1.0f) {
      throw new IllegalArgumentException(
          "Illegal value  of maximumCapacity " + maximumCapacity + 
          " used in call to setMaxCapacity for queue " + queueName);
    }
    }

  public static void checkAbsoluteCapacities(String queueName,
      float absCapacity, float absMaxCapacity) {
    if (absMaxCapacity < (absCapacity - EPSILON)) {
      throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
          + "Queue '" + queueName + "' has " + "an absolute capacity (" + absCapacity
          + ") greater than " + "its absolute maximumCapacity (" + absMaxCapacity
          + ")");
  }
  }

  public static float computeAbsoluteMaximumCapacity(
      float maximumCapacity, CSQueue parent) {
    float parentAbsMaxCapacity = 
        (parent == null) ? 1.0f : parent.getAbsoluteMaximumCapacity();
    return (parentAbsMaxCapacity * maximumCapacity);
  }

  public static int computeMaxActiveApplications(
      ResourceCalculator calculator,
      Resource clusterResource, Resource minimumAllocation, 
      float maxAMResourcePercent, float absoluteMaxCapacity) {
    return
        Math.max(
            (int)Math.ceil(
                Resources.ratio(
                    calculator, 
                    clusterResource, 
                    minimumAllocation) * 
                    maxAMResourcePercent * absoluteMaxCapacity
                ), 
            1);
  }

  public static int computeMaxActiveApplicationsPerUser(
      int maxActiveApplications, int userLimit, float userLimitFactor) {
    return Math.max(
        (int)Math.ceil(
            maxActiveApplications * (userLimit / 100.0f) * userLimitFactor),
        1);
  }
  
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator calculator,
      final CSQueue childQueue, final CSQueue parentQueue, 
      final Resource clusterResource, final Resource minimumAllocation) {
    Resource queueLimit = Resources.none();
    Resource usedResources = childQueue.getUsedResources();
    
    float absoluteUsedCapacity = 0.0f;
    float usedCapacity = 0.0f;

    if (Resources.greaterThan(
        calculator, clusterResource, clusterResource, Resources.none())) {
      queueLimit = 
          Resources.multiply(clusterResource, childQueue.getAbsoluteCapacity());
      absoluteUsedCapacity = 
          Resources.divide(calculator, clusterResource, 
              usedResources, clusterResource);
      usedCapacity = 
          Resources.equals(queueLimit, Resources.none()) ? 0 :
          Resources.divide(calculator, clusterResource, 
              usedResources, queueLimit);
    }

    childQueue.setUsedCapacity(usedCapacity);
    childQueue.setAbsoluteUsedCapacity(absoluteUsedCapacity);
    
    Resource available = Resources.subtract(queueLimit, usedResources);
    childQueue.getMetrics().setAvailableResourcesToQueue(
        Resources.max(
            calculator, 
            clusterResource, 
            available, 
            Resources.none()
            )
        );
   }
}
