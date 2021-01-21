/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.helper;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

public class CapacitySchedulerInfoHelper {
  private static final String AUTO_CREATED_LEAF = "autoCreatedLeaf";
  private static final String STATIC_LEAF = "staticLeaf";
  private static final String AUTO_CREATED_PARENT = "autoCreatedParent";
  private static final String STATIC_PARENT = "staticParent";
  private static final String UNKNOWN_QUEUE = "unknown";

  private CapacitySchedulerInfoHelper() {}

  public static String getMode(CSQueue queue) throws YarnRuntimeException {
    if (queue.getCapacityConfigType() ==
        AbstractCSQueue.CapacityConfigType.ABSOLUTE_RESOURCE) {
      return "absolute";
    } else if (queue.getCapacityConfigType() ==
        AbstractCSQueue.CapacityConfigType.PERCENTAGE) {
      float weight = queue.getQueueCapacities().getWeight();
      if (weight == -1) {
        //-1 indicates we are not in weight mode
        return "percentage";
      } else {
        return "weight";
      }
    }
    throw new YarnRuntimeException("Unknown mode for queue: " +
        queue.getQueuePath() + ". Queue details: " + queue);
  }

  public static String getQueueType(CSQueue queue) {
    if (queue instanceof LeafQueue) {
      if (((AbstractCSQueue)queue).isDynamicQueue()) {
        return AUTO_CREATED_LEAF;
      } else {
        return STATIC_LEAF;
      }
    } else if (queue instanceof ParentQueue) {
      if (((AbstractCSQueue)queue).isDynamicQueue()) {
        return AUTO_CREATED_PARENT;
      } else {
        //A ParentQueue with isDynamic=false or an AbstractManagedParentQueue
        return STATIC_PARENT;
      }
    }
    return UNKNOWN_QUEUE;
  }
}
