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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

/**
 * This is the implementation of {@link ReservationsACLsManager} based on the
 * {@link CapacityScheduler}.
 */
public class CapacityReservationsACLsManager extends ReservationsACLsManager {

  public CapacityReservationsACLsManager(ResourceScheduler scheduler,
      Configuration conf) throws YarnException {
    super(conf);
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(
        conf);

    for (String planQueue : scheduler.getPlanQueues()) {
      CSQueue queue = ((CapacityScheduler) scheduler).getQueue(planQueue);
      reservationAcls.put(planQueue,
          csConf.getReservationAcls(queue.getQueuePathObject()));
    }
  }

}
