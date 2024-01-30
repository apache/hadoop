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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

/**
 * This is the implementation of {@link ReservationsACLsManager} based on the
 * {@link FairScheduler}.
 */
public class FairReservationsACLsManager extends ReservationsACLsManager {

  public FairReservationsACLsManager(ResourceScheduler scheduler,
      Configuration conf) throws YarnException {
    super(conf);
    AllocationConfiguration aConf = ((FairScheduler) scheduler)
        .getAllocationConfiguration();
    for (String planQueue : scheduler.getPlanQueues()) {
      reservationAcls.put(planQueue, aConf.getReservationAcls(new QueuePath(planQueue)));
    }
  }

}
