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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link ReservationsACLsManager} is used to check a specified user's
 * permissons to perform a reservation operation on the
 * {@link CapacityScheduler} and the {@link FairScheduler}.
 * {@link ReservationACL}s are used to specify reservation operations.
 */
public class ReservationsACLsManager {
  private boolean isReservationACLsEnable;
  private Map<String, Map<ReservationACL, AccessControlList>> reservationAcls
          = new HashMap<>();

  public ReservationsACLsManager(ResourceScheduler scheduler,
          Configuration conf) throws YarnException {
    this.isReservationACLsEnable =
            conf.getBoolean(YarnConfiguration.YARN_RESERVATION_ACL_ENABLE,
                    YarnConfiguration.DEFAULT_YARN_RESERVATION_ACL_ENABLE) &&
            conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
                    YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
    if (scheduler instanceof CapacityScheduler) {
      CapacitySchedulerConfiguration csConf = new
              CapacitySchedulerConfiguration(conf);

      for (String planQueue : scheduler.getPlanQueues()) {
        CSQueue queue = ((CapacityScheduler) scheduler).getQueue(planQueue);
        reservationAcls.put(planQueue, csConf.getReservationAcls(queue
                .getQueuePath()));
      }
    } else if (scheduler instanceof FairScheduler) {
      AllocationConfiguration aConf = ((FairScheduler) scheduler)
              .getAllocationConfiguration();
      for (String planQueue : scheduler.getPlanQueues()) {
        reservationAcls.put(planQueue, aConf.getReservationAcls(planQueue));
      }
    }
  }

  public boolean checkAccess(UserGroupInformation callerUGI,
      ReservationACL acl, String queueName) {
    if (!isReservationACLsEnable) {
      return true;
    }

    if (this.reservationAcls.containsKey(queueName)) {
      Map<ReservationACL, AccessControlList> acls = this.reservationAcls.get(
              queueName);
      if (acls != null && acls.containsKey(acl)) {
        return acls.get(acl).isUserAllowed(callerUGI);
      } else {
        // Give access if acl is undefined for queue.
        return true;
      }
    }

    return false;
  }
}
