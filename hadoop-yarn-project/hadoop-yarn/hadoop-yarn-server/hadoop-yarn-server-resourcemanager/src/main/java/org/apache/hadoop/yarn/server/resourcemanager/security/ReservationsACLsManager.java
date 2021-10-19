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
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link ReservationsACLsManager} is used to check a specified user's
 * permissons to perform a reservation operation on the
 * {@link ReservationACL}s are used to specify reservation operations.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class ReservationsACLsManager {
  private boolean isReservationACLsEnable;
  Map<String, Map<ReservationACL, AccessControlList>> reservationAcls =
      new HashMap<>();

  public ReservationsACLsManager(Configuration conf) throws YarnException {
    this.isReservationACLsEnable = conf.getBoolean(
        YarnConfiguration.YARN_RESERVATION_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_RESERVATION_ACL_ENABLE)
        && conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
            YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
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
