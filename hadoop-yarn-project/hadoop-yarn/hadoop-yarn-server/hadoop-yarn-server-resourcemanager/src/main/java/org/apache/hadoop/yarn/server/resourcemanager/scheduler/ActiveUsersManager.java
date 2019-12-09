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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.utils.Lock;

/**
 * {@link ActiveUsersManager} tracks active users in the system.
 * A user is deemed to be active if he has any running applications with
 * outstanding resource requests.
 * 
 * An active user is defined as someone with outstanding resource requests.
 */
@Private
public class ActiveUsersManager implements AbstractUsersManager {

  private static final Log LOG = LogFactory.getLog(ActiveUsersManager.class);
  
  private final QueueMetrics metrics;
  
  private int activeUsers = 0;
  private Map<String, Set<ApplicationId>> usersApplications = 
      new HashMap<String, Set<ApplicationId>>();

  public ActiveUsersManager(QueueMetrics metrics) {
    this.metrics = metrics;
  }
  
  /**
   * An application has new outstanding requests.
   * 
   * @param user application user 
   * @param applicationId activated application
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public void activateApplication(
      String user, ApplicationId applicationId) {
    Set<ApplicationId> userApps = usersApplications.get(user);
    if (userApps == null) {
      userApps = new HashSet<ApplicationId>();
      usersApplications.put(user, userApps);
      ++activeUsers;
      metrics.incrActiveUsers();
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + user + " added to activeUsers, currently: "
            + activeUsers);
      }
    }
    if (userApps.add(applicationId)) {
      metrics.activateApp(user);
    }
  }
  
  /**
   * An application has no more outstanding requests.
   * 
   * @param user application user 
   * @param applicationId deactivated application
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public void deactivateApplication(
      String user, ApplicationId applicationId) {
    Set<ApplicationId> userApps = usersApplications.get(user);
    if (userApps != null) {
      if (userApps.remove(applicationId)) {
        metrics.deactivateApp(user);
      }
      if (userApps.isEmpty()) {
        usersApplications.remove(user);
        --activeUsers;
        metrics.decrActiveUsers();
        if (LOG.isDebugEnabled()) {
          LOG.debug("User " + user + " removed from activeUsers, currently: "
              + activeUsers);
        }
      }
    }
  }

  /**
   * Get number of active users i.e. users with applications which have pending
   * resource requests.
   * @return number of active users
   */
  @Lock({Queue.class, SchedulerApplicationAttempt.class})
  @Override
  synchronized public int getNumActiveUsers() {
    return activeUsers;
  }
}
