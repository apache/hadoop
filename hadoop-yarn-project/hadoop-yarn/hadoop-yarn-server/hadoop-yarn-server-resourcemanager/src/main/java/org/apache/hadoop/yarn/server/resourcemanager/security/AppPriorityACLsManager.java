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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * Manager class to store and check permission for Priority ACLs.
 */
public class AppPriorityACLsManager {

  private static final Log LOG = LogFactory
      .getLog(AppPriorityACLsManager.class);

  /*
   * An internal class to store ACLs specific to each priority. This will be
   * used to read and process acl's during app submission time as well.
   */
  private static class PriorityACL {
    private Priority priority;
    private Priority defaultPriority;
    private AccessControlList acl;

    PriorityACL(Priority priority, Priority defaultPriority,
        AccessControlList acl) {
      this.setPriority(priority);
      this.setDefaultPriority(defaultPriority);
      this.setAcl(acl);
    }

    public Priority getPriority() {
      return priority;
    }

    public void setPriority(Priority maxPriority) {
      this.priority = maxPriority;
    }

    public Priority getDefaultPriority() {
      return defaultPriority;
    }

    public void setDefaultPriority(Priority defaultPriority) {
      this.defaultPriority = defaultPriority;
    }

    public AccessControlList getAcl() {
      return acl;
    }

    public void setAcl(AccessControlList acl) {
      this.acl = acl;
    }
  }

  private boolean isACLsEnable;
  private final ConcurrentMap<String, List<PriorityACL>> allAcls =
      new ConcurrentHashMap<>();

  public AppPriorityACLsManager(Configuration conf) {
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }

  /**
   * Clear priority acl during refresh.
   *
   * @param queueName
   *          Queue Name
   */
  public void clearPriorityACLs(String queueName) {
    allAcls.remove(queueName);
  }

  /**
   * Each Queue could have configured with different priority acl's groups. This
   * method helps to store each such ACL list against queue.
   *
   * @param priorityACLGroups
   *          List of Priority ACL Groups.
   * @param queueName
   *          Queue Name associate with priority acl groups.
   */
  public void addPrioirityACLs(List<AppPriorityACLGroup> priorityACLGroups,
      String queueName) {

    List<PriorityACL> priorityACL = allAcls.get(queueName);
    if (null == priorityACL) {
      priorityACL = new ArrayList<PriorityACL>();
      allAcls.put(queueName, priorityACL);
    }

    // Ensure lowest priority PriorityACLGroup comes first in the list.
    Collections.sort(priorityACLGroups);

    for (AppPriorityACLGroup priorityACLGroup : priorityACLGroups) {
      priorityACL.add(new PriorityACL(priorityACLGroup.getMaxPriority(),
          priorityACLGroup.getDefaultPriority(),
          priorityACLGroup.getACLList()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Priority ACL group added: max-priority - "
            + priorityACLGroup.getMaxPriority() + "default-priority - "
            + priorityACLGroup.getDefaultPriority());
      }
    }
  }

  /**
   * Priority based checkAccess to ensure that given user has enough permission
   * to submit application at a given priority level.
   *
   * @param callerUGI
   *          User who submits the application.
   * @param queueName
   *          Queue to which application is submitted.
   * @param submittedPriority
   *          priority of the application.
   * @return True or False to indicate whether application can be submitted at
   *         submitted priority level or not.
   */
  public boolean checkAccess(UserGroupInformation callerUGI, String queueName,
      Priority submittedPriority) {
    if (!isACLsEnable) {
      return true;
    }

    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return true;
    }

    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls,
        callerUGI, submittedPriority);
    if (approvedPriorityACL == null) {
      return false;
    }

    return true;
  }

  /**
   * If an application is submitted without any priority, and submitted user has
   * a default priority, this method helps to update this default priority as
   * app's priority.
   *
   * @param queueName
   *          Submitted queue
   * @param user
   *          User who submitted this application
   * @return Default priority associated with given user.
   */
  public Priority getDefaultPriority(String queueName,
      UserGroupInformation user) {
    if (!isACLsEnable) {
      return null;
    }

    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return null;
    }

    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls, user,
        null);
    if (approvedPriorityACL == null) {
      return null;
    }

    Priority defaultPriority = Priority
        .newInstance(approvedPriorityACL.getDefaultPriority().getPriority());
    return defaultPriority;
  }

  private PriorityACL getMappedPriorityAclForUGI(List<PriorityACL> acls ,
      UserGroupInformation user, Priority submittedPriority) {

    // Iterate through all configured ACLs starting from lower priority.
    // If user is found corresponding to a configured priority, then store
    // that entry. if failed, continue iterate through whole acl list.
    PriorityACL selectedAcl = null;
    for (PriorityACL entry : acls) {
      AccessControlList list = entry.getAcl();

      if (list.isUserAllowed(user)) {
        selectedAcl = entry;

        // If submittedPriority is passed through the argument, also check
        // whether submittedPriority is under max-priority of each ACL group.
        if (submittedPriority != null) {
          selectedAcl = null;
          if (submittedPriority.getPriority() <= entry.getPriority()
              .getPriority()) {
            return entry;
          }
        }
      }
    }
    return selectedAcl;
  }
}
