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

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;

/**
 * PriorityACLGroup will hold all ACL related information per priority.
 *
 */
public class AppPriorityACLGroup implements Comparable<AppPriorityACLGroup> {

  private Priority maxPriority = null;
  private Priority defaultPriority = null;
  private AccessControlList aclList = null;

  public AppPriorityACLGroup(Priority maxPriority, Priority defaultPriority,
      AccessControlList aclList) {
    this.setMaxPriority(Priority.newInstance(maxPriority.getPriority()));
    this.setDefaultPriority(
        Priority.newInstance(defaultPriority.getPriority()));
    this.setACLList(aclList);
  }

  public AppPriorityACLGroup() {
  }

  @Override
  public int compareTo(AppPriorityACLGroup o) {
    return getMaxPriority().compareTo(o.getMaxPriority());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    AppPriorityACLGroup other = (AppPriorityACLGroup) obj;
    if (getMaxPriority() != other.getMaxPriority()) {
      return false;
    }

    if (getDefaultPriority() != other.getDefaultPriority()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 517861;
    int result = 9511;
    result = prime * result + getMaxPriority().getPriority();
    result = prime * result + getDefaultPriority().getPriority();
    return result;
  }

  public Priority getMaxPriority() {
    return maxPriority;
  }

  public Priority getDefaultPriority() {
    return defaultPriority;
  }

  public AccessControlList getACLList() {
    return aclList;
  }

  public void setMaxPriority(Priority maxPriority) {
    this.maxPriority = maxPriority;
  }

  public void setDefaultPriority(Priority defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setACLList(AccessControlList accessControlList) {
    this.aclList = accessControlList;
  }
}
