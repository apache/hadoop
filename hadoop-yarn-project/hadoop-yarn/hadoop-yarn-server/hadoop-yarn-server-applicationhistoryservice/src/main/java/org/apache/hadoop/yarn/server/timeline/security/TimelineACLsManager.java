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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.TimelineStore.SystemFilter;

import com.google.common.annotations.VisibleForTesting;

/**
 * <code>TimelineACLsManager</code> check the entity level timeline data access.
 */
@Private
public class TimelineACLsManager {

  private static final Log LOG = LogFactory.getLog(TimelineACLsManager.class);

  private AdminACLsManager adminAclsManager;

  public TimelineACLsManager(Configuration conf) {
    this.adminAclsManager = new AdminACLsManager(conf);
  }

  public boolean checkAccess(UserGroupInformation callerUGI,
      TimelineEntity entity) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying the access of " + callerUGI.getShortUserName()
          + " on the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType()));
    }

    if (!adminAclsManager.areACLsEnabled()) {
      return true;
    }

    Set<Object> values =
        entity.getPrimaryFilters().get(
            SystemFilter.ENTITY_OWNER.toString());
    if (values == null || values.size() != 1) {
      throw new YarnException("Owner information of the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType())
          + " is corrupted.");
    }
    String owner = values.iterator().next().toString();
    // TODO: Currently we just check the user is the admin or the timeline
    // entity owner. In the future, we need to check whether the user is in the
    // allowed user/group list
    if (callerUGI != null
        && (adminAclsManager.isAdmin(callerUGI) ||
            callerUGI.getShortUserName().equals(owner))) {
      return true;
    }
    return false;
  }

  @Private
  @VisibleForTesting
  public AdminACLsManager
      setAdminACLsManager(AdminACLsManager adminAclsManager) {
    AdminACLsManager oldAdminACLsManager = this.adminAclsManager;
    this.adminAclsManager = adminAclsManager;
    return oldAdminACLsManager;
  }

}
