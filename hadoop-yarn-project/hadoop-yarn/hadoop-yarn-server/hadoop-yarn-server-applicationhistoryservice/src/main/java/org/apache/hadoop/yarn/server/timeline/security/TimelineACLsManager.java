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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timeline.EntityIdentifier;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.util.StringHelper;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>TimelineACLsManager</code> check the entity level timeline data access.
 */
@Private
public class TimelineACLsManager {

  private static final Logger LOG = LoggerFactory.
      getLogger(TimelineACLsManager.class);
  private static final int DOMAIN_ACCESS_ENTRY_CACHE_SIZE = 100;

  private AdminACLsManager adminAclsManager;
  private Map<String, AccessControlListExt> aclExts;
  private TimelineStore store;

  @SuppressWarnings("unchecked")
  public TimelineACLsManager(Configuration conf) {
    this.adminAclsManager = new AdminACLsManager(conf);
    aclExts = Collections.synchronizedMap(
        new LRUMap(DOMAIN_ACCESS_ENTRY_CACHE_SIZE));
  }

  public void setTimelineStore(TimelineStore store) {
    this.store = store;
  }

  private AccessControlListExt loadDomainFromTimelineStore(
      String domainId) throws IOException {
    if (store == null) {
      return null;
    }
    TimelineDomain domain = store.getDomain(domainId);
    if (domain == null) {
      return null;
    } else {
      return putDomainIntoCache(domain);
    }
  }

  public void replaceIfExist(TimelineDomain domain) {
    if (aclExts.containsKey(domain.getId())) {
      putDomainIntoCache(domain);
    }
  }

  private AccessControlListExt putDomainIntoCache(
      TimelineDomain domain) {
    Map<ApplicationAccessType, AccessControlList> acls
    = new HashMap<ApplicationAccessType, AccessControlList>(2);
    acls.put(ApplicationAccessType.VIEW_APP,
        new AccessControlList(StringHelper.cjoin(domain.getReaders())));
    acls.put(ApplicationAccessType.MODIFY_APP,
        new AccessControlList(StringHelper.cjoin(domain.getWriters())));
    AccessControlListExt aclExt =
        new AccessControlListExt(domain.getOwner(), acls);
    aclExts.put(domain.getId(), aclExt);
    return aclExt;
  }

  public boolean checkAccess(UserGroupInformation callerUGI,
      ApplicationAccessType applicationAccessType,
      TimelineEntity entity) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying the access of "
          + (callerUGI == null ? null : callerUGI.getShortUserName())
          + " on the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType()));
    }

    if (!adminAclsManager.areACLsEnabled()) {
      return true;
    }

    // find domain owner and acls
    AccessControlListExt aclExt = aclExts.get(entity.getDomainId());
    if (aclExt == null) {
      aclExt = loadDomainFromTimelineStore(entity.getDomainId());
    }
    if (aclExt == null) {
      throw new YarnException("Domain information of the timeline entity "
          + new EntityIdentifier(entity.getEntityId(), entity.getEntityType())
          + " doesn't exist.");
    }
    String owner = aclExt.owner;
    AccessControlList domainACL = aclExt.acls.get(applicationAccessType);
    if (domainACL == null) {
      LOG.debug("ACL not found for access-type {} for domain {} owned by {}."
          + " Using default [{}]", applicationAccessType,
          entity.getDomainId(), owner, YarnConfiguration.DEFAULT_YARN_APP_ACL);
      domainACL =
          new AccessControlList(YarnConfiguration.DEFAULT_YARN_APP_ACL);
    }

    if (callerUGI != null
        && (adminAclsManager.isAdmin(callerUGI) ||
            callerUGI.getShortUserName().equals(owner) ||
            domainACL.isUserAllowed(callerUGI))) {
      return true;
    }
    return false;
  }

  public boolean checkAccess(UserGroupInformation callerUGI,
      TimelineDomain domain) throws YarnException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Verifying the access of "
          + (callerUGI == null ? null : callerUGI.getShortUserName())
          + " on the timeline domain " + domain);
    }

    if (!adminAclsManager.areACLsEnabled()) {
      return true;
    }

    String owner = domain.getOwner();
    if (owner == null || owner.length() == 0) {
      throw new YarnException("Owner information of the timeline domain "
          + domain.getId() + " is corrupted.");
    }
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

  private static class AccessControlListExt {
    private String owner;
    private Map<ApplicationAccessType, AccessControlList> acls;

    public AccessControlListExt(
        String owner, Map<ApplicationAccessType, AccessControlList> acls) {
      this.owner = owner;
      this.acls = acls;
    }
  }
}
