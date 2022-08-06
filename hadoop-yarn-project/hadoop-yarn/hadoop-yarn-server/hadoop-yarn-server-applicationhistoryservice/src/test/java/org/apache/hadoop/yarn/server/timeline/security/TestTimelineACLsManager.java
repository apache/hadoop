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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineACLsManager {

  private static TimelineDomain domain;

  static {
    domain = new TimelineDomain();
    domain.setId("domain_id_1");
    domain.setOwner("owner");
    domain.setReaders("reader");
    domain.setWriters("writer");
  }

  @Test
  void testYarnACLsNotEnabledForEntity() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    timelineACLsManager.setTimelineStore(new TestTimelineStore());
    TimelineEntity entity = new TimelineEntity();
    entity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER
            .toString(), "owner");
    entity.setDomainId("domain_id_1");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"),
            ApplicationAccessType.VIEW_APP, entity),
        "Always true when ACLs are not enabled");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"),
            ApplicationAccessType.MODIFY_APP, entity),
        "Always true when ACLs are not enabled");
  }

  @Test
  void testYarnACLsEnabledForEntity() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    timelineACLsManager.setTimelineStore(new TestTimelineStore());
    TimelineEntity entity = new TimelineEntity();
    entity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER
            .toString(), "owner");
    entity.setDomainId("domain_id_1");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"),
            ApplicationAccessType.VIEW_APP, entity),
        "Owner should be allowed to view");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("reader"),
            ApplicationAccessType.VIEW_APP, entity),
        "Reader should be allowed to view");
    assertFalse(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"),
            ApplicationAccessType.VIEW_APP, entity),
        "Other shouldn't be allowed to view");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"),
            ApplicationAccessType.VIEW_APP, entity),
        "Admin should be allowed to view");

    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"),
            ApplicationAccessType.MODIFY_APP, entity),
        "Owner should be allowed to modify");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("writer"),
            ApplicationAccessType.MODIFY_APP, entity),
        "Writer should be allowed to modify");
    assertFalse(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"),
            ApplicationAccessType.MODIFY_APP, entity),
        "Other shouldn't be allowed to modify");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"),
            ApplicationAccessType.MODIFY_APP, entity),
        "Admin should be allowed to modify");
  }

  @Test
  void testCorruptedOwnerInfoForEntity() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "owner");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    timelineACLsManager.setTimelineStore(new TestTimelineStore());
    TimelineEntity entity = new TimelineEntity();
    try {
      timelineACLsManager.checkAccess(
          UserGroupInformation.createRemoteUser("owner"),
          ApplicationAccessType.VIEW_APP, entity);
      fail("Exception is expected");
    } catch (YarnException e) {
      assertTrue(e.getMessage()
          .contains("doesn't exist."), "It's not the exact expected exception");
    }
  }

  @Test
  void testYarnACLsNotEnabledForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    domain.setOwner("owner");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"), domain),
        "Always true when ACLs are not enabled");
  }

  @Test
  void testYarnACLsEnabledForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    domain.setOwner("owner");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"), domain),
        "Owner should be allowed to access");
    assertFalse(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"), domain),
        "Other shouldn't be allowed to access");
    assertTrue(
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"), domain),
        "Admin should be allowed to access");
  }

  @Test
  void testCorruptedOwnerInfoForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "owner");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    try {
      timelineACLsManager.checkAccess(
          UserGroupInformation.createRemoteUser("owner"), domain);
      fail("Exception is expected");
    } catch (YarnException e) {
      assertTrue(e.getMessage()
          .contains("is corrupted."), "It's not the exact expected exception");
    }
  }

  private static class TestTimelineStore extends MemoryTimelineStore {
    @Override
    public TimelineDomain getDomain(
        String domainId) throws IOException {
      if (domainId == null) {
        return null;
      } else {
        return domain;
      }
    }
  }
}
