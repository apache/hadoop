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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.junit.Assert;
import org.junit.Test;

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
  public void testYarnACLsNotEnabledForEntity() throws Exception {
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
    Assert.assertTrue(
        "Always true when ACLs are not enabled",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"),
            ApplicationAccessType.VIEW_APP, entity));
    Assert.assertTrue(
        "Always true when ACLs are not enabled",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"),
            ApplicationAccessType.MODIFY_APP, entity));
  }

  @Test
  public void testYarnACLsEnabledForEntity() throws Exception {
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
    Assert.assertTrue(
        "Owner should be allowed to view",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"),
            ApplicationAccessType.VIEW_APP, entity));
    Assert.assertTrue(
        "Reader should be allowed to view",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("reader"),
            ApplicationAccessType.VIEW_APP, entity));
    Assert.assertFalse(
        "Other shouldn't be allowed to view",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"),
            ApplicationAccessType.VIEW_APP, entity));
    Assert.assertTrue(
        "Admin should be allowed to view",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"),
            ApplicationAccessType.VIEW_APP, entity));

    Assert.assertTrue(
        "Owner should be allowed to modify",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"),
            ApplicationAccessType.MODIFY_APP, entity));
    Assert.assertTrue(
        "Writer should be allowed to modify",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("writer"),
            ApplicationAccessType.MODIFY_APP, entity));
    Assert.assertFalse(
        "Other shouldn't be allowed to modify",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"),
            ApplicationAccessType.MODIFY_APP, entity));
    Assert.assertTrue(
        "Admin should be allowed to modify",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"),
            ApplicationAccessType.MODIFY_APP, entity));
  }

  @Test
  public void testCorruptedOwnerInfoForEntity() throws Exception {
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
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      Assert.assertTrue("It's not the exact expected exception", e.getMessage()
          .contains("doesn't exist."));
    }
  }

  @Test
  public void testYarnACLsNotEnabledForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    domain.setOwner("owner");
    Assert.assertTrue(
        "Always true when ACLs are not enabled",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"), domain));
  }

  @Test
  public void testYarnACLsEnabledForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    domain.setOwner("owner");
    Assert.assertTrue(
        "Owner should be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"), domain));
    Assert.assertFalse(
        "Other shouldn't be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"), domain));
    Assert.assertTrue(
        "Admin should be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"), domain));
  }

  @Test
  public void testCorruptedOwnerInfoForDomain() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "owner");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineDomain domain = new TimelineDomain();
    try {
      timelineACLsManager.checkAccess(
          UserGroupInformation.createRemoteUser("owner"), domain);
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      Assert.assertTrue("It's not the exact expected exception", e.getMessage()
          .contains("is corrupted."));
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
