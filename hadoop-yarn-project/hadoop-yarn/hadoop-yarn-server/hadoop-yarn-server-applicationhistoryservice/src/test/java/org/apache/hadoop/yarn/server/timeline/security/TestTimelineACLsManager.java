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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.junit.Assert;
import org.junit.Test;

public class TestTimelineACLsManager {

  @Test
  public void testYarnACLsNotEnabled() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineEntity entity = new TimelineEntity();
    entity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER
            .toString(), "owner");
    Assert.assertTrue(
        "Always true when ACLs are not enabled",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("user"), entity));
  }

  @Test
  public void testYarnACLsEnabled() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineEntity entity = new TimelineEntity();
    entity.addPrimaryFilter(
        TimelineStore.SystemFilter.ENTITY_OWNER
            .toString(), "owner");
    Assert.assertTrue(
        "Owner should be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("owner"), entity));
    Assert.assertFalse(
        "Other shouldn't be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("other"), entity));
    Assert.assertTrue(
        "Admin should be allowed to access",
        timelineACLsManager.checkAccess(
            UserGroupInformation.createRemoteUser("admin"), entity));
  }

  @Test
  public void testCorruptedOwnerInfo() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "owner");
    TimelineACLsManager timelineACLsManager =
        new TimelineACLsManager(conf);
    TimelineEntity entity = new TimelineEntity();
    try {
      timelineACLsManager.checkAccess(
          UserGroupInformation.createRemoteUser("owner"), entity);
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      Assert.assertTrue("It's not the exact expected exception", e.getMessage()
          .contains("is corrupted."));
    }
  }

}
