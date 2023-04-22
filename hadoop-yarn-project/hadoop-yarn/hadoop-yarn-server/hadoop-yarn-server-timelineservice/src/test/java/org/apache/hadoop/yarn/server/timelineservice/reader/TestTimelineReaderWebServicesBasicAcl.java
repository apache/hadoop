/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.ForbiddenException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineReaderWebServicesBasicAcl {

  private TimelineReaderManager manager;
  private static String adminUser = "admin";
  private static UserGroupInformation adminUgi =
      UserGroupInformation.createRemoteUser(adminUser);
  private Configuration config;

  @BeforeEach public void setUp() throws Exception {
    config = new YarnConfiguration();
  }

  @AfterEach public void tearDown() throws Exception {
    if (manager != null) {
      manager.stop();
      manager = null;
    }
    config = null;
  }

  @Test
  void testTimelineReaderManagerAclsWhenDisabled()
      throws Exception {
    config.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    config.set(YarnConfiguration.YARN_ADMIN_ACL, adminUser);
    manager = new TimelineReaderManager(null);
    manager.init(config);
    manager.start();

    // when acls are disabled, always return true
    assertTrue(manager.checkAccess(null));

    // filter is disabled, so should return false
    assertFalse(
        TimelineReaderWebServices.isDisplayEntityPerUserFilterEnabled(config));
  }

  @Test
  void testTimelineReaderManagerAclsWhenEnabled()
      throws Exception {
    Configuration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    config.setBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, true);
    config.set(YarnConfiguration.YARN_ADMIN_ACL, adminUser);
    manager = new TimelineReaderManager(null);
    manager.init(config);
    manager.start();

    String user1 = "user1";
    String user2 = "user2";
    UserGroupInformation user1Ugi =
        UserGroupInformation.createRemoteUser(user1);
    UserGroupInformation user2Ugi =
        UserGroupInformation.createRemoteUser(user2);

    // false because ugi is null
    assertFalse(TimelineReaderWebServices
        .validateAuthUserWithEntityUser(manager, null, user1));

    // false because ugi is null in non-secure cluster. User must pass
    // ?user.name as query params in REST end points.
    try {
      TimelineReaderWebServices.checkAccess(manager, null, user1);
      fail("user1Ugi is not allowed to view user1");
    } catch (ForbiddenException e) {
      // expected
    }

    // incoming ugi is admin asking for entity owner user1
    assertTrue(
        TimelineReaderWebServices.checkAccess(manager, adminUgi, user1));

    // incoming ugi is admin asking for entity owner user1
    assertTrue(
        TimelineReaderWebServices.checkAccess(manager, adminUgi, user2));

    // incoming ugi is non-admin i.e user1Ugi asking for entity owner user2
    try {
      TimelineReaderWebServices.checkAccess(manager, user1Ugi, user2);
      fail("user1Ugi is not allowed to view user2");
    } catch (ForbiddenException e) {
      // expected
    }

    // incoming ugi is non-admin i.e user2Ugi asking for entity owner user1
    try {
      TimelineReaderWebServices.checkAccess(manager, user1Ugi, user2);
      fail("user2Ugi is not allowed to view user1");
    } catch (ForbiddenException e) {
      // expected
    }

    String userKey = "user";
    // incoming ugi is admin asking for entities
    Set<TimelineEntity> entities = createEntities(10, userKey);
    TimelineReaderWebServices
        .checkAccess(manager, adminUgi, entities, userKey, true);
    // admin is allowed to view other entities
    assertEquals(10, entities.size());

    // incoming ugi is user1Ugi asking for entities
    // only user1 entities are allowed to view
    entities = createEntities(5, userKey);
    TimelineReaderWebServices
        .checkAccess(manager, user1Ugi, entities, userKey, true);
    assertEquals(1, entities.size());
    assertEquals(user1, entities.iterator().next().getInfo().get(userKey));

    // incoming ugi is user2Ugi asking for entities
    // only user2 entities are allowed to view
    entities = createEntities(8, userKey);
    TimelineReaderWebServices
        .checkAccess(manager, user2Ugi, entities, userKey, true);
    assertEquals(1, entities.size());
    assertEquals(user2, entities.iterator().next().getInfo().get(userKey));
  }

  Set<TimelineEntity> createEntities(int noOfUsers, String userKey) {
    Set<TimelineEntity> entities = new LinkedHashSet<>();
    for (int i = 0; i < noOfUsers; i++) {
      TimelineEntity e = new TimelineEntity();
      e.setType("user" + i);
      e.setId("user" + i);
      e.getInfo().put(userKey, "user" + i);
      entities.add(e);
    }
    return entities;
  }

}
