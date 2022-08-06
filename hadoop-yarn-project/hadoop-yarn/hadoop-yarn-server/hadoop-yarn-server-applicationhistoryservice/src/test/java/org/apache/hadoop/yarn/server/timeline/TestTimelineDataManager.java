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

package org.apache.hadoop.yarn.server.timeline;

import java.io.File;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class TestTimelineDataManager extends TimelineStoreTestUtils {

  private FileContext fsContext;
  private File fsPath;
  private TimelineDataManager dataManaer;
  private static TimelineACLsManager aclsManager;
  private static AdminACLsManager adminACLsManager;
  @BeforeEach
  public void setup() throws Exception {
    fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    fsContext = FileContext.getLocalFSFileContext();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    store = new LeveldbTimelineStore();
    store.init(conf);
    store.start();
    loadTestEntityData();
    loadVerificationEntityData();
    loadTestDomainData();

    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
    aclsManager = new TimelineACLsManager(conf);
    aclsManager.setTimelineStore(store);
    dataManaer = new TimelineDataManager(store, aclsManager);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    dataManaer.init(conf);
    adminACLsManager = new AdminACLsManager(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store != null) {
      store.stop();
    }
    if (fsContext != null) {
      fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    }
  }

  @Test
  void testGetOldEntityWithOutDomainId() throws Exception {
    TimelineEntity entity = dataManaer.getEntity(
        "OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1", null,
        UserGroupInformation.getCurrentUser());
    assertNotNull(entity);
    assertEquals("OLD_ENTITY_ID_1", entity.getEntityId());
    assertEquals("OLD_ENTITY_TYPE_1", entity.getEntityType());
    assertEquals(
        TimelineDataManager.DEFAULT_DOMAIN_ID, entity.getDomainId());
  }

  @Test
  void testGetEntitiesAclEnabled() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        aclsManager.setAdminACLsManager(adminACLsManager);
    try {
      TimelineEntities entities = dataManaer.getEntities(
          "ACL_ENTITY_TYPE_1", null, null, null, null, null, null, 1L, null,
          UserGroupInformation.createUserForTesting("owner_1", new String[]{"group1"}));
      assertEquals(1, entities.getEntities().size());
      assertEquals("ACL_ENTITY_ID_11",
          entities.getEntities().get(0).getEntityId());
    } finally {
      aclsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  void testGetOldEntitiesWithOutDomainId() throws Exception {
    TimelineEntities entities = dataManaer.getEntities(
        "OLD_ENTITY_TYPE_1", null, null, null, null, null, null, null, null,
        UserGroupInformation.getCurrentUser());
    assertEquals(2, entities.getEntities().size());
    assertEquals("OLD_ENTITY_ID_2",
        entities.getEntities().get(0).getEntityId());
    assertEquals("OLD_ENTITY_TYPE_1",
        entities.getEntities().get(0).getEntityType());
    assertEquals(TimelineDataManager.DEFAULT_DOMAIN_ID,
        entities.getEntities().get(0).getDomainId());
    assertEquals("OLD_ENTITY_ID_1",
        entities.getEntities().get(1).getEntityId());
    assertEquals("OLD_ENTITY_TYPE_1",
        entities.getEntities().get(1).getEntityType());
    assertEquals(TimelineDataManager.DEFAULT_DOMAIN_ID,
        entities.getEntities().get(1).getDomainId());
  }

  @Test
  void testUpdatingOldEntityWithoutDomainId() throws Exception {
    // Set the domain to the default domain when updating
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityType("OLD_ENTITY_TYPE_1");
    entity.setEntityId("OLD_ENTITY_ID_1");
    entity.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entity.addOtherInfo("NEW_OTHER_INFO_KEY", "NEW_OTHER_INFO_VALUE");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entity);
    TimelinePutResponse response = dataManaer.postEntities(
        entities, UserGroupInformation.getCurrentUser());
    assertEquals(0, response.getErrors().size());
    entity = store.getEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
    assertNotNull(entity);
    // Even in leveldb, the domain is updated to the default domain Id
    assertEquals(
        TimelineDataManager.DEFAULT_DOMAIN_ID, entity.getDomainId());
    assertEquals(1, entity.getOtherInfo().size());
    assertEquals("NEW_OTHER_INFO_KEY",
        entity.getOtherInfo().keySet().iterator().next());
    assertEquals("NEW_OTHER_INFO_VALUE",
        entity.getOtherInfo().values().iterator().next());

    // Set the domain to the non-default domain when updating
    entity = new TimelineEntity();
    entity.setEntityType("OLD_ENTITY_TYPE_1");
    entity.setEntityId("OLD_ENTITY_ID_2");
    entity.setDomainId("NON_DEFAULT");
    entity.addOtherInfo("NEW_OTHER_INFO_KEY", "NEW_OTHER_INFO_VALUE");
    entities = new TimelineEntities();
    entities.addEntity(entity);
    response = dataManaer.postEntities(
        entities, UserGroupInformation.getCurrentUser());
    assertEquals(1, response.getErrors().size());
    assertEquals(TimelinePutResponse.TimelinePutError.ACCESS_DENIED,
        response.getErrors().get(0).getErrorCode());
    entity = store.getEntity("OLD_ENTITY_ID_2", "OLD_ENTITY_TYPE_1", null);
    assertNotNull(entity);
    // In leveldb, the domain Id is still null
    assertNull(entity.getDomainId());
    // Updating is not executed
    assertEquals(0, entity.getOtherInfo().size());
  }
  
}
