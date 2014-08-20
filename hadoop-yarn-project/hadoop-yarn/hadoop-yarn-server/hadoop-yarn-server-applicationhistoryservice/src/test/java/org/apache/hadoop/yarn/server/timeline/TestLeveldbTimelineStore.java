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

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.iq80.leveldb.DBIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TestLeveldbTimelineStore extends TimelineStoreTestUtils {
  private FileContext fsContext;
  private File fsPath;
  private Configuration config = new YarnConfiguration();

  @Before
  public void setup() throws Exception {
    fsContext = FileContext.getLocalFSFileContext();
    fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    config.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    store = new LeveldbTimelineStore();
    store.init(config);
    store.start();
    loadTestData();
    loadVerificationData();
  }

  @After
  public void tearDown() throws Exception {
    store.stop();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
  }

  @Test
  public void testRootDirPermission() throws IOException {
    FileSystem fs = FileSystem.getLocal(new YarnConfiguration());
    FileStatus file = fs.getFileStatus(
        new Path(fsPath.getAbsolutePath(), LeveldbTimelineStore.FILENAME));
    assertNotNull(file);
    assertEquals(LeveldbTimelineStore.LEVELDB_DIR_UMASK, file.getPermission());
  }

  @Test
  public void testGetSingleEntity() throws IOException {
    super.testGetSingleEntity();
    ((LeveldbTimelineStore)store).clearStartTimeCache();
    super.testGetSingleEntity();
    loadTestData();
  }

  @Test
  public void testGetEntities() throws IOException {
    super.testGetEntities();
  }

  @Test
  public void testGetEntitiesWithFromId() throws IOException {
    super.testGetEntitiesWithFromId();
  }

  @Test
  public void testGetEntitiesWithFromTs() throws IOException {
    super.testGetEntitiesWithFromTs();
  }

  @Test
  public void testGetEntitiesWithPrimaryFilters() throws IOException {
    super.testGetEntitiesWithPrimaryFilters();
  }

  @Test
  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    super.testGetEntitiesWithSecondaryFilters();
  }

  @Test
  public void testGetEvents() throws IOException {
    super.testGetEvents();
  }

  @Test
  public void testCacheSizes() {
    Configuration conf = new Configuration();
    assertEquals(10000, LeveldbTimelineStore.getStartTimeReadCacheSize(conf));
    assertEquals(10000, LeveldbTimelineStore.getStartTimeWriteCacheSize(conf));
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        10001);
    assertEquals(10001, LeveldbTimelineStore.getStartTimeReadCacheSize(conf));
    conf = new Configuration();
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        10002);
    assertEquals(10002, LeveldbTimelineStore.getStartTimeWriteCacheSize(conf));
  }

  private boolean deleteNextEntity(String entityType, byte[] ts)
      throws IOException, InterruptedException {
    DBIterator iterator = null;
    DBIterator pfIterator = null;
    try {
      iterator = ((LeveldbTimelineStore)store).getDbIterator(false);
      pfIterator = ((LeveldbTimelineStore)store).getDbIterator(false);
      return ((LeveldbTimelineStore)store).deleteNextEntity(entityType, ts,
          iterator, pfIterator, false);
    } finally {
      IOUtils.cleanup(null, iterator, pfIterator);
    }
  }

  @Test
  public void testGetEntityTypes() throws IOException {
    List<String> entityTypes = ((LeveldbTimelineStore)store).getEntityTypes();
    assertEquals(4, entityTypes.size());
    assertEquals(entityType1, entityTypes.get(0));
    assertEquals(entityType2, entityTypes.get(1));
    assertEquals(entityType4, entityTypes.get(2));
    assertEquals(entityType5, entityTypes.get(3));
  }

  @Test
  public void testDeleteEntities() throws IOException, InterruptedException {
    assertEquals(2, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());

    assertEquals(false, deleteNextEntity(entityType1,
        writeReverseOrderedLong(122l)));
    assertEquals(2, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());

    assertEquals(true, deleteNextEntity(entityType1,
        writeReverseOrderedLong(123l)));
    List<TimelineEntity> entities = getEntities("type_2");
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, Collections.singletonMap(
        entityType1, Collections.singleton(entityId1b)), EMPTY_PRIMARY_FILTERS,
        EMPTY_MAP, entities.get(0));
    entities = getEntitiesWithPrimaryFilter("type_1", userFilter);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    ((LeveldbTimelineStore)store).discardOldEntities(-123l);
    assertEquals(1, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(3, ((LeveldbTimelineStore)store).getEntityTypes().size());

    ((LeveldbTimelineStore)store).discardOldEntities(123l);
    assertEquals(0, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(0, ((LeveldbTimelineStore)store).getEntityTypes().size());
    assertEquals(0, getEntitiesWithPrimaryFilter("type_1", userFilter).size());
  }

  @Test
  public void testDeleteEntitiesPrimaryFilters()
      throws IOException, InterruptedException {
    Map<String, Set<Object>> primaryFilter =
        Collections.singletonMap("user", Collections.singleton(
            (Object) "otheruser"));
    TimelineEntities atsEntities = new TimelineEntities();
    atsEntities.setEntities(Collections.singletonList(createEntity(entityId1b,
        entityType1, 789l, Collections.singletonList(ev2), null, primaryFilter,
        null)));
    TimelinePutResponse response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());

    NameValuePair pfPair = new NameValuePair("user", "otheruser");
    List<TimelineEntity> entities = getEntitiesWithPrimaryFilter("type_1",
        pfPair);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1b, entityType1, Collections.singletonList(ev2),
        EMPTY_REL_ENTITIES, primaryFilter, EMPTY_MAP, entities.get(0));

    entities = getEntitiesWithPrimaryFilter("type_1", userFilter);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    ((LeveldbTimelineStore)store).discardOldEntities(-123l);
    assertEquals(1, getEntitiesWithPrimaryFilter("type_1", pfPair).size());
    assertEquals(2, getEntitiesWithPrimaryFilter("type_1", userFilter).size());

    ((LeveldbTimelineStore)store).discardOldEntities(123l);
    assertEquals(0, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(0, ((LeveldbTimelineStore)store).getEntityTypes().size());

    assertEquals(0, getEntitiesWithPrimaryFilter("type_1", pfPair).size());
    assertEquals(0, getEntitiesWithPrimaryFilter("type_1", userFilter).size());
  }

  @Test
  public void testFromTsWithDeletion()
      throws IOException, InterruptedException {
    long l = System.currentTimeMillis();
    assertEquals(2, getEntitiesFromTs("type_1", l).size());
    assertEquals(1, getEntitiesFromTs("type_2", l).size());
    assertEquals(2, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        l).size());
    ((LeveldbTimelineStore)store).discardOldEntities(123l);
    assertEquals(0, getEntitiesFromTs("type_1", l).size());
    assertEquals(0, getEntitiesFromTs("type_2", l).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        l).size());
    assertEquals(0, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        l).size());
    loadTestData();
    assertEquals(0, getEntitiesFromTs("type_1", l).size());
    assertEquals(0, getEntitiesFromTs("type_2", l).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        l).size());
    assertEquals(2, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());
    assertEquals(2, getEntitiesWithPrimaryFilter("type_1", userFilter).size());
  }
  
  @Test
  public void testCheckVersion() throws IOException {
    LeveldbTimelineStore dbStore = (LeveldbTimelineStore) store;
    // default version
    Version defaultVersion = dbStore.getCurrentVersion();
    Assert.assertEquals(defaultVersion, dbStore.loadVersion());

    // compatible version
    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
          defaultVersion.getMinorVersion() + 2);
    dbStore.storeVersion(compatibleVersion);
    Assert.assertEquals(compatibleVersion, dbStore.loadVersion());
    restartTimelineStore();
    dbStore = (LeveldbTimelineStore) store;
    // overwrite the compatible version
    Assert.assertEquals(defaultVersion, dbStore.loadVersion());

    // incompatible version
    Version incompatibleVersion =
      Version.newInstance(defaultVersion.getMajorVersion() + 1,
          defaultVersion.getMinorVersion());
    dbStore.storeVersion(incompatibleVersion);
    try {
      restartTimelineStore();
      Assert.fail("Incompatible version, should expect fail here.");
    } catch (ServiceStateException e) {
      Assert.assertTrue("Exception message mismatch", 
        e.getMessage().contains("Incompatible version for timeline store"));
    }
  }
  
  private void restartTimelineStore() throws IOException {
    // need to close so leveldb releases database lock
    if (store != null) {
      store.close();
    }
    store = new LeveldbTimelineStore();
    store.init(config);
    store.start();
  }

}
