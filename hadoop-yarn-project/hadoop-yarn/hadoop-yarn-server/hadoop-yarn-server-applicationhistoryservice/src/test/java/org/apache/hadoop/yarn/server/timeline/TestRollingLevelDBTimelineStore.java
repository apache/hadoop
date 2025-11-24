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
import java.io.FilenameFilter;
import java.io.IOException;

import org.eclipse.jetty.util.log.Log;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.Options;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Test class to verify RollingLevelDBTimelineStore. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TestRollingLevelDBTimelineStore extends TimelineStoreTestUtils {
  private FileContext fsContext;
  private File fsPath;
  private Configuration config = new YarnConfiguration();

  @BeforeEach
  public void setup() throws Exception {
    fsContext = FileContext.getLocalFSFileContext();
    fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    config.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    store = new RollingLevelDBTimelineStore();
    store.init(config);
    store.start();
    loadTestEntityData();
    loadVerificationEntityData();
    loadTestDomainData();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.stop();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
  }

  @Test
  void testRootDirPermission() throws IOException {
    FileSystem fs = FileSystem.getLocal(new YarnConfiguration());
    FileStatus file = fs.getFileStatus(new Path(fsPath.getAbsolutePath(),
        RollingLevelDBTimelineStore.FILENAME));
    assertNotNull(file);
    assertEquals(RollingLevelDBTimelineStore.LEVELDB_DIR_UMASK,
        file.getPermission());
  }

  @Test
  public void testGetSingleEntity() throws IOException {
    super.testGetSingleEntity();
    ((RollingLevelDBTimelineStore)store).clearStartTimeCache();
    super.testGetSingleEntity();
    loadTestEntityData();
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
    // feature not supported
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
  void testCacheSizes() {
    Configuration conf = new Configuration();
    assertEquals(10000,
        RollingLevelDBTimelineStore.getStartTimeReadCacheSize(conf));
    assertEquals(10000,
        RollingLevelDBTimelineStore.getStartTimeWriteCacheSize(conf));
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        10001);
    assertEquals(10001,
        RollingLevelDBTimelineStore.getStartTimeReadCacheSize(conf));
    conf = new Configuration();
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        10002);
    assertEquals(10002,
        RollingLevelDBTimelineStore.getStartTimeWriteCacheSize(conf));
  }

  @Test
  void testCheckVersion() throws IOException {
    RollingLevelDBTimelineStore dbStore = (RollingLevelDBTimelineStore) store;
    // default version
    Version defaultVersion = dbStore.getCurrentVersion();
    assertEquals(defaultVersion, dbStore.loadVersion());

    // compatible version
    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
            defaultVersion.getMinorVersion() + 2);
    dbStore.storeVersion(compatibleVersion);
    assertEquals(compatibleVersion, dbStore.loadVersion());
    restartTimelineStore();
    dbStore = (RollingLevelDBTimelineStore) store;
    // overwrite the compatible version
    assertEquals(defaultVersion, dbStore.loadVersion());

    // incompatible version
    Version incompatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion() + 1,
            defaultVersion.getMinorVersion());
    dbStore.storeVersion(incompatibleVersion);
    try {
      restartTimelineStore();
      fail("Incompatible version, should expect fail here.");
    } catch (ServiceStateException e) {
      assertTrue(e.getMessage().contains("Incompatible version for timeline store"),
          "Exception message mismatch");
    }
  }

  @Test
  void testValidateConfig() throws IOException {
    Configuration copyConfig = new YarnConfiguration(config);
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS, 0);
      config = newConfig;
      restartTimelineStore();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_TTL_MS));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS, 0);
      config = newConfig;
      restartTimelineStore();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE, -1);
      config = newConfig;
      restartTimelineStore();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
          0);
      config = newConfig;
      restartTimelineStore();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration
              .TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration
              .TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
          0);
      config = newConfig;
      restartTimelineStore();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration
              .TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE));
    }
    config = copyConfig;
    restartTimelineStore();
  }

  private void restartTimelineStore() throws IOException {
    // need to close so leveldb releases database lock
    if (store != null) {
      store.close();
    }
    store = new RollingLevelDBTimelineStore();
    store.init(config);
    store.start();
  }

  @Test
  public void testGetDomain() throws IOException {
    super.testGetDomain();
  }

  @Test
  public void testGetDomains() throws IOException {
    super.testGetDomains();
  }

  @Test
  void testRelatingToNonExistingEntity() throws IOException {
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("TEST_ENTITY_TYPE_1");
    entityToStore.setEntityId("TEST_ENTITY_ID_1");
    entityToStore.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entityToStore.addRelatedEntity("TEST_ENTITY_TYPE_2", "TEST_ENTITY_ID_2");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    store.put(entities);
    TimelineEntity entityToGet =
        store.getEntity("TEST_ENTITY_ID_2", "TEST_ENTITY_TYPE_2", null);
    assertNotNull(entityToGet);
    assertEquals("DEFAULT", entityToGet.getDomainId());
    assertEquals("TEST_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    assertEquals("TEST_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());
  }

  @Test
  void testRelatingToEntityInSamePut() throws IOException {
    TimelineEntity entityToRelate = new TimelineEntity();
    entityToRelate.setEntityType("TEST_ENTITY_TYPE_2");
    entityToRelate.setEntityId("TEST_ENTITY_ID_2");
    entityToRelate.setDomainId("TEST_DOMAIN");
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("TEST_ENTITY_TYPE_1");
    entityToStore.setEntityId("TEST_ENTITY_ID_1");
    entityToStore.setDomainId("TEST_DOMAIN");
    entityToStore.addRelatedEntity("TEST_ENTITY_TYPE_2", "TEST_ENTITY_ID_2");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    entities.addEntity(entityToRelate);
    store.put(entities);
    TimelineEntity entityToGet =
        store.getEntity("TEST_ENTITY_ID_2", "TEST_ENTITY_TYPE_2", null);
    assertNotNull(entityToGet);
    assertEquals("TEST_DOMAIN", entityToGet.getDomainId());
    assertEquals("TEST_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    assertEquals("TEST_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());
  }

  @Test
  void testRelatingToOldEntityWithoutDomainId() throws IOException {
    // New entity is put in the default domain
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("NEW_ENTITY_TYPE_1");
    entityToStore.setEntityId("NEW_ENTITY_ID_1");
    entityToStore.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entityToStore.addRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    store.put(entities);

    TimelineEntity entityToGet =
        store.getEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
    assertNotNull(entityToGet);
    assertEquals("DEFAULT", entityToGet.getDomainId());
    assertEquals("NEW_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    assertEquals("NEW_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());

    // New entity is not put in the default domain
    entityToStore = new TimelineEntity();
    entityToStore.setEntityType("NEW_ENTITY_TYPE_2");
    entityToStore.setEntityId("NEW_ENTITY_ID_2");
    entityToStore.setDomainId("NON_DEFAULT");
    entityToStore.addRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
    entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    TimelinePutResponse response = store.put(entities);
    assertEquals(1, response.getErrors().size());
    assertEquals(TimelinePutError.FORBIDDEN_RELATION,
        response.getErrors().get(0).getErrorCode());
    entityToGet =
        store.getEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
    assertNotNull(entityToGet);
    assertEquals("DEFAULT", entityToGet.getDomainId());
    // Still have one related entity
    assertEquals(1, entityToGet.getRelatedEntities().keySet().size());
    assertEquals(1, entityToGet.getRelatedEntities().values()
        .iterator().next().size());
  }

  public void testStorePerformance() throws IOException {
    TimelineEntity entityToStorePrep = new TimelineEntity();
    entityToStorePrep.setEntityType("TEST_ENTITY_TYPE_PREP");
    entityToStorePrep.setEntityId("TEST_ENTITY_ID_PREP");
    entityToStorePrep.setDomainId("TEST_DOMAIN");
    entityToStorePrep.addRelatedEntity("TEST_ENTITY_TYPE_2",
        "TEST_ENTITY_ID_2");
    entityToStorePrep.setStartTime(0L);

    TimelineEntities entitiesPrep = new TimelineEntities();
    entitiesPrep.addEntity(entityToStorePrep);
    store.put(entitiesPrep);

    long start = System.currentTimeMillis();
    int num = 1000000;

    Log.getLog().info("Start test for " + num);

    final String tezTaskAttemptId = "TEZ_TA";
    final String tezEntityId = "attempt_1429158534256_0001_1_00_000000_";
    final String tezTaskId = "TEZ_T";
    final String tezDomainId = "Tez_ATS_application_1429158534256_0001";

    TimelineEntity entityToStore = new TimelineEntity();
    TimelineEvent startEvt = new TimelineEvent();
    entityToStore.setEntityType(tezTaskAttemptId);

    startEvt.setEventType("TASK_ATTEMPT_STARTED");
    startEvt.setTimestamp(0);
    entityToStore.addEvent(startEvt);
    entityToStore.setDomainId(tezDomainId);

    entityToStore.addPrimaryFilter("status", "SUCCEEDED");
    entityToStore.addPrimaryFilter("applicationId",
        "application_1429158534256_0001");
    entityToStore.addPrimaryFilter("TEZ_VERTEX_ID",
        "vertex_1429158534256_0001_1_00");
    entityToStore.addPrimaryFilter("TEZ_DAG_ID", "dag_1429158534256_0001_1");
    entityToStore.addPrimaryFilter("TEZ_TASK_ID",
        "task_1429158534256_0001_1_00_000000");

    entityToStore.setStartTime(0L);
    entityToStore.addOtherInfo("startTime", 0);
    entityToStore.addOtherInfo("inProgressLogsURL",
        "localhost:8042/inProgressLogsURL");
    entityToStore.addOtherInfo("completedLogsURL", "");
    entityToStore.addOtherInfo("nodeId", "localhost:54450");
    entityToStore.addOtherInfo("nodeHttpAddress", "localhost:8042");
    entityToStore.addOtherInfo("containerId",
        "container_1429158534256_0001_01_000002");
    entityToStore.addOtherInfo("status", "RUNNING");
    entityToStore.addRelatedEntity(tezTaskId, "TEZ_TASK_ID_1");

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);

    for (int i = 0; i < num; ++i) {
      entityToStore.setEntityId(tezEntityId + i);
      store.put(entities);
    }

    long duration = System.currentTimeMillis() - start;
    Log.getLog().info("Duration for " + num + ": " + duration);
  }

  /**
   * Test that RollingLevelDb repair is attempted at least once during
   * serviceInit for RollingLeveldbTimelineStore in case open fails the
   * first time.
   */ @Test
  void testLevelDbRepair() throws IOException {
    RollingLevelDBTimelineStore store = new RollingLevelDBTimelineStore();
    JniDBFactory factory = Mockito.mock(JniDBFactory.class);
    Mockito.when(factory.open(Mockito.any(File.class), Mockito.any(Options.class)))
        .thenThrow(new IOException()).thenCallRealMethod();
    store.setFactory(factory);

    //Create the LevelDb in a different location
    File path = new File("target", this.getClass().getSimpleName() + "-tmpDir2").getAbsoluteFile();
    Configuration conf = new Configuration(this.config);
    conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH, path.getAbsolutePath());
    try {
      store.init(conf);
      Mockito.verify(factory, Mockito.times(1))
          .repair(Mockito.any(File.class), Mockito.any(Options.class));
      FilenameFilter fileFilter = WildcardFileFilter.builder()
              .setWildcards("*" + RollingLevelDBTimelineStore.BACKUP_EXT + "*")
              .get();
      assertTrue(new File(path.getAbsolutePath(), RollingLevelDBTimelineStore.FILENAME)
          .list(fileFilter).length > 0);
    } finally {
      store.close();
      fsContext.delete(new Path(path.getAbsolutePath()), true);
    }
  }

  public static void main(String[] args) throws Exception {
    TestRollingLevelDBTimelineStore store =
        new TestRollingLevelDBTimelineStore();
    store.setup();
    store.testStorePerformance();
    store.tearDown();
  }
}