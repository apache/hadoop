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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore.AppState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestEntityGroupFSTimelineStore extends TimelineStoreTestUtils {

  private static final String SAMPLE_APP_NAME = "1234_5678";

  static final ApplicationId TEST_APPLICATION_ID
      = ConverterUtils.toApplicationId(
      ConverterUtils.APPLICATION_PREFIX + "_" + SAMPLE_APP_NAME);

  private static final String TEST_APP_DIR_NAME
      = TEST_APPLICATION_ID.toString();
  private static final String TEST_ATTEMPT_DIR_NAME
      = ApplicationAttemptId.appAttemptIdStrPrefix + SAMPLE_APP_NAME + "_1";
  private static final String TEST_SUMMARY_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.SUMMARY_LOG_PREFIX + "test";
  private static final String TEST_ENTITY_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.ENTITY_LOG_PREFIX
          + EntityGroupPlugInForTest.getStandardTimelineGroupId();
  private static final String TEST_DOMAIN_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.DOMAIN_LOG_PREFIX + "test";

  private static final Path TEST_ROOT_DIR
      = new Path(System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestEntityGroupFSTimelineStore.class.getSimpleName());
  private static final Path TEST_APP_DIR_PATH
      = new Path(TEST_ROOT_DIR, TEST_APP_DIR_NAME);
  private static final Path TEST_ATTEMPT_DIR_PATH
      = new Path(TEST_APP_DIR_PATH, TEST_ATTEMPT_DIR_NAME);
  private static final Path TEST_DONE_DIR_PATH
      = new Path(TEST_ROOT_DIR, "done");

  private static Configuration config = new YarnConfiguration();
  private static MiniDFSCluster hdfsCluster;
  private static FileSystem fs;
  private EntityGroupFSTimelineStore store;
  private TimelineEntity entityNew;

  @Rule
  public TestName currTestName = new TestName();

  @BeforeClass
  public static void setupClass() throws Exception {
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    config.set(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
        "YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER");
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        TEST_DONE_DIR_PATH.toString());
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR.toString());
    HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    hdfsCluster
        = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(1).build();
    fs = hdfsCluster.getFileSystem();
  }

  @Before
  public void setup() throws Exception {
    createTestFiles();
    store = new EntityGroupFSTimelineStore();
    if (currTestName.getMethodName().contains("Plugin")) {
      config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES,
          EntityGroupPlugInForTest.class.getName());
    }
    store.init(config);
    store.start();
    store.setFs(fs);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(TEST_APP_DIR_PATH, true);
    store.stop();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    hdfsCluster.shutdown();
    FileContext fileContext = FileContext.getLocalFSFileContext();
    fileContext.delete(new Path(
        config.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH)), true);
  }

  @Test
  public void testAppLogsScanLogs() throws Exception {
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    appLogs.scanForLogs();
    List<LogInfo> summaryLogs = appLogs.getSummaryLogs();
    List<LogInfo> detailLogs = appLogs.getDetailLogs();
    assertEquals(2, summaryLogs.size());
    assertEquals(1, detailLogs.size());

    for (LogInfo log : summaryLogs) {
      String fileName = log.getFilename();
      assertTrue(fileName.equals(TEST_SUMMARY_LOG_FILE_NAME)
          || fileName.equals(TEST_DOMAIN_LOG_FILE_NAME));
    }

    for (LogInfo log : detailLogs) {
      String fileName = log.getFilename();
      assertEquals(fileName, TEST_ENTITY_LOG_FILE_NAME);
    }
  }

  @Test
  public void testMoveToDone() throws Exception {
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    Path pathBefore = appLogs.getAppDirPath();
    appLogs.moveToDone();
    Path pathAfter = appLogs.getAppDirPath();
    assertNotEquals(pathBefore, pathAfter);
    assertTrue(pathAfter.toString().contains(TEST_DONE_DIR_PATH.toString()));
  }

  @Test
  public void testParseSummaryLogs() throws Exception {
    TimelineDataManager tdm = PluginStoreTestUtils.getTdmWithMemStore(config);
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    appLogs.scanForLogs();
    appLogs.parseSummaryLogs(tdm);
    PluginStoreTestUtils.verifyTestEntities(tdm);
  }

  @Test
  public void testCleanLogs() throws Exception {
    // Create test dirs and files
    // Irrelevant file, should not be reclaimed
    Path irrelevantFilePath = new Path(
        TEST_DONE_DIR_PATH, "irrelevant.log");
    FSDataOutputStream stream = fs.create(irrelevantFilePath);
    stream.close();
    // Irrelevant directory, should not be reclaimed
    Path irrelevantDirPath = new Path(TEST_DONE_DIR_PATH, "irrelevant");
    fs.mkdirs(irrelevantDirPath);

    Path doneAppHomeDir = new Path(new Path(TEST_DONE_DIR_PATH, "0000"), "001");
    // First application, untouched after creation
    Path appDirClean = new Path(doneAppHomeDir, TEST_APP_DIR_NAME);
    Path attemptDirClean = new Path(appDirClean, TEST_ATTEMPT_DIR_NAME);
    fs.mkdirs(attemptDirClean);
    Path filePath = new Path(attemptDirClean, "test.log");
    stream = fs.create(filePath);
    stream.close();
    // Second application, one file touched after creation
    Path appDirHoldByFile = new Path(doneAppHomeDir, TEST_APP_DIR_NAME + "1");
    Path attemptDirHoldByFile
        = new Path(appDirHoldByFile, TEST_ATTEMPT_DIR_NAME);
    fs.mkdirs(attemptDirHoldByFile);
    Path filePathHold = new Path(attemptDirHoldByFile, "test1.log");
    stream = fs.create(filePathHold);
    stream.close();
    // Third application, one dir touched after creation
    Path appDirHoldByDir = new Path(doneAppHomeDir, TEST_APP_DIR_NAME + "2");
    Path attemptDirHoldByDir = new Path(appDirHoldByDir, TEST_ATTEMPT_DIR_NAME);
    fs.mkdirs(attemptDirHoldByDir);
    Path dirPathHold = new Path(attemptDirHoldByDir, "hold");
    fs.mkdirs(dirPathHold);
    // Fourth application, empty dirs
    Path appDirEmpty = new Path(doneAppHomeDir, TEST_APP_DIR_NAME + "3");
    Path attemptDirEmpty = new Path(appDirEmpty, TEST_ATTEMPT_DIR_NAME);
    fs.mkdirs(attemptDirEmpty);
    Path dirPathEmpty = new Path(attemptDirEmpty, "empty");
    fs.mkdirs(dirPathEmpty);

    // Should retain all logs after this run
    EntityGroupFSTimelineStore.cleanLogs(TEST_DONE_DIR_PATH, fs, 10000);
    assertTrue(fs.exists(irrelevantDirPath));
    assertTrue(fs.exists(irrelevantFilePath));
    assertTrue(fs.exists(filePath));
    assertTrue(fs.exists(filePathHold));
    assertTrue(fs.exists(dirPathHold));
    assertTrue(fs.exists(dirPathEmpty));

    // Make sure the created dir is old enough
    Thread.sleep(2000);
    // Touch the second application
    stream = fs.append(filePathHold);
    stream.writeBytes("append");
    stream.close();
    // Touch the third application by creating a new dir
    fs.mkdirs(new Path(dirPathHold, "holdByMe"));

    EntityGroupFSTimelineStore.cleanLogs(TEST_DONE_DIR_PATH, fs, 1000);

    // Verification after the second cleaner call
    assertTrue(fs.exists(irrelevantDirPath));
    assertTrue(fs.exists(irrelevantFilePath));
    assertTrue(fs.exists(filePathHold));
    assertTrue(fs.exists(dirPathHold));
    assertTrue(fs.exists(doneAppHomeDir));

    // appDirClean and appDirEmpty should be cleaned up
    assertFalse(fs.exists(appDirClean));
    assertFalse(fs.exists(appDirEmpty));
  }

  @Test
  public void testPluginRead() throws Exception {
    // Verify precondition
    assertEquals(EntityGroupPlugInForTest.class.getName(),
        store.getConfig().get(
            YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES));
    // Load data and cache item, prepare timeline store by making a cache item
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    EntityCacheItem cacheItem = new EntityCacheItem(config, fs);
    cacheItem.setAppLogs(appLogs);
    store.setCachedLogs(
        EntityGroupPlugInForTest.getStandardTimelineGroupId(), cacheItem);
    // Generate TDM
    TimelineDataManager tdm
        = PluginStoreTestUtils.getTdmWithStore(config, store);

    // Verify single entity read
    TimelineEntity entity3 = tdm.getEntity("type_3", "id_3",
        EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertNotNull(entity3);
    assertEquals(entityNew.getStartTime(), entity3.getStartTime());
    // Verify multiple entities read
    TimelineEntities entities = tdm.getEntities("type_3", null, null, null,
        null, null, null, null, EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertEquals(entities.getEntities().size(), 1);
    for (TimelineEntity entity : entities.getEntities()) {
      assertEquals(entityNew.getStartTime(), entity.getStartTime());
    }
  }

  @Test
  public void testSummaryRead() throws Exception {
    // Load data
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(TEST_APPLICATION_ID, TEST_APP_DIR_PATH,
        AppState.COMPLETED);
    TimelineDataManager tdm
        = PluginStoreTestUtils.getTdmWithStore(config, store);
    appLogs.scanForLogs();
    appLogs.parseSummaryLogs(tdm);

    // Verify single entity read
    PluginStoreTestUtils.verifyTestEntities(tdm);
    // Verify multiple entities read
    TimelineEntities entities = tdm.getEntities("type_1", null, null, null,
        null, null, null, null, EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertEquals(entities.getEntities().size(), 1);
    for (TimelineEntity entity : entities.getEntities()) {
      assertEquals((Long) 123l, entity.getStartTime());
    }

  }

  private void createTestFiles() throws IOException {
    TimelineEntities entities = PluginStoreTestUtils.generateTestEntities();
    PluginStoreTestUtils.writeEntities(entities,
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_SUMMARY_LOG_FILE_NAME), fs);

    entityNew = PluginStoreTestUtils
        .createEntity("id_3", "type_3", 789l, null, null,
            null, null, "domain_id_1");
    TimelineEntities entityList = new TimelineEntities();
    entityList.addEntity(entityNew);
    PluginStoreTestUtils.writeEntities(entityList,
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_ENTITY_LOG_FILE_NAME), fs);

    FSDataOutputStream out = fs.create(
        new Path(TEST_ATTEMPT_DIR_PATH, TEST_DOMAIN_LOG_FILE_NAME));
    out.close();
  }

}
