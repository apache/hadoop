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
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.EntityGroupFSTimelineStore.AppState;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestEntityGroupFSTimelineStore extends TimelineStoreTestUtils {

  private static final String SAMPLE_APP_PREFIX_CACHE_TEST = "1234_000";
  private static final int CACHE_TEST_CACHE_SIZE = 5;

  private static final String TEST_SUMMARY_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.SUMMARY_LOG_PREFIX + "test";
  private static final String TEST_DOMAIN_LOG_FILE_NAME
      = EntityGroupFSTimelineStore.DOMAIN_LOG_PREFIX + "test";

  private static final Path TEST_ROOT_DIR
      = new Path(System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestEntityGroupFSTimelineStore.class.getSimpleName());

  private static Configuration config = new YarnConfiguration();
  private static MiniDFSCluster hdfsCluster;
  private static FileSystem fs;
  private static FileContext fc;
  private static FileContextTestHelper fileContextTestHelper =
      new FileContextTestHelper("/tmp/TestEntityGroupFSTimelineStore");

  private static List<ApplicationId> sampleAppIds;
  private static ApplicationId mainTestAppId;
  private static Path mainTestAppDirPath;
  private static Path testDoneDirPath;
  private static Path testActiveDirPath;
  private static String mainEntityLogFileName;

  private EntityGroupFSTimelineStore store;
  private TimelineEntity entityNew;

  @Rule
  public TestName currTestName = new TestName();
  private File rootDir;
  private File testJar;

  @BeforeClass
  public static void setupClass() throws Exception {
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    config.set(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES,
        "YARN_APPLICATION,YARN_APPLICATION_ATTEMPT,YARN_CONTAINER");
    config.setInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE,
        CACHE_TEST_CACHE_SIZE);
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR.toString());
    HdfsConfiguration hdfsConfig = new HdfsConfiguration();
    hdfsCluster
        = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(1).build();
    fs = hdfsCluster.getFileSystem();
    fc = FileContext.getFileContext(hdfsCluster.getURI(0), config);

    sampleAppIds = new ArrayList<>(CACHE_TEST_CACHE_SIZE + 1);
    for (int i = 0; i < CACHE_TEST_CACHE_SIZE + 1; i++) {
      ApplicationId appId = ApplicationId.fromString(
          ConverterUtils.APPLICATION_PREFIX + "_" + SAMPLE_APP_PREFIX_CACHE_TEST
              + i);
      sampleAppIds.add(appId);
    }
    testActiveDirPath = getTestRootPath("active");
    // Among all sample applicationIds, choose the first one for most of the
    // tests.
    mainTestAppId = sampleAppIds.get(0);
    mainTestAppDirPath = new Path(testActiveDirPath, mainTestAppId.toString());
    mainEntityLogFileName = EntityGroupFSTimelineStore.ENTITY_LOG_PREFIX
          + EntityGroupPlugInForTest.getStandardTimelineGroupId(mainTestAppId);

    testDoneDirPath = getTestRootPath("done");
    config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        testDoneDirPath.toString());
    config.set(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
        testActiveDirPath.toString());
  }

  @Before
  public void setup() throws Exception {
    for (ApplicationId appId : sampleAppIds) {
      Path attemotDirPath =
          new Path(new Path(testActiveDirPath, appId.toString()),
              getAttemptDirName(appId));
      createTestFiles(appId, attemotDirPath);
    }

    store = new EntityGroupFSTimelineStore();
    if (currTestName.getMethodName().contains("Plugin")) {
      rootDir = GenericTestUtils.getTestDir(getClass()
          .getSimpleName());
      if (!rootDir.exists()) {
        rootDir.mkdirs();
      }
      testJar = null;
      testJar = JarFinder.makeClassLoaderTestJar(this.getClass(), rootDir,
          "test-runjar.jar", 2048,
          EntityGroupPlugInForTest.class.getName());
      config.set(
          YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSPATH,
          testJar.getAbsolutePath());
      // add "-org.apache.hadoop." as system classes
      String systemClasses = "-org.apache.hadoop." + "," +
          ApplicationClassLoader.SYSTEM_CLASSES_DEFAULT;
      config.set(
          YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_SYSTEM_CLASSES,
          systemClasses);

      config.set(YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES,
          EntityGroupPlugInForTest.class.getName());
    }
    store.init(config);
    store.setFs(fs);
    store.start();
  }

  @After
  public void tearDown() throws Exception {
    store.stop();
    for (ApplicationId appId : sampleAppIds) {
      fs.delete(new Path(testActiveDirPath,appId.toString()), true);
    }
    if (testJar != null) {
      testJar.delete();
      rootDir.delete();
    }
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
        store.new AppLogs(mainTestAppId, mainTestAppDirPath,
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
      assertEquals(fileName, mainEntityLogFileName);
    }
  }

  @Test
  public void testMoveToDone() throws Exception {
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(mainTestAppId, mainTestAppDirPath,
        AppState.COMPLETED);
    Path pathBefore = appLogs.getAppDirPath();
    appLogs.moveToDone();
    Path pathAfter = appLogs.getAppDirPath();
    assertNotEquals(pathBefore, pathAfter);
    assertTrue(pathAfter.toString().contains(testDoneDirPath.toString()));

    fs.delete(pathAfter, true);
  }

  @Test
  public void testParseSummaryLogs() throws Exception {
    TimelineDataManager tdm = PluginStoreTestUtils.getTdmWithMemStore(config);
    MutableCounterLong scanned = store.metrics.getEntitiesReadToSummary();
    long beforeScan = scanned.value();
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(mainTestAppId, mainTestAppDirPath,
        AppState.COMPLETED);
    appLogs.scanForLogs();
    appLogs.parseSummaryLogs(tdm);
    PluginStoreTestUtils.verifyTestEntities(tdm);
    assertEquals(beforeScan + 2L, scanned.value());
  }

  @Test
  public void testCleanLogs() throws Exception {
    // Create test dirs and files
    // Irrelevant file, should not be reclaimed
    String appDirName = mainTestAppId.toString();
    String attemptDirName = ApplicationAttemptId.appAttemptIdStrPrefix
        + appDirName + "_1";
    Path irrelevantFilePath = new Path(
            testDoneDirPath, "irrelevant.log");
    FSDataOutputStream stream = fs.create(irrelevantFilePath);
    stream.close();
    // Irrelevant directory, should not be reclaimed
    Path irrelevantDirPath = new Path(testDoneDirPath, "irrelevant");
    fs.mkdirs(irrelevantDirPath);

    Path doneAppHomeDir = new Path(new Path(testDoneDirPath, "0000"), "001");
    // First application, untouched after creation
    Path appDirClean = new Path(doneAppHomeDir, appDirName);
    Path attemptDirClean = new Path(appDirClean, attemptDirName);
    fs.mkdirs(attemptDirClean);
    Path filePath = new Path(attemptDirClean, "test.log");
    stream = fs.create(filePath);
    stream.close();
    // Second application, one file touched after creation
    Path appDirHoldByFile = new Path(doneAppHomeDir, appDirName + "1");
    Path attemptDirHoldByFile
        = new Path(appDirHoldByFile, attemptDirName);
    fs.mkdirs(attemptDirHoldByFile);
    Path filePathHold = new Path(attemptDirHoldByFile, "test1.log");
    stream = fs.create(filePathHold);
    stream.close();
    // Third application, one dir touched after creation
    Path appDirHoldByDir = new Path(doneAppHomeDir, appDirName + "2");
    Path attemptDirHoldByDir = new Path(appDirHoldByDir, attemptDirName);
    fs.mkdirs(attemptDirHoldByDir);
    Path dirPathHold = new Path(attemptDirHoldByDir, "hold");
    fs.mkdirs(dirPathHold);
    // Fourth application, empty dirs
    Path appDirEmpty = new Path(doneAppHomeDir, appDirName + "3");
    Path attemptDirEmpty = new Path(appDirEmpty, attemptDirName);
    fs.mkdirs(attemptDirEmpty);
    Path dirPathEmpty = new Path(attemptDirEmpty, "empty");
    fs.mkdirs(dirPathEmpty);

    // Should retain all logs after this run
    MutableCounterLong dirsCleaned = store.metrics.getLogsDirsCleaned();
    long before = dirsCleaned.value();
    store.cleanLogs(testDoneDirPath, fs, 10000);
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

    store.cleanLogs(testDoneDirPath, fs, 1000);

    // Verification after the second cleaner call
    assertTrue(fs.exists(irrelevantDirPath));
    assertTrue(fs.exists(irrelevantFilePath));
    assertTrue(fs.exists(filePathHold));
    assertTrue(fs.exists(dirPathHold));
    assertTrue(fs.exists(doneAppHomeDir));

    // appDirClean and appDirEmpty should be cleaned up
    assertFalse(fs.exists(appDirClean));
    assertFalse(fs.exists(appDirEmpty));
    assertEquals(before + 2L, dirsCleaned.value());
  }

  @Test
  public void testPluginRead() throws Exception {
    // Verify precondition
    assertEquals(EntityGroupPlugInForTest.class.getName(),
        store.getConfig().get(
            YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES));
    List<TimelineEntityGroupPlugin> currPlugins = store.getPlugins();
    for (TimelineEntityGroupPlugin plugin : currPlugins) {
      ClassLoader pluginClassLoader = plugin.getClass().getClassLoader();
      assertTrue("Should set up ApplicationClassLoader",
          pluginClassLoader instanceof ApplicationClassLoader);
      URL[] paths = ((URLClassLoader) pluginClassLoader).getURLs();
      boolean foundJAR = false;
      for (URL path : paths) {
        if (path.toString().contains(testJar.getAbsolutePath())) {
          foundJAR = true;
        }
      }
      assertTrue("Not found path " + testJar.getAbsolutePath()
          + " for plugin " + plugin.getClass().getName(), foundJAR);
    }
    // Load data and cache item, prepare timeline store by making a cache item
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(mainTestAppId, mainTestAppDirPath,
        AppState.COMPLETED);
    EntityCacheItem cacheItem = new EntityCacheItem(
        EntityGroupPlugInForTest.getStandardTimelineGroupId(mainTestAppId),
        config);
    cacheItem.setAppLogs(appLogs);
    store.setCachedLogs(
        EntityGroupPlugInForTest.getStandardTimelineGroupId(mainTestAppId),
        cacheItem);
    MutableCounterLong detailLogEntityRead =
        store.metrics.getGetEntityToDetailOps();
    MutableStat cacheRefresh = store.metrics.getCacheRefresh();
    long numEntityReadBefore = detailLogEntityRead.value();
    long cacheRefreshBefore = cacheRefresh.lastStat().numSamples();

    // Generate TDM
    TimelineDataManager tdm
        = PluginStoreTestUtils.getTdmWithStore(config, store);

    // Verify single entity read
    TimelineEntity entity3 = tdm.getEntity("type_3", mainTestAppId.toString(),
        EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertNotNull(entity3);
    assertEquals(entityNew.getStartTime(), entity3.getStartTime());
    // Verify multiple entities read
    NameValuePair primaryFilter = new NameValuePair(
        EntityGroupPlugInForTest.APP_ID_FILTER_NAME, mainTestAppId.toString());
    TimelineEntities entities = tdm.getEntities("type_3", primaryFilter, null,
        null, null, null, null, null, EnumSet.allOf(TimelineReader.Field.class),
        UserGroupInformation.getLoginUser());
    assertEquals(1, entities.getEntities().size());
    for (TimelineEntity entity : entities.getEntities()) {
      assertEquals(entityNew.getStartTime(), entity.getStartTime());
    }
    // Verify metrics
    assertEquals(numEntityReadBefore + 2L, detailLogEntityRead.value());
    assertEquals(cacheRefreshBefore + 1L, cacheRefresh.lastStat().numSamples());
  }

  @Test
  public void testSummaryRead() throws Exception {
    // Load data
    EntityGroupFSTimelineStore.AppLogs appLogs =
        store.new AppLogs(mainTestAppId, mainTestAppDirPath,
        AppState.COMPLETED);
    MutableCounterLong summaryLogEntityRead
        = store.metrics.getGetEntityToSummaryOps();
    long numEntityReadBefore = summaryLogEntityRead.value();
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
      assertEquals((Long) 123L, entity.getStartTime());
    }
    // Verify metrics
    assertEquals(numEntityReadBefore + 5L, summaryLogEntityRead.value());

  }

  @Test
  public void testGetEntityPluginRead() throws Exception {
    EntityGroupFSTimelineStore store = null;
    ApplicationId appId =
        ApplicationId.fromString("application_1501509265053_0001");
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userBase = new Path(testActiveDirPath, user);
    Path userAppRoot = new Path(userBase, appId.toString());
    Path attemotDirPath = new Path(userAppRoot, getAttemptDirName(appId));

    try {
      store = createAndStartTimelineStore(AppState.ACTIVE);
      String logFileName = EntityGroupFSTimelineStore.ENTITY_LOG_PREFIX
          + EntityGroupPlugInForTest.getStandardTimelineGroupId(appId);
      createTestFiles(appId, attemotDirPath, logFileName);
      TimelineEntity entity = store.getEntity(entityNew.getEntityId(),
          entityNew.getEntityType(), EnumSet.allOf(Field.class));
      assertNotNull(entity);
      assertEquals(entityNew.getEntityId(), entity.getEntityId());
      assertEquals(entityNew.getEntityType(), entity.getEntityType());
    } finally {
      if (store != null) {
        store.stop();
      }
      fs.delete(userBase, true);
    }
  }

  @Test
  public void testScanActiveLogsAndMoveToDonePluginRead() throws Exception {
    EntityGroupFSTimelineStore store = null;
    ApplicationId appId =
        ApplicationId.fromString("application_1501509265053_0002");
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userBase = new Path(testActiveDirPath, user);
    Path userAppRoot = new Path(userBase, appId.toString());
    Path attemotDirPath = new Path(userAppRoot, getAttemptDirName(appId));

    try {
      store = createAndStartTimelineStore(AppState.COMPLETED);
      String logFileName = EntityGroupFSTimelineStore.ENTITY_LOG_PREFIX
          + EntityGroupPlugInForTest.getStandardTimelineGroupId(appId);
      createTestFiles(appId, attemotDirPath, logFileName);
      store.scanActiveLogs();

      TimelineEntity entity = store.getEntity(entityNew.getEntityId(),
          entityNew.getEntityType(), EnumSet.allOf(Field.class));
      assertNotNull(entity);
      assertEquals(entityNew.getEntityId(), entity.getEntityId());
      assertEquals(entityNew.getEntityType(), entity.getEntityType());
    } finally {
      if (store != null) {
        store.stop();
      }
      fs.delete(userBase, true);
    }
  }

  private EntityGroupFSTimelineStore createAndStartTimelineStore(
      AppState appstate) {
    // stop before creating new store to get the lock
    store.stop();

    EntityGroupFSTimelineStore newStore = new EntityGroupFSTimelineStore() {
      @Override
      protected AppState getAppState(ApplicationId appId) throws IOException {
        return appstate;
      }
    };
    newStore.init(config);
    newStore.setFs(fs);
    newStore.start();
    return newStore;
  }

  private void createTestFiles(ApplicationId appId, Path attemptDirPath)
      throws IOException {
    createTestFiles(appId, attemptDirPath, mainEntityLogFileName);
  }

  private void createTestFiles(ApplicationId appId, Path attemptDirPath,
      String logPath) throws IOException {
    TimelineEntities entities = PluginStoreTestUtils.generateTestEntities();
    PluginStoreTestUtils.writeEntities(entities,
        new Path(attemptDirPath, TEST_SUMMARY_LOG_FILE_NAME), fs);
    Map<String, Set<Object>> primaryFilters = new HashMap<>();
    Set<Object> appSet = new HashSet<Object>();
    appSet.add(appId.toString());
    primaryFilters.put(EntityGroupPlugInForTest.APP_ID_FILTER_NAME, appSet);
    entityNew = PluginStoreTestUtils
        .createEntity(appId.toString(), "type_3", 789L, null, null,
            primaryFilters, null, "domain_id_1");
    TimelineEntities entityList = new TimelineEntities();
    entityList.addEntity(entityNew);
    PluginStoreTestUtils.writeEntities(entityList,
        new Path(attemptDirPath, logPath), fs);

    FSDataOutputStream out = fs.create(
        new Path(attemptDirPath, TEST_DOMAIN_LOG_FILE_NAME));
    out.close();
  }

  private static Path getTestRootPath(String pathString) {
    return fileContextTestHelper.getTestRootPath(fc, pathString);
  }

  private static String getAttemptDirName(ApplicationId appId) {
    return ApplicationAttemptId.appAttemptIdStrPrefix + appId.toString() + "_1";
  }
}
