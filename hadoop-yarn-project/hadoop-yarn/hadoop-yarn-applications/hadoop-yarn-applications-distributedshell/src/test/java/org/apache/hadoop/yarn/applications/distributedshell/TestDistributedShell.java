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

package org.apache.hadoop.yarn.applications.distributedshell;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.function.Supplier;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEvent;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.DirectTimelineWriter;
import org.apache.hadoop.yarn.client.api.impl.TestTimelineClient;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.client.api.impl.TimelineWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.PluginStoreTestUtils;
import org.apache.hadoop.yarn.server.timeline.TimelineVersion;
import org.apache.hadoop.yarn.server.timeline.TimelineVersionWatcher;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDistributedShell {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDistributedShell.class);

  protected MiniYARNCluster yarnCluster = null;
  protected MiniDFSCluster hdfsCluster = null;
  private FileSystem fs = null;
  private TimelineWriter spyTimelineWriter;
  protected YarnConfiguration conf = null;
  // location of the filesystem timeline writer for timeline service v.2
  private String timelineV2StorageDir = null;
  private static final int NUM_NMS = 1;
  private static final float DEFAULT_TIMELINE_VERSION = 1.0f;
  private static final String TIMELINE_AUX_SERVICE_NAME = "timeline_collector";
  private static final int MIN_ALLOCATION_MB = 128;
  private static final int TEST_TIME_OUT = 150000;
  // set the timeout of the yarnClient to be 95% of the globalTimeout.
  private static final int TEST_TIME_WINDOW_EXPIRE = (TEST_TIME_OUT * 90) / 100;

  protected final static String APPMASTER_JAR =
      JarFinder.getJar(ApplicationMaster.class);

  @Rule
  public TimelineVersionWatcher timelineVersionWatcher
      = new TimelineVersionWatcher();

  @Rule
  public Timeout globalTimeout = new Timeout(TEST_TIME_OUT,
      TimeUnit.MILLISECONDS);

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public TestName name = new TestName();

  // set the timeout of the yarnClient to be 95% of the globalTimeout.
  private final String yarnClientTimeout =
      String.valueOf(TEST_TIME_WINDOW_EXPIRE);

  private final String[] commonArgs = {
      "--jar",
      APPMASTER_JAR,
      "--timeout",
      yarnClientTimeout,
      "--appname",
      ""
  };

  @Before
  public void setup() throws Exception {
    setupInternal(NUM_NMS, timelineVersionWatcher.getTimelineVersion());
  }

  protected void setupInternal(int numNodeManager) throws Exception {
    setupInternal(numNodeManager, DEFAULT_TIMELINE_VERSION);
  }

  private void setupInternal(int numNodeManager, float timelineVersion)
      throws Exception {
    LOG.info("Starting up YARN cluster");

    conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        MIN_ALLOCATION_MB);
    // reduce the teardown waiting time
    conf.setLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT, 1000);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 500);
    conf.set("yarn.log.dir", "target");
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    // mark if we need to launch the v1 timeline server
    // disable aux-service based timeline aggregators
    conf.set(YarnConfiguration.NM_AUX_SERVICES, "");
    conf.setBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED, true);

    conf.set(YarnConfiguration.NM_VMEM_PMEM_RATIO, "8");
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set("mapreduce.jobhistory.address",
        "0.0.0.0:" + ServerSocketUtil.getPort(10021, 10));
    // Enable ContainersMonitorImpl
    conf.set(YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class.getName());
    conf.set(YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
        ProcfsBasedProcessTree.class.getName());
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, true);
    conf.setBoolean(
        YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
        true);
    conf.setBoolean(YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_ENABLED,
          true);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        10);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    // ATS version specific settings
    if (timelineVersion == 1.0f) {
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.0f);
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
          CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
    } else if (timelineVersion == 1.5f) {
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(1).build();
      hdfsCluster.waitActive();
      fs = hdfsCluster.getFileSystem();
      PluginStoreTestUtils.prepareFileSystemForPluginStore(fs);
      PluginStoreTestUtils.prepareConfiguration(conf, hdfsCluster);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES,
          DistributedShellTimelinePlugin.class.getName());
    } else if (timelineVersion == 2.0f) {
      // set version to 2
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      // disable v1 timeline server since we no longer have a server here
      // enable aux-service based timeline aggregators
      conf.set(YarnConfiguration.NM_AUX_SERVICES, TIMELINE_AUX_SERVICE_NAME);
      conf.set(YarnConfiguration.NM_AUX_SERVICES + "." +
          TIMELINE_AUX_SERVICE_NAME + ".class",
          PerNodeTimelineCollectorsAuxService.class.getName());
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
          FileSystemTimelineWriterImpl.class,
          org.apache.hadoop.yarn.server.timelineservice.storage.
              TimelineWriter.class);
      timelineV2StorageDir = tmpFolder.newFolder().getAbsolutePath();
      // set the file system timeline writer storage directory
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          timelineV2StorageDir);
    } else {
      Assert.fail("Wrong timeline version number: " + timelineVersion);
    }

    yarnCluster =
        new MiniYARNCluster(TestDistributedShell.class.getSimpleName(), 1,
            numNodeManager, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    conf.set(
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
        MiniYARNCluster.getHostname() + ":"
            + yarnCluster.getApplicationHistoryServer().getPort());

    waitForNMsToRegister();

    URL url = Thread.currentThread().getContextClassLoader().getResource(
        "yarn-site.xml");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'yarn-site.xml' dummy file in classpath");
    }
    Configuration yarnClusterConfig = yarnCluster.getConfig();
    yarnClusterConfig.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        new File(url.getPath()).getParent());
    //write the document to a buffer (not directly to the file, as that
    //can cause the file being written to get read -which will then fail.
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    yarnClusterConfig.writeXml(bytesOut);
    bytesOut.close();
    //write the bytes to the file in the classpath
    OutputStream os = new FileOutputStream(url.getPath());
    os.write(bytesOut.toByteArray());
    os.close();

    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH)),
            true);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @After
  public void tearDown() throws IOException {
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH)),
            true);
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
    if (hdfsCluster != null) {
      try {
        hdfsCluster.shutdown();
      } finally {
        hdfsCluster = null;
      }
    }
  }

  @Test
  public void testDSShellWithDomain() throws Exception {
    testDSShell(true);
  }

  @Test
  public void testDSShellWithoutDomain() throws Exception {
    testDSShell(false);
  }

  @Test
  @TimelineVersion(1.5f)
  public void testDSShellWithoutDomainV1_5() throws Exception {
    testDSShell(false);
  }

  @Test
  @TimelineVersion(1.5f)
  public void testDSShellWithDomainV1_5() throws Exception {
    testDSShell(true);
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDSShellWithoutDomainV2() throws Exception {
    testDSShell(false);
  }

  public void testDSShell(boolean haveDomain) throws Exception {
    testDSShell(haveDomain, true);
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDSShellWithoutDomainV2DefaultFlow() throws Exception {
    testDSShell(false, true);
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDSShellWithoutDomainV2CustomizedFlow() throws Exception {
    testDSShell(false, false);
  }

  public void testDSShell(boolean haveDomain, boolean defaultFlow)
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1");

    if (haveDomain) {
      String[] domainArgs = {
          "--domain",
          "TEST_DOMAIN",
          "--view_acls",
          "reader_user reader_group",
          "--modify_acls",
          "writer_user writer_group",
          "--create"
      };
      args = mergeArgs(args, domainArgs);
    }
    boolean isTestingTimelineV2 = false;
    if (timelineVersionWatcher.getTimelineVersion() == 2.0f) {
      isTestingTimelineV2 = true;
      if (!defaultFlow) {
        String[] flowArgs = {
            "--flow_name",
            "test_flow_name",
            "--flow_version",
            "test_flow_version",
            "--flow_run_id",
            "12345678"
        };
        args = mergeArgs(args, flowArgs);
      }
      LOG.info("Setup: Using timeline v2!");
    }

    LOG.info("Initializing DS Client");
    YarnClient yarnClient;
    final Client client = new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    final AtomicBoolean result = new AtomicBoolean(false);
    Thread t = new Thread() {
      public void run() {
        try {
          result.set(client.run());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnCluster.getConfig()));
    yarnClient.start();

    boolean verified = false;
    String errorMessage = "";
    ApplicationId appId = null;
    ApplicationReport appReport = null;
    while (!verified) {
      List<ApplicationReport> apps = yarnClient.getApplications();
      if (apps.size() == 0) {
        Thread.sleep(10);
        continue;
      }
      appReport = apps.get(0);
      appId = appReport.getApplicationId();
      if (appReport.getHost().equals("N/A")) {
        Thread.sleep(10);
        continue;
      }
      errorMessage =
          "'. Expected rpc port to be '-1', was '"
              + appReport.getRpcPort() + "'.";
      if (appReport.getRpcPort() == -1) {
        verified = true;
      }

      if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
          && appReport.getFinalApplicationStatus() !=
          FinalApplicationStatus.UNDEFINED) {
        break;
      }
    }
    Assert.assertTrue(errorMessage, verified);
    t.join();
    LOG.info("Client run completed for testDSShell. Result=" + result);
    Assert.assertTrue(result.get());

    if (timelineVersionWatcher.getTimelineVersion() == 1.5f) {
      long scanInterval = conf.getLong(
          YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS,
          YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS_DEFAULT
      );
      Path doneDir = new Path(
          YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT
      );
      // Wait till the data is moved to done dir, or timeout and fail
      while (true) {
        RemoteIterator<FileStatus> iterApps = fs.listStatusIterator(doneDir);
        if (iterApps.hasNext()) {
          break;
        }
        Thread.sleep(scanInterval * 2);
      }
    }

    if (!isTestingTimelineV2) {
      checkTimelineV1(haveDomain);
    } else {
      checkTimelineV2(appId, defaultFlow, appReport);
    }
  }

  private void checkTimelineV1(boolean haveDomain) throws Exception {
    TimelineDomain domain = null;
    if (haveDomain) {
      domain = yarnCluster.getApplicationHistoryServer()
          .getTimelineStore().getDomain("TEST_DOMAIN");
      Assert.assertNotNull(domain);
      Assert.assertEquals("reader_user reader_group", domain.getReaders());
      Assert.assertEquals("writer_user writer_group", domain.getWriters());
    }
    TimelineEntities entitiesAttempts = yarnCluster
        .getApplicationHistoryServer()
        .getTimelineStore()
        .getEntities(ApplicationMaster.DSEntity.DS_APP_ATTEMPT.toString(),
            null, null, null, null, null, null, null, null, null);
    Assert.assertNotNull(entitiesAttempts);
    Assert.assertEquals(1, entitiesAttempts.getEntities().size());
    Assert.assertEquals(2, entitiesAttempts.getEntities().get(0).getEvents()
        .size());
    Assert.assertEquals(entitiesAttempts.getEntities().get(0).getEntityType(),
        ApplicationMaster.DSEntity.DS_APP_ATTEMPT.toString());
    if (haveDomain) {
      Assert.assertEquals(domain.getId(),
          entitiesAttempts.getEntities().get(0).getDomainId());
    } else {
      Assert.assertEquals("DEFAULT",
          entitiesAttempts.getEntities().get(0).getDomainId());
    }
    String currAttemptEntityId
        = entitiesAttempts.getEntities().get(0).getEntityId();
    ApplicationAttemptId attemptId = ApplicationAttemptId.fromString(
        currAttemptEntityId);
    NameValuePair primaryFilter = new NameValuePair(
        ApplicationMaster.APPID_TIMELINE_FILTER_NAME,
        attemptId.getApplicationId().toString());
    TimelineEntities entities = yarnCluster
        .getApplicationHistoryServer()
        .getTimelineStore()
        .getEntities(ApplicationMaster.DSEntity.DS_CONTAINER.toString(), null,
            null, null, null, null, primaryFilter, null, null, null);
    Assert.assertNotNull(entities);
    Assert.assertEquals(2, entities.getEntities().size());
    Assert.assertEquals(entities.getEntities().get(0).getEntityType(),
        ApplicationMaster.DSEntity.DS_CONTAINER.toString());

    String entityId = entities.getEntities().get(0).getEntityId();
    org.apache.hadoop.yarn.api.records.timeline.TimelineEntity entity =
        yarnCluster.getApplicationHistoryServer().getTimelineStore()
            .getEntity(entityId,
                ApplicationMaster.DSEntity.DS_CONTAINER.toString(), null);
    Assert.assertNotNull(entity);
    Assert.assertEquals(entityId, entity.getEntityId());

    if (haveDomain) {
      Assert.assertEquals(domain.getId(),
          entities.getEntities().get(0).getDomainId());
    } else {
      Assert.assertEquals("DEFAULT",
          entities.getEntities().get(0).getDomainId());
    }
  }

  private void checkTimelineV2(ApplicationId appId,
      boolean defaultFlow, ApplicationReport appReport) throws Exception {
    LOG.info("Started checkTimelineV2 ");
    // For PoC check using the file-based timeline writer (YARN-3264)
    String tmpRoot = timelineV2StorageDir + File.separator + "entities" +
        File.separator;

    File tmpRootFolder = new File(tmpRoot);
    try {
      Assert.assertTrue(tmpRootFolder.isDirectory());
      String basePath = tmpRoot +
          YarnConfiguration.DEFAULT_RM_CLUSTER_ID + File.separator +
          UserGroupInformation.getCurrentUser().getShortUserName() +
          (defaultFlow ?
              File.separator + appReport.getName() + File.separator +
                  TimelineUtils.DEFAULT_FLOW_VERSION + File.separator +
                  appReport.getStartTime() + File.separator :
              File.separator + "test_flow_name" + File.separator +
                  "test_flow_version" + File.separator + "12345678" +
                  File.separator) +
          appId.toString();
      LOG.info("basePath: " + basePath);
      // for this test, we expect DS_APP_ATTEMPT AND DS_CONTAINER dirs

      // Verify DS_APP_ATTEMPT entities posted by the client
      // there will be at least one attempt, look for that file
      String appTimestampFileName =
          "appattempt_" + appId.getClusterTimestamp() + "_000" + appId.getId()
              + "_000001"
              + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      File dsAppAttemptEntityFile = verifyEntityTypeFileExists(basePath,
          "DS_APP_ATTEMPT", appTimestampFileName);
      // Check if required events are published and same idprefix is sent for
      // on each publish.
      verifyEntityForTimelineV2(dsAppAttemptEntityFile,
          DSEvent.DS_APP_ATTEMPT_START.toString(), 1, 1, 0, true);
      // to avoid race condition of testcase, atleast check 40 times with sleep
      // of 50ms
      verifyEntityForTimelineV2(dsAppAttemptEntityFile,
          DSEvent.DS_APP_ATTEMPT_END.toString(), 1, 40, 50, true);

      // Verify DS_CONTAINER entities posted by the client.
      String containerTimestampFileName =
          "container_" + appId.getClusterTimestamp() + "_000" + appId.getId()
              + "_01_000002.thist";
      File dsContainerEntityFile = verifyEntityTypeFileExists(basePath,
          "DS_CONTAINER", containerTimestampFileName);
      // Check if required events are published and same idprefix is sent for
      // on each publish.
      verifyEntityForTimelineV2(dsContainerEntityFile,
          DSEvent.DS_CONTAINER_START.toString(), 1, 1, 0, true);
      // to avoid race condition of testcase, atleast check 40 times with sleep
      // of 50ms
      verifyEntityForTimelineV2(dsContainerEntityFile,
          DSEvent.DS_CONTAINER_END.toString(), 1, 40, 50, true);

      // Verify NM posting container metrics info.
      String containerMetricsTimestampFileName =
          "container_" + appId.getClusterTimestamp() + "_000" + appId.getId()
              + "_01_000001"
              + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      File containerEntityFile = verifyEntityTypeFileExists(basePath,
          TimelineEntityType.YARN_CONTAINER.toString(),
          containerMetricsTimestampFileName);
      verifyEntityForTimelineV2(containerEntityFile,
          ContainerMetricsConstants.CREATED_EVENT_TYPE, 1, 1, 0, true);

      // to avoid race condition of testcase, atleast check 40 times with sleep
      // of 50ms
      verifyEntityForTimelineV2(containerEntityFile,
          ContainerMetricsConstants.FINISHED_EVENT_TYPE, 1, 40, 50, true);

      // Verify RM posting Application life cycle Events are getting published
      String appMetricsTimestampFileName =
          "application_" + appId.getClusterTimestamp() + "_000" + appId.getId()
              + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      File appEntityFile =
          verifyEntityTypeFileExists(basePath,
              TimelineEntityType.YARN_APPLICATION.toString(),
              appMetricsTimestampFileName);
      // No need to check idprefix for app.
      verifyEntityForTimelineV2(appEntityFile,
          ApplicationMetricsConstants.CREATED_EVENT_TYPE, 1, 1, 0, false);

      // to avoid race condition of testcase, atleast check 40 times with sleep
      // of 50ms
      verifyEntityForTimelineV2(appEntityFile,
          ApplicationMetricsConstants.FINISHED_EVENT_TYPE, 1, 40, 50, false);

      // Verify RM posting AppAttempt life cycle Events are getting published
      String appAttemptMetricsTimestampFileName =
          "appattempt_" + appId.getClusterTimestamp() + "_000" + appId.getId()
              + "_000001"
              + FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION;
      File appAttemptEntityFile =
          verifyEntityTypeFileExists(basePath,
              TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
              appAttemptMetricsTimestampFileName);
      verifyEntityForTimelineV2(appAttemptEntityFile,
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE, 1, 1, 0, true);
      verifyEntityForTimelineV2(appAttemptEntityFile,
          AppAttemptMetricsConstants.FINISHED_EVENT_TYPE, 1, 1, 0, true);
    } finally {
      try {
        FileUtils.deleteDirectory(tmpRootFolder.getParentFile());
      } catch (FileNotFoundException ex) {
        // the recursive delete can throw an exception when one of the file
        // does not exist.
        LOG.warn("Exception deleting a file/subDirectory: {}", ex.getMessage());
      }
    }
  }

  private File verifyEntityTypeFileExists(String basePath, String entityType,
      String entityfileName) {
    String outputDirPathForEntity =
        basePath + File.separator + entityType + File.separator;
    LOG.info(outputDirPathForEntity);
    File outputDirForEntity = new File(outputDirPathForEntity);
    Assert.assertTrue(outputDirForEntity.isDirectory());

    String entityFilePath = outputDirPathForEntity + entityfileName;

    File entityFile = new File(entityFilePath);
    Assert.assertTrue(entityFile.exists());
    return entityFile;
  }

  /**
   * Checks the events and idprefix published for an entity.
   *
   * @param entityFile Entity file.
   * @param expectedEvent Expected event Id.
   * @param numOfExpectedEvent Number of expected occurences of expected event
   *     id.
   * @param checkTimes Number of times to check.
   * @param sleepTime Sleep time for each iteration.
   * @param checkIdPrefix Whether to check idprefix.
   * @throws IOException if entity file reading fails.
   * @throws InterruptedException if sleep is interrupted.
   */
  private void verifyEntityForTimelineV2(File entityFile, String expectedEvent,
      long numOfExpectedEvent, int checkTimes, long sleepTime,
      boolean checkIdPrefix) throws IOException, InterruptedException {
    long actualCount = 0;
    for (int i = 0; i < checkTimes; i++) {
      BufferedReader reader = null;
      String strLine;
      actualCount = 0;
      try {
        reader = new BufferedReader(new FileReader(entityFile));
        long idPrefix = -1;
        while ((strLine = reader.readLine()) != null) {
          String entityLine = strLine.trim();
          if (entityLine.isEmpty()) {
            continue;
          }
          if (entityLine.contains(expectedEvent)) {
            actualCount++;
          }
          if (expectedEvent.equals(DSEvent.DS_CONTAINER_END.toString()) &&
              entityLine.contains(expectedEvent)) {
            TimelineEntity entity = FileSystemTimelineReaderImpl.
                getTimelineRecordFromJSON(entityLine, TimelineEntity.class);
            TimelineEvent event = entity.getEvents().pollFirst();
            Assert.assertNotNull(event);
            Assert.assertTrue("diagnostics",
                event.getInfo().containsKey(ApplicationMaster.DIAGNOSTICS));
          }
          if (checkIdPrefix) {
            TimelineEntity entity = FileSystemTimelineReaderImpl.
                getTimelineRecordFromJSON(entityLine, TimelineEntity.class);
            Assert.assertTrue("Entity ID prefix expected to be > 0",
                entity.getIdPrefix() > 0);
            if (idPrefix == -1) {
              idPrefix = entity.getIdPrefix();
            } else {
              Assert.assertEquals("Entity ID prefix should be same across " +
                  "each publish of same entity",
                      idPrefix, entity.getIdPrefix());
            }
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
      if (numOfExpectedEvent == actualCount) {
        break;
      }
      if (sleepTime > 0 && i < checkTimes - 1) {
        Thread.sleep(sleepTime);
      }
    }
    Assert.assertEquals("Unexpected number of " +  expectedEvent +
        " event published.", numOfExpectedEvent, actualCount);
  }

  /**
   * Utility function to merge two String arrays to form a new String array for
   * our argumemts.
   *
   * @param args the first set of the arguments.
   * @param newArgs the second set of the arguments.
   * @return a String array consists of {args, newArgs}
   */
  private String[] mergeArgs(String[] args, String[] newArgs) {
    int length = args.length + newArgs.length;
    String[] result = new String[length];
    System.arraycopy(args, 0, result, 0, args.length);
    System.arraycopy(newArgs, 0, result, args.length, newArgs.length);
    return result;
  }

  private String generateAppName(String postFix) {
    return name.getMethodName().replaceFirst("test", "")
        .concat(postFix == null? "" : "-" + postFix);
  }

  private String[] createArguments(String... args) {
    String[] res = mergeArgs(commonArgs, args);
    // set the application name so we can track down which command is running.
    res[commonArgs.length - 1] = generateAppName(null);
    return res;
  }

  private String[] createArgsWithPostFix(int index, String... args) {
    String[] res = mergeArgs(commonArgs, args);
    // set the application name so we can track down which command is running.
    res[commonArgs.length - 1] = generateAppName(String.valueOf(index));
    return res;
  }

  protected String getSleepCommand(int sec) {
    // Windows doesn't have a sleep command, ping -n does the trick
    return Shell.WINDOWS ? "ping -n " + (sec + 1) + " 127.0.0.1 >nul"
        : "sleep " + sec;
  }

  @Test
  public void testDSRestartWithPreviousRunningContainers() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        getSleepCommand(8),
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--keep_containers_across_application_attempts"
    );

    LOG.info("Initializing DS Client");
    Client client = new Client(TestDSFailedAppMaster.class.getName(),
        new Configuration(yarnCluster.getConfig()));

    client.init(args);

    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    // application should succeed
    Assert.assertTrue(result);
  }

  /*
   * The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
   * Set attempt_failures_validity_interval as 2.5 seconds. It will check
   * how many attempt failures for previous 2.5 seconds.
   * The application is expected to be successful.
   */
  @Test
  public void testDSAttemptFailuresValidityIntervalSucess() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        getSleepCommand(8),
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--attempt_failures_validity_interval",
        "2500"
    );

    LOG.info("Initializing DS Client");
    Configuration config = yarnCluster.getConfig();
    config.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    Client client = new Client(TestDSSleepingAppMaster.class.getName(),
        new Configuration(config));

    client.init(args);

    LOG.info("Running DS Client");
    boolean result = client.run();

    LOG.info("Client run completed. Result=" + result);
    // application should succeed
    Assert.assertTrue(result);
  }

  /*
   * The sleeping period in TestDSSleepingAppMaster is set as 5 seconds.
   * Set attempt_failures_validity_interval as 15 seconds. It will check
   * how many attempt failure for previous 15 seconds.
   * The application is expected to be fail.
   */
  @Test
  public void testDSAttemptFailuresValidityIntervalFailed() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        getSleepCommand(8),
        "--master_memory",
        "512",
        "--container_memory",
        "128",
        "--attempt_failures_validity_interval",
        "15000"
    );

    LOG.info("Initializing DS Client");
    Configuration config = yarnCluster.getConfig();
    config.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    Client client = new Client(TestDSSleepingAppMaster.class.getName(),
        new Configuration(config));

    client.init(args);

    LOG.info("Running DS Client");
    boolean result = client.run();

    LOG.info("Client run completed. Result=" + result);
    // application should be failed
    Assert.assertFalse(result);
  }

  @Test
  public void testDSShellWithCustomLogPropertyFile() throws Exception {
    final File basedir =
        new File("target", TestDistributedShell.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customLogProperty = new File(tmpDir, "custom_log4j.properties");
    if (customLogProperty.exists()) {
      customLogProperty.delete();
    }
    if(!customLogProperty.createNewFile()) {
      Assert.fail("Can not create custom log4j property file.");
    }
    PrintWriter fileWriter = new PrintWriter(customLogProperty);
    // set the output to DEBUG level
    fileWriter.write("log4j.rootLogger=debug,stdout");
    fileWriter.close();
    String[] args = createArguments(
        "--num_containers",
        "3",
        "--shell_command",
        "echo",
        "--shell_args",
        "HADOOP",
        "--log_properties",
        customLogProperty.getAbsolutePath(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    );

    //Before run the DS, the default the log level is INFO
    final Logger LOG_Client =
        LoggerFactory.getLogger(Client.class);
    Assert.assertTrue(LOG_Client.isInfoEnabled());
    Assert.assertFalse(LOG_Client.isDebugEnabled());
    final Logger LOG_AM = LoggerFactory.getLogger(ApplicationMaster.class);
    Assert.assertTrue(LOG_AM.isInfoEnabled());
    Assert.assertFalse(LOG_AM.isDebugEnabled());

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);

    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    Assert.assertTrue(verifyContainerLog(3, null, true, "DEBUG") > 10);
    //After DS is finished, the log level should be DEBUG
    Assert.assertTrue(LOG_Client.isInfoEnabled());
    Assert.assertTrue(LOG_Client.isDebugEnabled());
    Assert.assertTrue(LOG_AM.isInfoEnabled());
    Assert.assertTrue(LOG_AM.isDebugEnabled());
  }

  @Test
  public void testSpecifyingLogAggregationContext() throws Exception {
    String regex = ".*(foo|bar)\\d";
    String[] args = createArguments(
        "--shell_command",
        "echo",
        "--rolling_log_pattern",
        regex
    );
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    Assert.assertTrue(client.init(args));

    ApplicationSubmissionContext context =
        Records.newRecord(ApplicationSubmissionContext.class);
    client.specifyLogAggregationContext(context);
    LogAggregationContext logContext = context.getLogAggregationContext();
    assertEquals(logContext.getRolledLogsIncludePattern(), regex);
    assertTrue(logContext.getRolledLogsExcludePattern().isEmpty());
  }

  public void testDSShellWithCommands() throws Exception {

    String[] args = createArguments(
        "--num_containers",
        "2",
        "--shell_command",
        "\"echo output_ignored;echo output_expected\"",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    );

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    try {
      boolean result = client.run();
      LOG.info("Client run completed. Result=" + result);
      List<String> expectedContent = new ArrayList<>();
      expectedContent.add("output_expected");
      verifyContainerLog(2, expectedContent, false, "");
    } finally {
      client.sendStopSignal();
    }
  }

  @Test
  public void testDSShellWithMultipleArgs() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "4",
        "--shell_command",
        "echo",
        "--shell_args",
        "HADOOP YARN MAPREDUCE HDFS",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    );

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");

    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<>();
    expectedContent.add("HADOOP YARN MAPREDUCE HDFS");
    verifyContainerLog(4, expectedContent, false, "");
  }

  @Test
  public void testDSShellWithShellScript() throws Exception {
    final File basedir =
        new File("target", TestDistributedShell.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customShellScript = new File(tmpDir, "custom_script.sh");
    if (customShellScript.exists()) {
      customShellScript.delete();
    }
    if (!customShellScript.createNewFile()) {
      Assert.fail("Can not create custom shell script file.");
    }
    PrintWriter fileWriter = new PrintWriter(customShellScript);
    // set the output to DEBUG level
    fileWriter.write("echo testDSShellWithShellScript");
    fileWriter.close();
    LOG.info(customShellScript.getAbsolutePath());
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_script",
        customShellScript.getAbsolutePath(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    );

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<>();
    expectedContent.add("testDSShellWithShellScript");
    verifyContainerLog(1, expectedContent, false, "");
  }

  @Test
  public void testDSShellWithInvalidArgs() throws Exception {
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    int appNameCounter = 0;
    LOG.info("Initializing DS Client with no args");
    try {
      client.init(new String[]{});
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No args"));
    }

    LOG.info("Initializing DS Client with no jar file");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      );
      String[] argsNoJar = Arrays.copyOfRange(args, 2, args.length);
      client.init(argsNoJar);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No jar"));
    }

    LOG.info("Initializing DS Client with no shell command");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No shell command"));
    }

    LOG.info("Initializing DS Client with invalid no. of containers");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "-1",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--container_memory",
          "128"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Invalid no. of containers"));
    }
    
    LOG.info("Initializing DS Client with invalid no. of vcores");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--master_vcores",
          "-2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1"
      );
      client.init(args);
      client.run();
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Invalid virtual cores specified"));
    }

    LOG.info("Initializing DS Client with --shell_command and --shell_script");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1",
          "--shell_script",
          "test.sh"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Can not specify shell_command option " +
          "and shell_script option at the same time"));
    }

    LOG.info("Initializing DS Client without --shell_command and --shell_script");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("No shell command or shell script specified " +
          "to be executed by application master"));
    }

    LOG.info("Initializing DS Client with invalid container_type argument");
    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1",
          "--shell_command",
          "date",
          "--container_type",
          "UNSUPPORTED_TYPE"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("The throw exception is not expected",
          e.getMessage().contains("Invalid container_type: UNSUPPORTED_TYPE"));
    }

    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "1",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_resources",
          "memory-mb=invalid"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (IllegalArgumentException e) {
      // do nothing
      LOG.info("IllegalArgumentException exception is expected: {}",
          e.getMessage());
    }

    try {
      String[] args = createArgsWithPostFix(appNameCounter++,
          "--num_containers",
          "1",
          "--shell_command",
          Shell.WINDOWS ? "dir" : "ls",
          "--master_resources"
      );
      client.init(args);
      Assert.fail("Exception is expected");
    } catch (MissingArgumentException e) {
      // do nothing
      LOG.info("MissingArgumentException exception is expected: {}",
          e.getMessage());
    }
  }

  @Test
  public void testDSTimelineClientWithConnectionRefuse() throws Exception {
    ApplicationMaster am = new ApplicationMaster();

    TimelineClientImpl client = new TimelineClientImpl() {
      @Override
      protected TimelineWriter createTimelineWriter(Configuration conf,
          UserGroupInformation authUgi, com.sun.jersey.api.client.Client client,
          URI resURI) throws IOException {
        TimelineWriter timelineWriter =
            new DirectTimelineWriter(authUgi, client, resURI);
        spyTimelineWriter = spy(timelineWriter);
        return spyTimelineWriter;
      }
    };
    client.init(conf);
    client.start();
    TestTimelineClient.mockEntityClientResponse(spyTimelineWriter, null,
        false, true);
    try {
      UserGroupInformation ugi = mock(UserGroupInformation.class);
      when(ugi.getShortUserName()).thenReturn("user1");
      // verify no ClientHandlerException get thrown out.
      am.publishContainerEndEvent(client, ContainerStatus.newInstance(
          BuilderUtils.newContainerId(1, 1, 1, 1), ContainerState.COMPLETE, "",
          1), "domainId", ugi);
    } finally {
      client.stop();
    }
  }

  protected void waitForNMsToRegister() throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        RMContext rmContext = yarnCluster.getResourceManager().getRMContext();
        return (rmContext.getRMNodes().size() >= NUM_NMS);
      }
    }, 100, 60000);
  }

  @Test
  public void testContainerLaunchFailureHandling() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--container_memory",
        "128"
    );

    LOG.info("Initializing DS Client");
    Client client = new Client(ContainerLaunchFailAppMaster.class.getName(),
        new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    try {
      boolean result = client.run();
      Assert.assertFalse(result);
    } finally {
      client.sendStopSignal();
    }
  }

  @Test
  public void testDebugFlag() throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1",
        "--debug"
    );

    LOG.info("Initializing DS Client");
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    Assert.assertTrue(client.init(args));
    LOG.info("Running DS Client");
    Assert.assertTrue(client.run());
  }

  private int verifyContainerLog(int containerNum,
      List<String> expectedContent, boolean count, String expectedWord) {
    File logFolder =
        new File(yarnCluster.getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                YarnConfiguration.DEFAULT_NM_LOG_DIRS));

    File[] listOfFiles = logFolder.listFiles();
    int currentContainerLogFileIndex = -1;
    for (int i = listOfFiles.length - 1; i >= 0; i--) {
      if (listOfFiles[i].listFiles().length == containerNum + 1) {
        currentContainerLogFileIndex = i;
        break;
      }
    }
    Assert.assertTrue(currentContainerLogFileIndex != -1);
    File[] containerFiles =
        listOfFiles[currentContainerLogFileIndex].listFiles();

    int numOfWords = 0;
    for (int i = 0; i < containerFiles.length; i++) {
      for (File output : containerFiles[i].listFiles()) {
        if (output.getName().trim().contains("stdout")) {
          BufferedReader br = null;
          List<String> stdOutContent = new ArrayList<>();
          try {

            String sCurrentLine;
            br = new BufferedReader(new FileReader(output));
            int numOfline = 0;
            while ((sCurrentLine = br.readLine()) != null) {
              if (count) {
                if (sCurrentLine.contains(expectedWord)) {
                  numOfWords++;
                }
              } else if (output.getName().trim().equals("stdout")){
                if (! Shell.WINDOWS) {
                  Assert.assertEquals("The current is" + sCurrentLine,
                      expectedContent.get(numOfline), sCurrentLine.trim());
                  numOfline++;
                } else {
                  stdOutContent.add(sCurrentLine.trim());
                }
              }
            }
            /* By executing bat script using cmd /c,
             * it will output all contents from bat script first
             * It is hard for us to do check line by line
             * Simply check whether output from bat file contains
             * all the expected messages
             */
            if (Shell.WINDOWS && !count
                && output.getName().trim().equals("stdout")) {
              Assert.assertTrue(stdOutContent.containsAll(expectedContent));
            }
          } catch (IOException e) {
            LOG.error("Exception reading the buffer", e);
          } finally {
            try {
              if (br != null)
                br.close();
            } catch (IOException ex) {
              LOG.error("Exception closing the bufferReader", ex);
            }
          }
        }
      }
    }
    return numOfWords;
  }

  @Test
  public void testDistributedShellResourceProfiles() throws Exception {
    int appNameCounter = 0;
    String[][] args = {
        createArgsWithPostFix(appNameCounter++,
            "--num_containers", "1", "--shell_command",
            Shell.WINDOWS ? "dir" : "ls", "--container_resource_profile",
            "maximum"),
        createArgsWithPostFix(appNameCounter++,
            "--num_containers", "1", "--shell_command",
            Shell.WINDOWS ? "dir" : "ls", "--master_resource_profile",
            "default"),
        createArgsWithPostFix(appNameCounter++,
            "--num_containers", "1", "--shell_command",
            Shell.WINDOWS ? "dir" : "ls", "--master_resource_profile",
            "default", "--container_resource_profile", "maximum"),
    };

    for (int i = 0; i < args.length; ++i) {
      LOG.info("Initializing DS Client");
      Client client = new Client(new Configuration(yarnCluster.getConfig()));
      Assert.assertTrue(client.init(args[i]));
      LOG.info("Running DS Client");
      try {
        client.run();
        Assert.fail("Client run should throw error");
      } catch (Exception e) {
        continue;
      }
    }
  }

  @Test
  public void testDSShellWithOpportunisticContainers() throws Exception {
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    try {
      String[] args = createArguments(
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1",
          "--shell_command",
          "date",
          "--container_type",
          "OPPORTUNISTIC"
      );
      client.init(args);
      assertTrue(client.run());
    } catch (Exception e) {
      LOG.error("Job execution with opportunistic containers failed.", e);
      Assert.fail("Exception. " + e.getMessage());
    } finally {
      client.sendStopSignal();
    }
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDSShellWithEnforceExecutionType() throws Exception {
    YarnClient yarnClient = null;
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    try {
      String[] args = createArguments(
          "--num_containers",
          "2",
          "--master_memory",
          "512",
          "--master_vcores",
          "2",
          "--container_memory",
          "128",
          "--container_vcores",
          "1",
          "--shell_command",
          "date",
          "--container_type",
          "OPPORTUNISTIC",
          "--enforce_execution_type"
      );
      client.init(args);
      final AtomicBoolean result = new AtomicBoolean(false);
      Thread t = new Thread() {
        public void run() {
          try {
            result.set(client.run());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      t.start();

      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(new Configuration(yarnCluster.getConfig()));
      yarnClient.start();
      waitForContainersLaunch(yarnClient, 2);
      List<ApplicationReport> apps = yarnClient.getApplications();
      ApplicationReport appReport = apps.get(0);
      ApplicationId appId = appReport.getApplicationId();
      List<ApplicationAttemptReport> appAttempts =
          yarnClient.getApplicationAttempts(appId);
      ApplicationAttemptReport appAttemptReport = appAttempts.get(0);
      ApplicationAttemptId appAttemptId =
          appAttemptReport.getApplicationAttemptId();
      List<ContainerReport> containers =
          yarnClient.getContainers(appAttemptId);
      // we should get two containers.
      Assert.assertEquals(2, containers.size());
      ContainerId amContainerId = appAttemptReport.getAMContainerId();
      for (ContainerReport container : containers) {
        if (!container.getContainerId().equals(amContainerId)) {
          Assert.assertEquals(container.getExecutionType(),
              ExecutionType.OPPORTUNISTIC);
        }
      }
    } catch (Exception e) {
      LOG.error("Job execution with enforce execution type failed.", e);
      Assert.fail("Exception. " + e.getMessage());
    } finally {
      client.sendStopSignal();
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }
  }

  private void waitForContainersLaunch(YarnClient client,
      int nContainers) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        try {
          List<ApplicationReport> apps = client.getApplications();
          if (apps == null || apps.isEmpty()) {
            return false;
          }
          ApplicationId appId = apps.get(0).getApplicationId();
          List<ApplicationAttemptReport> appAttempts =
              client.getApplicationAttempts(appId);
          if (appAttempts == null || appAttempts.isEmpty()) {
            return false;
          }
          ApplicationAttemptId attemptId =
              appAttempts.get(0).getApplicationAttemptId();
          List<ContainerReport> containers = client.getContainers(attemptId);
          return (containers.size() == nContainers);
        } catch (Exception e) {
          return false;
        }
      }
    }, 10, 60000);
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDistributedShellWithResources() throws Exception {
    doTestDistributedShellWithResources(false);
  }

  @Test
  @TimelineVersion(2.0f)
  public void testDistributedShellWithResourcesWithLargeContainers()
      throws Exception {
    doTestDistributedShellWithResources(true);
  }

  public void doTestDistributedShellWithResources(boolean largeContainers)
      throws Exception {
    Resource clusterResource = yarnCluster.getResourceManager()
        .getResourceScheduler().getClusterResource();
    String masterMemoryString = "1 Gi";
    String containerMemoryString = "512 Mi";
    long[] memVars = {1024, 512};

    Assume.assumeTrue("The cluster doesn't have enough memory for this test",
        clusterResource.getMemorySize() >= memVars[0] + memVars[1]);
    Assume.assumeTrue("The cluster doesn't have enough cores for this test",
        clusterResource.getVirtualCores() >= 2);
    if (largeContainers) {
      memVars[0] = clusterResource.getMemorySize() * 2 / 3;
      memVars[0] = memVars[0] - memVars[0] % MIN_ALLOCATION_MB;
      masterMemoryString = memVars[0] + "Mi";
      memVars[1] = clusterResource.getMemorySize() / 3;
      memVars[1] = memVars[1] - memVars[1] % MIN_ALLOCATION_MB;
      containerMemoryString = String.valueOf(memVars[1]);
    }

    String[] args = createArguments(
        "--num_containers",
        "2",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_resources",
        "memory=" + masterMemoryString + ",vcores=1",
        "--container_resources",
        "memory=" + containerMemoryString + ",vcores=1"
    );

    LOG.info("Initializing DS Client");
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    Assert.assertTrue(client.init(args));
    LOG.info("Running DS Client");
    final AtomicBoolean result = new AtomicBoolean(false);
    Thread t = new Thread() {
      public void run() {
        try {
          result.set(client.run());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnCluster.getConfig()));
    yarnClient.start();

    final AtomicBoolean testFailed = new AtomicBoolean(false);
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          if (testFailed.get()) {
            return true;
          }
          List<ContainerReport> containers;
          try {
            List<ApplicationReport> apps = yarnClient.getApplications();
            if (apps.isEmpty()) {
              return false;
            }
            ApplicationReport appReport = apps.get(0);
            ApplicationId appId = appReport.getApplicationId();
            List<ApplicationAttemptReport> appAttempts =
                yarnClient.getApplicationAttempts(appId);
            if (appAttempts.isEmpty()) {
              return false;
            }
            ApplicationAttemptReport appAttemptReport = appAttempts.get(0);
            ContainerId amContainerId = appAttemptReport.getAMContainerId();
            if (amContainerId == null) {
              return false;
            }
            ContainerReport report = yarnClient.getContainerReport(
                amContainerId);
            Resource masterResource = report.getAllocatedResource();
            Assert.assertEquals(memVars[0],
                masterResource.getMemorySize());
            Assert.assertEquals(1, masterResource.getVirtualCores());
            containers = yarnClient.getContainers(
                appAttemptReport.getApplicationAttemptId());
            if (containers.size() < 2) {
              return false;
            }
            for (ContainerReport container : containers) {
              if (!container.getContainerId().equals(amContainerId)) {
                Resource containerResource = container.getAllocatedResource();
                Assert.assertEquals(memVars[1],
                    containerResource.getMemorySize());
                Assert.assertEquals(1, containerResource.getVirtualCores());
              }
            }
            return true;
          } catch (Exception ex) {
            LOG.error("Error waiting for expected results", ex);
            testFailed.set(true);
          }
          return false;
        }
      }, 10, TEST_TIME_WINDOW_EXPIRE);
      assertFalse(testFailed.get());
    } finally {
      LOG.info("Signaling Client to Stop");
      client.sendStopSignal();
      if (yarnClient != null) {
        LOG.info("Stopping yarnClient service");
        yarnClient.stop();
      }
    }
  }

  @Test(expected=ResourceNotFoundException.class)
  public void testDistributedShellAMResourcesWithUnknownResource()
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_resources",
        "unknown-resource=5"
    );
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    client.init(args);
    client.run();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testDistributedShellNonExistentQueue()
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--queue",
        "non-existent-queue"
    );
    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    client.init(args);
    client.run();
  }

  @Test
  public void testDistributedShellWithSingleFileLocalization()
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "type" : "cat",
        "--localize_files",
        "./src/test/resources/a.txt",
        "--shell_args",
        "a.txt"
    );

    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    client.init(args);
    assertTrue("Client exited with an error", client.run());
  }

  @Test
  public void testDistributedShellWithMultiFileLocalization()
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "type" : "cat",
        "--localize_files",
        "./src/test/resources/a.txt,./src/test/resources/b.txt",
        "--shell_args",
        "a.txt b.txt"
    );

    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    client.init(args);
    assertTrue("Client exited with an error", client.run());
  }

  @Test(expected=UncheckedIOException.class)
  public void testDistributedShellWithNonExistentFileLocalization()
      throws Exception {
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "type" : "cat",
        "--localize_files",
        "/non/existing/path/file.txt",
        "--shell_args",
        "file.txt"
    );

    Client client = new Client(new Configuration(yarnCluster.getConfig()));
    client.init(args);
    assertTrue(client.run());
  }


  @Test
  public void testDistributedShellCleanup()
      throws Exception {
    String appName = "DistributedShellCleanup";
    String[] args = createArguments(
        "--num_containers",
        "1",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls"
    );
    Configuration config = new Configuration(yarnCluster.getConfig());
    Client client = new Client(config);
    try {
      client.init(args);
      client.run();
      ApplicationId appId = client.getAppId();
      String relativePath =
          ApplicationMaster.getRelativePath(appName, appId.toString(), "");
      FileSystem fs1 = FileSystem.get(config);
      Path path = new Path(fs1.getHomeDirectory(), relativePath);

      GenericTestUtils.waitFor(() -> {
        try {
          return !fs1.exists(path);
        } catch (IOException e) {
          return false;
        }
      }, 10, 60000);

      assertFalse("Distributed Shell Cleanup failed", fs1.exists(path));
    } finally {
      client.sendStopSignal();
    }
  }
}
