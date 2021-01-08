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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;

/**
 * Base class for testing DistributedShell features.
 */
public abstract class DistributedShellBaseTest {
  protected static final int MIN_ALLOCATION_MB = 128;
  protected static final int NUM_DATA_NODES = 1;
  protected static final int TEST_TIME_OUT = 160000;
  // set the timeout of the yarnClient to be 95% of the globalTimeout.
  protected static final int TEST_TIME_WINDOW_EXPIRE =
      (TEST_TIME_OUT * 90) / 100;
  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedShellBaseTest.class);
  private static final String APP_MASTER_JAR =
      JarFinder.getJar(ApplicationMaster.class);
  private static final int NUM_NMS = 1;
  // set the timeout of the yarnClient to be 95% of the globalTimeout.
  private static final String YARN_CLIENT_TIMEOUT =
      String.valueOf(TEST_TIME_WINDOW_EXPIRE);
  private static final String[] COMMON_ARGS = {
      "--jar",
      APP_MASTER_JAR,
      "--timeout",
      YARN_CLIENT_TIMEOUT,
      "--appname",
      ""
  };
  private static MiniDFSCluster hdfsCluster = null;
  private static MiniYARNCluster yarnCluster = null;
  private static String yarnSiteBackupPath = null;
  private static String yarnSitePath = null;
  @Rule
  public Timeout globalTimeout = new Timeout(TEST_TIME_OUT,
      TimeUnit.MILLISECONDS);
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public TestName name = new TestName();
  private Client dsClient;
  private YarnConfiguration conf = null;
  // location of the filesystem timeline writer for timeline service v.2
  private String timelineV2StorageDir = null;

  @BeforeClass
  public static void setupUnitTests() throws Exception {
    URL url = Thread.currentThread().getContextClassLoader().getResource(
        "yarn-site.xml");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'yarn-site.xml' dummy file in classpath");
    }
    // backup the original yarn-site file.
    yarnSitePath = url.getPath();
    yarnSiteBackupPath = url.getPath() + "-backup";
    Files.copy(Paths.get(yarnSitePath),
        Paths.get(yarnSiteBackupPath),
        StandardCopyOption.COPY_ATTRIBUTES,
        StandardCopyOption.REPLACE_EXISTING);
  }

  @AfterClass
  public static void tearDownUnitTests() throws Exception {
    // shutdown the clusters.
    shutdownYarnCluster();
    shutdownHdfsCluster();
    if (yarnSitePath == null || yarnSiteBackupPath == null) {
      return;
    }
    // restore the original yarn-site file.
    if (Files.exists(Paths.get(yarnSiteBackupPath))) {
      Files.move(Paths.get(yarnSiteBackupPath), Paths.get(yarnSitePath),
          StandardCopyOption.REPLACE_EXISTING);
    }
  }

  /**
   * Utility function to merge two String arrays to form a new String array for
   * our arguments.
   *
   * @param args the first set of the arguments.
   * @param newArgs the second set of the arguments.
   * @return a String array consists of {args, newArgs}
   */
  protected static String[] mergeArgs(String[] args, String[] newArgs) {
    int length = args.length + newArgs.length;
    String[] result = new String[length];
    System.arraycopy(args, 0, result, 0, args.length);
    System.arraycopy(newArgs, 0, result, args.length, newArgs.length);
    return result;
  }

  protected static String[] createArguments(Supplier<String> testNameProvider,
      String... args) {
    String[] res = mergeArgs(COMMON_ARGS, args);
    // set the application name so we can track down which command is running.
    res[COMMON_ARGS.length - 1] = testNameProvider.get();
    return res;
  }

  protected static String getSleepCommand(int sec) {
    // Windows doesn't have a sleep command, ping -n does the trick
    return Shell.WINDOWS ? "ping -n " + (sec + 1) + " 127.0.0.1 >nul"
        : "sleep " + sec;
  }

  protected static String getListCommand() {
    return Shell.WINDOWS ? "dir" : "ls";
  }

  protected static String getCatCommand() {
    return Shell.WINDOWS ? "type" : "cat";
  }

  protected static void shutdownYarnCluster() {
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
  }

  protected static void shutdownHdfsCluster() {
    if (hdfsCluster != null) {
      try {
        hdfsCluster.shutdown();
      } finally {
        hdfsCluster = null;
      }
    }
  }

  public String getTimelineV2StorageDir() {
    return timelineV2StorageDir;
  }

  public void setTimelineV2StorageDir() throws Exception {
    timelineV2StorageDir = tmpFolder.newFolder().getAbsolutePath();
  }

  @Before
  public void setup() throws Exception {
    setupInternal(NUM_NMS, new YarnConfiguration());
  }

  @After
  public void tearDown() throws IOException {
    cleanUpDFSClient();
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH)),
            true);
    shutdownYarnCluster();
    shutdownHdfsCluster();
  }

  protected String[] createArgumentsWithAppName(String... args) {
    return createArguments(() -> generateAppName(), args);
  }

  protected void waitForContainersLaunch(YarnClient client, int nContainers,
      AtomicReference<ApplicationAttemptReport> appAttemptReportRef,
      AtomicReference<List<ContainerReport>> containersListRef,
      AtomicReference<ApplicationAttemptId> appAttemptIdRef,
      AtomicReference<Throwable> thrownErrorRef) throws Exception {
    GenericTestUtils.waitFor(() -> {
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
        if (containers == null || containers.size() < nContainers) {
          return false;
        }
        containersListRef.set(containers);
        appAttemptIdRef.set(attemptId);
        appAttemptReportRef.set(appAttempts.get(0));
      } catch (Exception e) {
        LOG.error("Exception waiting for Containers Launch", e);
        thrownErrorRef.set(e);
      }
      return true;
    }, 10, TEST_TIME_WINDOW_EXPIRE);
  }

  protected abstract void customizeConfiguration(YarnConfiguration config)
      throws Exception;

  protected String[] appendFlowArgsForTestDSShell(String[] args,
      boolean defaultFlow) {
    return args;
  }

  protected String[] appendDomainArgsForTestDSShell(String[] args,
      boolean haveDomain) {
    String[] result = args;
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
      result = mergeArgs(args, domainArgs);
    }
    return result;
  }

  protected Client setAndGetDSClient(Configuration config) throws Exception {
    dsClient = new Client(config);
    return dsClient;
  }

  protected Client setAndGetDSClient(String appMasterMainClass,
      Configuration config) throws Exception {
    dsClient = new Client(appMasterMainClass, config);
    return dsClient;
  }

  protected void baseTestDSShell(boolean haveDomain, boolean defaultFlow)
      throws Exception {
    String[] baseArgs = createArgumentsWithAppName(
        "--num_containers",
        "2",
        "--shell_command",
        getListCommand(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1");
    String[] domainArgs = appendDomainArgsForTestDSShell(baseArgs, haveDomain);
    String[] args = appendFlowArgsForTestDSShell(domainArgs, defaultFlow);

    LOG.info("Initializing DS Client");
    YarnClient yarnClient;
    dsClient = setAndGetDSClient(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = dsClient.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    final AtomicBoolean result = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
      try {
        result.set(dsClient.run());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new Configuration(yarnCluster.getConfig()));
    yarnClient.start();

    AtomicInteger waitResult = new AtomicInteger(0);
    AtomicReference<ApplicationId> appIdRef =
        new AtomicReference<>(null);
    AtomicReference<ApplicationReport> appReportRef =
        new AtomicReference<>(null);
    GenericTestUtils.waitFor(() -> {
      try {
        List<ApplicationReport> apps = yarnClient.getApplications();
        if (apps.size() == 0) {
          return false;
        }
        ApplicationReport appReport = apps.get(0);
        appReportRef.set(appReport);
        appIdRef.set(appReport.getApplicationId());
        if (appReport.getHost().equals("N/A")) {
          return false;
        }
        if (appReport.getRpcPort() == -1) {
          waitResult.set(1);
        }
        if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
            && appReport.getFinalApplicationStatus() !=
            FinalApplicationStatus.UNDEFINED) {
          return true;
        }
      } catch (Exception e) {
        LOG.error("Exception get application from Yarn Client", e);
        waitResult.set(2);
      }
      return waitResult.get() != 0;
    }, 10, TEST_TIME_WINDOW_EXPIRE);
    t.join();
    if (waitResult.get() == 2) {
      // Exception was raised
      Assert.fail("Exception in getting application report. Failed");
    }
    if (waitResult.get() == 1) {
      Assert.assertEquals("Failed waiting for expected rpc port to be -1.",
          -1, appReportRef.get().getRpcPort());
    }
    checkTimeline(appIdRef.get(), defaultFlow, haveDomain, appReportRef.get());
  }

  protected void baseTestDSShell(boolean haveDomain) throws Exception {
    baseTestDSShell(haveDomain, true);
  }

  protected void checkTimeline(ApplicationId appId,
      boolean defaultFlow, boolean haveDomain,
      ApplicationReport appReport) throws Exception {
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
    Assert.assertEquals(haveDomain ? domain.getId() : "DEFAULT",
        entitiesAttempts.getEntities().get(0).getDomainId());
    String currAttemptEntityId =
        entitiesAttempts.getEntities().get(0).getEntityId();
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
    TimelineEntity entity =
        yarnCluster.getApplicationHistoryServer().getTimelineStore()
            .getEntity(entityId,
                ApplicationMaster.DSEntity.DS_CONTAINER.toString(), null);
    Assert.assertNotNull(entity);
    Assert.assertEquals(entityId, entity.getEntityId());
    Assert.assertEquals(haveDomain ? domain.getId() : "DEFAULT",
        entities.getEntities().get(0).getDomainId());
  }

  protected String[] createArgsWithPostFix(int index, String... args) {
    String[] res = mergeArgs(COMMON_ARGS, args);
    // set the application name so we can track down which command is running.
    res[COMMON_ARGS.length - 1] = generateAppName(String.format("%03d",
        index));
    return res;
  }

  protected String generateAppName() {
    return generateAppName(null);
  }

  protected String generateAppName(String postFix) {
    return name.getMethodName().replaceFirst("test", "")
        .concat(postFix == null ? "" : "-" + postFix);
  }

  protected void setUpHDFSCluster() throws IOException {
    if (hdfsCluster == null) {
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(NUM_DATA_NODES).build();
      hdfsCluster.waitActive();
    }
  }

  protected void setUpYarnCluster(int numNodeManagers,
      YarnConfiguration yarnConfig) throws Exception {
    if (yarnCluster != null) {
      return;
    }
    yarnCluster =
        new MiniYARNCluster(getClass().getSimpleName(), 1, numNodeManagers,
            1, 1);
    yarnCluster.init(yarnConfig);
    yarnCluster.start();
    // wait for the node managers to register.
    waitForNMsToRegister();
    conf.set(
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
        MiniYARNCluster.getHostname() + ":"
            + yarnCluster.getApplicationHistoryServer().getPort());
    Configuration yarnClusterConfig = yarnCluster.getConfig();
    yarnClusterConfig.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        new File(yarnSitePath).getParent());
    // write the document to a buffer (not directly to the file, as that
    // can cause the file being written to get read -which will then fail.
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    yarnClusterConfig.writeXml(bytesOut);
    bytesOut.close();
    // write the bytes to the file in the classpath
    OutputStream os = new FileOutputStream(yarnSitePath);
    os.write(bytesOut.toByteArray());
    os.close();
  }

  protected void setupInternal(int numNodeManagers,
      YarnConfiguration yarnConfig) throws Exception {
    LOG.info("========== Setting UP UnitTest {}#{} ==========",
        getClass().getCanonicalName(), name.getMethodName());
    LOG.info("Starting up YARN cluster. Timeline version {}",
        getTimelineVersion());
    conf = yarnConfig;
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        MIN_ALLOCATION_MB);
    // reduce the tearDown waiting time
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
        YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING, true);
    conf.setBoolean(YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_ENABLED,
        true);
    conf.setBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED, true);
    conf.setInt(YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        10);
    conf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    // ATS version specific settings
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION,
        getTimelineVersion());
    // setup the configuration of relevant for each TimelineService version.
    customizeConfiguration(conf);
    // setup the yarn cluster.
    setUpYarnCluster(numNodeManagers, conf);
  }

  protected NodeManager getNodeManager(int index) {
    return yarnCluster.getNodeManager(index);
  }

  protected MiniYARNCluster getYarnCluster() {
    return yarnCluster;
  }

  protected void setConfiguration(String key, String value) {
    conf.set(key, value);
  }

  protected Configuration getYarnClusterConfiguration() {
    return yarnCluster.getConfig();
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  protected ResourceManager getResourceManager() {
    return yarnCluster.getResourceManager();
  }

  protected ResourceManager getResourceManager(int index) {
    return yarnCluster.getResourceManager(index);
  }

  protected Client getDSClient() {
    return dsClient;
  }

  protected void resetDSClient() {
    dsClient = null;
  }

  protected abstract float getTimelineVersion();

  protected void cleanUpDFSClient() {
    if (getDSClient() != null) {
      getDSClient().sendStopSignal();
      resetDSClient();
    }
  }

  private void waitForNMsToRegister() throws Exception {
    GenericTestUtils.waitFor(() -> {
      RMContext rmContext = yarnCluster.getResourceManager().getRMContext();
      return (rmContext.getRMNodes().size() >= NUM_NMS);
    }, 100, 60000);
  }

  protected MiniDFSCluster getHDFSCluster() {
    return hdfsCluster;
  }
}
