/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.slider.client.SliderClient;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.SliderXmlConfKeys;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.hadoop.yarn.service.client.params.Arguments;
import org.apache.hadoop.yarn.service.client.params.SliderActions;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.slider.utils.KeysForTests.*;

/**
 * Base class for mini cluster tests -creates a field for the
 * mini yarn cluster.
 */
public abstract class YarnMiniClusterTestBase extends SliderTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnMiniClusterTestBase.class);

  /**
   * Mini YARN cluster only.
   */
  public static final int CLUSTER_GO_LIVE_TIME = 3 * 60 * 1000;
  public static final int CLUSTER_STOP_TIME = 1 * 60 * 1000;
  public static final int SIGTERM = -15;
  public static final int SIGKILL = -9;
  public static final int SIGSTOP = -17;
  public static final String NO_ARCHIVE_DEFINED = "Archive configuration " +
      "option not set: ";
  /**
   * RAM for the YARN containers: {@value}.
   */
  public static final String YRAM = "256";
  public static final String FIFO_SCHEDULER = "org.apache.hadoop.yarn.server" +
      ".resourcemanager.scheduler.fifo.FifoScheduler";
  public static final YarnConfiguration SLIDER_CONFIG =
      SliderUtils.createConfiguration();
  private static boolean killSupported;

  static {
    SLIDER_CONFIG.setInt(SliderXmlConfKeys.KEY_AM_RESTART_LIMIT, 1);
    SLIDER_CONFIG.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 100);
    SLIDER_CONFIG.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    SLIDER_CONFIG.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    SLIDER_CONFIG
        .setBoolean(SliderXmlConfKeys.KEY_SLIDER_AM_DEPENDENCY_CHECKS_DISABLED,
            true);
    SLIDER_CONFIG
        .setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 1);
  }


  private int thawWaitTime = DEFAULT_THAW_WAIT_TIME_SECONDS * 1000;
  private int freezeWaitTime = DEFAULT_TEST_FREEZE_WAIT_TIME_SECONDS * 1000;
  private int sliderTestTimeout = DEFAULT_TEST_TIMEOUT_SECONDS * 1000;
  private boolean teardownKillall = DEFAULT_TEARDOWN_KILLALL;

  /**
   * This is set in a system property.
   */
  @Rule
  public Timeout testTimeout = new Timeout(
      getTimeOptionMillis(getTestConfiguration(),
          KEY_TEST_TIMEOUT,
          DEFAULT_TEST_TIMEOUT_SECONDS * 1000)
  );
  private MiniDFSCluster hdfsCluster;
  private MiniYARNCluster miniCluster;
  private boolean switchToImageDeploy = false;
  private boolean imageIsRemote = false;
  private URI remoteImageURI;
  private List<SliderClient> clustersToTeardown = new ArrayList<>();
  private int clusterCount = 1;

  /**
   * Clent side test: validate system env before launch.
   */
  @BeforeClass
  public static void checkClientEnv() throws IOException, SliderException {
    SliderUtils.validateSliderClientEnvironment(null);
  }

  /**
   * Work out if kill is supported.
   */
  @BeforeClass
  public static void checkKillSupport() {
    killSupported = !Shell.WINDOWS;
  }

  protected static boolean getKillSupported() {
    return killSupported;
  }

  protected MiniYARNCluster getMiniCluster() {
    return miniCluster;
  }

  /**
   * Probe for the disks being healthy in a mini cluster. Only the first
   * NM is checked.
   *
   * @param miniCluster
   */
  public static void assertMiniClusterDisksHealthy(
      MiniYARNCluster miniCluster) {
    boolean healthy = miniCluster.getNodeManager(
        0).getNodeHealthChecker().getDiskHandler().areDisksHealthy();
    assertTrue("Disks on test cluster unhealthy —may be full", healthy);
  }

  /**
   * Inner work building the mini dfs cluster.
   *
   * @param name
   * @param conf
   * @return
   */
  public static MiniDFSCluster buildMiniHDFSCluster(
      String name,
      YarnConfiguration conf) throws IOException {
    assertNativeLibrariesPresent();

    File baseDir = new File("./target/hdfs", name).getAbsoluteFile();
    //use file: to rm it recursively
    FileUtil.fullyDelete(baseDir);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);

    MiniDFSCluster cluster = builder.build();
    return cluster;
  }

  public static String buildFsDefaultName(MiniDFSCluster miniDFSCluster) {
    if (miniDFSCluster != null) {
      return String.format("hdfs://localhost:%d/",
          miniDFSCluster.getNameNodePort());
    } else {
      return "file:///";
    }
  }

  /**
   * Assert that an operation failed because a cluster is in use.
   *
   * @param e exception
   */
  public static void assertFailureClusterInUse(SliderException e) {
    assertExceptionDetails(e,
        SliderExitCodes.EXIT_APPLICATION_IN_USE,
        ErrorStrings.E_CLUSTER_RUNNING);
  }

  protected String buildClustername(String clustername) {
    if (SliderUtils.isSet(clustername)) {
      return clustername;
    } else {
      return createClusterName();
    }
  }

  /**
   * Create the cluster name from the method name and an auto-incrementing
   * counter.
   *
   * @return a cluster name
   */
  protected String createClusterName() {
    String base = methodName.getMethodName().toLowerCase(Locale.ENGLISH);
    if (clusterCount++ > 1) {
      return String.format("%s-%d", base, clusterCount);
    }
    return base;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Configuration testConf = getTestConfiguration();
    thawWaitTime = getTimeOptionMillis(testConf,
        KEY_TEST_THAW_WAIT_TIME,
        thawWaitTime);
    freezeWaitTime = getTimeOptionMillis(testConf,
        KEY_TEST_FREEZE_WAIT_TIME,
        freezeWaitTime);
    sliderTestTimeout = getTimeOptionMillis(testConf,
        KEY_TEST_TIMEOUT,
        sliderTestTimeout);
    teardownKillall =
        testConf.getBoolean(KEY_TEST_TEARDOWN_KILLALL,
            teardownKillall);

  }

  @After
  public void teardown() {
    describe("teardown");
    stopRunningClusters();
    stopMiniCluster();
  }

  protected void addToTeardown(SliderClient client) {
    clustersToTeardown.add(client);
  }

  protected void addToTeardown(ServiceLauncher<SliderClient> launcher) {
    if (launcher != null) {
      SliderClient sliderClient = launcher.getService();
      if (sliderClient != null) {
        addToTeardown(sliderClient);
      }
    }
  }

  /**
   * Kill any java process with the given grep pattern.
   *
   * @param grepString string to grep for
   */
  public int killJavaProcesses(String grepString, int signal)
      throws IOException, InterruptedException {

    String[] commandString;
    if (!Shell.WINDOWS) {
      String killCommand = String.format(
          "jps -l| grep %s | awk '{print $1}' | xargs kill %d", grepString,
          signal);
      LOG.info("Command command = {}", killCommand);

      commandString = new String[]{"bash", "-c", killCommand};
    } else {
      // windows
      if (!killSupported) {
        return -1;
      }

      // "jps -l | grep "String" | awk "{print $1}" | xargs -n 1 taskkill /PID"
      String killCommand = String.format(
          "jps -l | grep %s | gawk '{print $1}' | xargs -n 1 taskkill /f " +
              "/PID", grepString);
      commandString = new String[]{"CMD", "/C", killCommand};
    }

    Process command = new ProcessBuilder(commandString).start();
    int exitCode = command.waitFor();

    logStdOutStdErr(command);
    return exitCode;
  }

  /**
   * Kill all processes which match one of the list of grepstrings.
   *
   * @param greps
   * @param signal
   */
  public void killJavaProcesses(List<String> greps, int signal)
      throws IOException, InterruptedException {
    for (String grep : greps) {
      killJavaProcesses(grep, signal);
    }
  }

  protected YarnConfiguration getConfiguration() {
    return SLIDER_CONFIG;
  }

  /**
   * Stop any running cluster that has been added.
   */
  public void stopRunningClusters() {
    for (SliderClient client : clustersToTeardown) {
      maybeStopCluster(client, client.getDeployedClusterName(),
          "Teardown at end of test case", true);
    }
  }

  public void stopMiniCluster() {
    Log commonslog = LogFactory.getLog(YarnMiniClusterTestBase.class);
    ServiceOperations.stopQuietly(commonslog, miniCluster);
    if (hdfsCluster != null) {
      hdfsCluster.shutdown();
    }
  }

  /**
   * Create and start a minicluster.
   *
   * @param name             cluster/test name; if empty one is created from
   *                         the junit method
   * @param conf             configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs     #of local dirs
   * @param numLogDirs       #of log dirs
   * @param startHDFS        create an HDFS mini cluster
   * @return the name of the cluster
   */
  protected String createMiniCluster(String name,
      YarnConfiguration conf,
      int noOfNodeManagers,
      int numLocalDirs,
      int numLogDirs,
      boolean startHDFS) throws IOException {
    assertNativeLibrariesPresent();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.set(YarnConfiguration.RM_SCHEDULER, FIFO_SCHEDULER);
    patchDiskCapacityLimits(conf);
    SliderUtils.patchConfiguration(conf);
    name = buildClustername(name);
    miniCluster = new MiniYARNCluster(
        name,
        noOfNodeManagers,
        numLocalDirs,
        numLogDirs);
    miniCluster.init(conf);
    miniCluster.start();
    // health check
    assertMiniClusterDisksHealthy(miniCluster);
    if (startHDFS) {
      createMiniHDFSCluster(name, conf);
    }
    return name;
  }

  public void patchDiskCapacityLimits(YarnConfiguration conf) {
    conf.setFloat(
        YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
        99.0f);
    conf.setInt(SliderXmlConfKeys.DFS_NAMENODE_DU_RESERVED_KEY,
        2 * 1024 * 1024);
    conf.setBoolean("yarn.nodemanager.disk-health-checker.enable", false);
  }

  /**
   * Create a mini HDFS cluster and save it to the hdfsClusterField.
   *
   * @param name
   * @param conf
   */
  public void createMiniHDFSCluster(String name, YarnConfiguration conf)
      throws IOException {
    hdfsCluster = buildMiniHDFSCluster(name, conf);
  }

  /**
   * Launch the client with the specific args against the MiniMR cluster
   * launcher i.e. expected to have successfully completed.
   *
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected ServiceLauncher<SliderClient> launchClientAgainstMiniMR(
      Configuration conf,
      List args)
      throws Throwable {
    ServiceLauncher<SliderClient> launcher =
        launchClientNoExitCodeCheck(conf, args);
    int exited = launcher.getServiceExitCode();
    if (exited != 0) {
      throw new SliderException(exited, "Launch failed with exit code " +
          exited);
    }
    return launcher;
  }

  /**
   * Launch the client with the specific args against the MiniMR cluster
   * without any checks for exit codes.
   *
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  public ServiceLauncher<SliderClient> launchClientNoExitCodeCheck(
      Configuration conf,
      List args) throws Throwable {
    assertNotNull(miniCluster);
    return launchClientAgainstRM(getRMAddr(), args, conf);
  }

  /**
   * Kill all Slider Services.
   *
   * @param signal
   */
  public int killAM(int signal) throws IOException, InterruptedException {
    return killJavaProcesses(SliderAppMaster.SERVICE_CLASSNAME_SHORT, signal);
  }

  public void logStdOutStdErr(Process p) throws IOException {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(p
        .getInputStream()))) {
      String line = br.readLine();
      while (line != null) {
        LOG.info(line);
        line = br.readLine();
      }
    }
    try (BufferedReader br = new BufferedReader(new InputStreamReader(p
        .getErrorStream()))) {
      String line = br.readLine();
      while (line != null) {
        LOG.error(line);
        line = br.readLine();
      }
    }
  }

  /**
   * List any java process.
   */
  public void lsJavaProcesses() throws InterruptedException, IOException {
    Process bash = new ProcessBuilder("jps", "-v").start();
    bash.waitFor();
    logStdOutStdErr(bash);
  }

  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = getConfiguration();
    conf.addResource(SLIDER_TEST_XML);
    return conf;
  }

  protected String getRMAddr() {
    assertNotNull(miniCluster);
    String addr = miniCluster.getConfig().get(YarnConfiguration.RM_ADDRESS);
    assertNotNull(addr != null);
    assertNotEquals("", addr);
    return addr;
  }

  /**
   * Return the default filesystem, which is HDFS if the miniDFS cluster is
   * up, file:// if not.
   *
   * @return a filesystem string to pass down
   */
  protected String getFsDefaultName() {
    return buildFsDefaultName(hdfsCluster);
  }

  /**
   * Create or build a cluster (the action is set by the first verb).
   * @param action operation to invoke: SliderActions.ACTION_CREATE or
   *               SliderActions.ACTION_BUILD
   * @param clustername cluster name
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createOrBuildCluster(String action,
      String clustername, List<String> extraArgs, boolean deleteExistingData,
      boolean blockUntilRunning) throws Throwable {
    assertNotNull(clustername);
    assertNotNull(miniCluster);
    // update action should keep existing data
    Configuration config = miniCluster.getConfig();
    if (deleteExistingData && !SliderActions.ACTION_UPDATE.equals(action)) {
      FileSystem dfs = FileSystem.get(new URI(getFsDefaultName()), config);

      SliderFileSystem sliderFileSystem = new SliderFileSystem(dfs, config);
      Path clusterDir = sliderFileSystem.buildClusterDirPath(clustername);
      LOG.info("deleting instance data at {}", clusterDir);
      //this is a safety check to stop us doing something stupid like deleting /
      assertTrue(clusterDir.toString().contains("/.slider/"));
      rigorousDelete(sliderFileSystem, clusterDir, 60000);
    }


    List<String> argsList = new ArrayList<>();
    argsList.addAll(Arrays.asList(
        action, clustername,
        Arguments.ARG_MANAGER, getRMAddr(),
        Arguments.ARG_FILESYSTEM, getFsDefaultName(),
        Arguments.ARG_DEBUG));

    argsList.addAll(getExtraCLIArgs());

    if (extraArgs != null) {
      argsList.addAll(extraArgs);
    }
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(config),
        //varargs list of command line params
        argsList
    );
    assertEquals(0, launcher.getServiceExitCode());
    SliderClient client = launcher.getService();
    if (blockUntilRunning) {
      client.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME));
    }
    return launcher;
  }

  /**
   * Delete with some pauses and backoff; designed to handle slow delete
   * operation in windows.
   */
  public void rigorousDelete(
      SliderFileSystem sliderFileSystem,
      Path path, long timeout) throws IOException, SliderException {

    if (path.toUri().getScheme() == "file") {
      File dir = new File(path.toUri().getPath());
      rigorousDelete(dir, timeout);
    } else {
      Duration duration = new Duration(timeout);
      duration.start();
      FileSystem dfs = sliderFileSystem.getFileSystem();
      boolean deleted = false;
      while (!deleted && !duration.getLimitExceeded()) {
        dfs.delete(path, true);
        deleted = !dfs.exists(path);
        if (!deleted) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.info("ignoring interrupted sleep");
          }
        }
      }
    }
    sliderFileSystem.verifyDirectoryNonexistent(path);
  }

  /**
   * Delete with some pauses and backoff; designed to handle slow delete
   * operation in windows.
   *
   * @param dir     dir to delete
   * @param timeout timeout in millis
   */
  public void rigorousDelete(File dir, long timeout) throws IOException {
    Duration duration = new Duration(timeout);
    duration.start();
    boolean deleted = false;
    while (!deleted && !duration.getLimitExceeded()) {
      FileUtils.deleteQuietly(dir);
      deleted = !dir.exists();
      if (!deleted) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("ignoring interrupted sleep");
        }
      }
    }
    if (!deleted) {
      // noisy delete raises an IOE
      FileUtils.deleteDirectory(dir);
    }
  }

  /**
   * Add arguments to launch Slider with.
   * <p>
   * Extra arguments are added after standard arguments and before roles.
   *
   * @return additional arguments to launch Slider with
   */
  protected List<String> getExtraCLIArgs() {
    return new ArrayList<>();
  }

  public String getConfDir() throws FileNotFoundException {
    return getResourceConfDirURI();
  }

  /**
   * Get the key for the application.
   *
   * @return
   */
  public String getApplicationHomeKey() {
    failNotImplemented();
    return null;
  }

  /**
   * Get the archive path -which defaults to the local one.
   *
   * @return
   */
  public String getArchivePath() {
    return getLocalArchive();
  }

  /**
   * Get the local archive -the one defined in the test configuration.
   *
   * @return a possibly null/empty string
   */
  public final String getLocalArchive() {
    return getTestConfiguration().getTrimmed(getArchiveKey());
  }

  /**
   * Get the key for archives in tests.
   *
   * @return
   */
  public String getArchiveKey() {
    failNotImplemented();
    return null;
  }

  /**
   * Merge a k-v pair into a simple k=v string; simple utility.
   *
   * @param key key
   * @param val value
   * @return the string to use after a -D option
   */
  public String define(String key, String val) {
    return String.format("%s=%s", key, val);
  }

  public void assumeTestEnabled(boolean flag) {
    assume(flag, "test disabled");
  }

  public void assumeArchiveDefined() {
    String archive = getArchivePath();
    boolean defined = archive != null && archive != "";
    if (!defined) {
      LOG.warn(NO_ARCHIVE_DEFINED + getArchiveKey());
    }
    assume(defined, NO_ARCHIVE_DEFINED + getArchiveKey());
  }

  /**
   * Assume that application home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * application home to be set.
   */
  public void assumeApplicationHome() {
    String applicationHome = getApplicationHome();
    assume(applicationHome != null && applicationHome != "",
        "Application home dir option not set " + getApplicationHomeKey());
  }

  public String getApplicationHome() {
    return getTestConfiguration().getTrimmed(getApplicationHomeKey());
  }

  /**
   * Get the resource configuration dir in the source tree.
   *
   * @return
   */
  public File getResourceConfDir() throws FileNotFoundException {
    File f = new File(getTestConfigurationPath()).getAbsoluteFile();
    if (!f.exists()) {
      throw new FileNotFoundException(
          "Resource configuration directory " + f + " not found");
    }
    return f;
  }

  public String getTestConfigurationPath() {
    failNotImplemented();
    return null;
  }

  /**
   * Get a URI string to the resource conf dir that is suitable for passing down
   * to the AM -and works even when the default FS is hdfs.
   */
  public String getResourceConfDirURI() throws FileNotFoundException {
    return getResourceConfDir().getAbsoluteFile().toURI().toString();
  }

  /**
   * Log an application report.
   *
   * @param report
   */
  public void logReport(ApplicationReport report) {
    LOG.info(SliderUtils.reportToString(report));
  }

  /**
   * Stop the cluster via the stop action -and wait for
   * {@link #CLUSTER_STOP_TIME} for the cluster to stop. If it doesn't
   *
   * @param sliderClient client
   * @param clustername  cluster
   * @return the exit code
   */
  public int clusterActionFreeze(SliderClient sliderClient, String clustername,
      String message, boolean force)
      throws IOException, YarnException {
    LOG.info("Stopping cluster {}: {}", clustername, message);
    ActionFreezeArgs freezeArgs = new ActionFreezeArgs();
    freezeArgs.setWaittime(CLUSTER_STOP_TIME);
    freezeArgs.message = message;
    freezeArgs.force = force;
    int exitCode = sliderClient.actionStop(clustername,
        freezeArgs);
    if (exitCode != 0) {
      LOG.warn("Cluster stop failed with error code {}", exitCode);
    }
    return exitCode;
  }

  /**
   * Teardown-time cluster termination; will stop the cluster iff the client
   * is not null.
   *
   * @param sliderClient client
   * @param clustername  name of cluster to teardown
   * @return
   */
  public int maybeStopCluster(
      SliderClient sliderClient,
      String clustername,
      String message,
      boolean force) {
    if (sliderClient != null) {
      if (SliderUtils.isUnset(clustername)) {
        clustername = sliderClient.getDeployedClusterName();
      }
      //only stop a cluster that exists
      if (SliderUtils.isSet(clustername)) {
        try {
          clusterActionFreeze(sliderClient, clustername, message, force);
        } catch (Exception e) {
          LOG.warn("While stopping cluster " + e, e);
        }
        try {
          sliderClient.actionDestroy(clustername);
        } catch (Exception e) {
          LOG.warn("While destroying cluster " + e, e);
        }
      }
    }
    return 0;
  }

  public String roleMapToString(Map<String, Integer> roles) {
    StringBuilder builder = new StringBuilder();
    for (Entry<String, Integer> entry : roles.entrySet()) {
      builder.append(entry.getKey());
      builder.append("->");
      builder.append(entry.getValue());
      builder.append(" ");
    }
    return builder.toString();
  }

  /**
   * Turn on test runs against a copy of the archive that is
   * uploaded to HDFS -this method copies up the
   * archive then switches the tests into archive mode.
   */
  public void enableTestRunAgainstUploadedArchive() throws IOException {
    Path remotePath = copyLocalArchiveToHDFS(getLocalArchive());
    // image mode
    switchToRemoteImageDeploy(remotePath);
  }

  /**
   * Switch to deploying a remote image.
   *
   * @param remotePath the remote path to use
   */
  public void switchToRemoteImageDeploy(Path remotePath) {
    switchToImageDeploy = true;
    imageIsRemote = true;
    remoteImageURI = remotePath.toUri();
  }

  /**
   * Copy a local archive to HDFS.
   *
   * @param localArchive local archive
   * @return the path of the uploaded image
   */
  public Path copyLocalArchiveToHDFS(String localArchive) throws IOException {
    assertNotNull(localArchive);
    File localArchiveFile = new File(localArchive);
    assertTrue(localArchiveFile.exists());
    assertNotNull(hdfsCluster);
    Path remoteUnresolvedArchive = new Path(localArchiveFile.getName());
    assertTrue(FileUtil.copy(
        localArchiveFile,
        hdfsCluster.getFileSystem(),
        remoteUnresolvedArchive,
        false,
        getTestConfiguration()));
    Path remotePath = hdfsCluster.getFileSystem().resolvePath(
        remoteUnresolvedArchive);
    return remotePath;
  }

  /**
   * Create a SliderFileSystem instance bonded to the running FS.
   * The YARN cluster must be up and running already
   *
   * @return
   */
  public SliderFileSystem createSliderFileSystem()
      throws URISyntaxException, IOException {
    FileSystem dfs =
        FileSystem.get(new URI(getFsDefaultName()), getConfiguration());
    SliderFileSystem hfs = new SliderFileSystem(dfs, getConfiguration());
    return hfs;
  }

}
