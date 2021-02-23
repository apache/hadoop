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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.client.api.impl.DirectTimelineWriter;
import org.apache.hadoop.yarn.client.api.impl.TestTimelineClient;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.client.api.impl.TimelineWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests implementations for distributed shell on TimeLineV1.
 */
public class TestDSTimelineV10 extends DistributedShellBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDSTimelineV10.class);

  @Override
  protected float getTimelineVersion() {
    return 1.0f;
  }

  @Override
  protected void cleanUpDFSClient() {

  }

  @Test
  public void testDSShellWithDomain() throws Exception {
    baseTestDSShell(true);
  }

  @Test
  public void testDSShellWithoutDomain() throws Exception {
    baseTestDSShell(false);
  }

  @Test
  public void testDSRestartWithPreviousRunningContainers() throws Exception {
    String[] args = createArgumentsWithAppName(
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
    setAndGetDSClient(TestDSFailedAppMaster.class.getName(),
        new Configuration(getYarnClusterConfiguration()));

    getDSClient().init(args);

    LOG.info("Running DS Client");
    boolean result = getDSClient().run();
    LOG.info("Client run completed. Result={}", result);
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
  public void testDSAttemptFailuresValidityIntervalSuccess() throws Exception {
    String[] args = createArgumentsWithAppName(
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
    Configuration config = getYarnClusterConfiguration();
    config.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    setAndGetDSClient(TestDSSleepingAppMaster.class.getName(),
        new Configuration(config));

    getDSClient().init(args);

    LOG.info("Running DS Client");
    boolean result = getDSClient().run();

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
    String[] args = createArgumentsWithAppName(
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
    Configuration config = getYarnClusterConfiguration();
    config.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    setAndGetDSClient(TestDSSleepingAppMaster.class.getName(),
        new Configuration(config));

    getDSClient().init(args);

    LOG.info("Running DS Client");
    boolean result = getDSClient().run();

    LOG.info("Client run completed. Result=" + result);
    // application should be failed
    Assert.assertFalse(result);
  }

  @Test
  public void testDSShellWithCustomLogPropertyFile() throws Exception {
    final File basedir = getBaseDirForTest();
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customLogProperty = new File(tmpDir, "custom_log4j.properties");
    if (customLogProperty.exists()) {
      customLogProperty.delete();
    }
    if (!customLogProperty.createNewFile()) {
      Assert.fail("Can not create custom log4j property file.");
    }
    PrintWriter fileWriter = new PrintWriter(customLogProperty);
    // set the output to DEBUG level
    fileWriter.write("log4j.rootLogger=debug,stdout");
    fileWriter.close();
    String[] args = createArgumentsWithAppName(
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

    // Before run the DS, the default the log level is INFO
    final Logger LOG_Client =
        LoggerFactory.getLogger(Client.class);
    Assert.assertTrue(LOG_Client.isInfoEnabled());
    Assert.assertFalse(LOG_Client.isDebugEnabled());
    final Logger LOG_AM = LoggerFactory.getLogger(ApplicationMaster.class);
    Assert.assertTrue(LOG_AM.isInfoEnabled());
    Assert.assertFalse(LOG_AM.isDebugEnabled());

    LOG.info("Initializing DS Client");
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    boolean initSuccess = getDSClient().init(args);
    Assert.assertTrue(initSuccess);

    LOG.info("Running DS Client");
    boolean result = getDSClient().run();
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
    String[] args = createArgumentsWithAppName(
        "--shell_command",
        "echo",
        "--rolling_log_pattern",
        regex
    );
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    Assert.assertTrue(getDSClient().init(args));

    ApplicationSubmissionContext context =
        Records.newRecord(ApplicationSubmissionContext.class);
    getDSClient().specifyLogAggregationContext(context);
    LogAggregationContext logContext = context.getLogAggregationContext();
    assertEquals(logContext.getRolledLogsIncludePattern(), regex);
    assertTrue(logContext.getRolledLogsExcludePattern().isEmpty());
  }

  @Test
  public void testDSShellWithMultipleArgs() throws Exception {
    String[] args = createArgumentsWithAppName(
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
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    boolean initSuccess = getDSClient().init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");

    boolean result = getDSClient().run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<>();
    expectedContent.add("HADOOP YARN MAPREDUCE HDFS");
    verifyContainerLog(4, expectedContent, false, "");
  }

  @Test
  public void testDSShellWithShellScript() throws Exception {
    final File basedir = getBaseDirForTest();
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
    String[] args = createArgumentsWithAppName(
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
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    Assert.assertTrue(getDSClient().init(args));
    LOG.info("Running DS Client");
    assertTrue(getDSClient().run());
    List<String> expectedContent = new ArrayList<>();
    expectedContent.add("testDSShellWithShellScript");
    verifyContainerLog(1, expectedContent, false, "");
  }

  @Test
  public void testDSShellWithInvalidArgs() throws Exception {
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    int appNameCounter = 0;
    LOG.info("Initializing DS Client with no args");
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "No args",
        () -> getDSClient().init(new String[]{}));

    LOG.info("Initializing DS Client with no jar file");
    String[] noJarArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "2",
        "--shell_command",
        getListCommand(),
        "--master_memory",
        "512",
        "--container_memory",
        "128"
    );
    String[] argsNoJar = Arrays.copyOfRange(noJarArgs, 2, noJarArgs.length);
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "No jar",
        () -> getDSClient().init(argsNoJar));

    LOG.info("Initializing DS Client with no shell command");
    String[] noShellCmdArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "2",
        "--master_memory",
        "512",
        "--container_memory",
        "128"
    );
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "No shell command",
        () -> getDSClient().init(noShellCmdArgs));

    LOG.info("Initializing DS Client with invalid no. of containers");

    String[] numContainersArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "-1",
        "--shell_command",
        getListCommand(),
        "--master_memory",
        "512",
        "--container_memory",
        "128"
    );
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid no. of containers",
        () -> getDSClient().init(numContainersArgs));

    LOG.info("Initializing DS Client with invalid no. of vcores");

    String[] vCoresArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "2",
        "--shell_command",
        getListCommand(),
        "--master_memory",
        "512",
        "--master_vcores",
        "-2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1"
    );
    getDSClient().init(vCoresArgs);

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid virtual cores specified",
        () -> {
          getDSClient().init(vCoresArgs);
          getDSClient().run();
        });

    LOG.info("Initializing DS Client with --shell_command and --shell_script");

    String[] scriptAndCmdArgs = createArgsWithPostFix(appNameCounter++,
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
        "1",
        "--shell_script",
        "test.sh"
    );

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Can not specify shell_command option and shell_script option at "
            + "the same time",
        () -> getDSClient().init(scriptAndCmdArgs));

    LOG.info(
        "Initializing DS Client without --shell_command and --shell_script");

    String[] noShellCmdNoScriptArgs = createArgsWithPostFix(appNameCounter++,
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
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "No shell command or shell script specified "
            + "to be executed by application master",
        () -> getDSClient().init(noShellCmdNoScriptArgs));

    LOG.info("Initializing DS Client with invalid container_type argument");
    String[] invalidTypeArgs = createArgsWithPostFix(appNameCounter++,
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
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid container_type: UNSUPPORTED_TYPE",
        () -> getDSClient().init(invalidTypeArgs));

    String[] invalidMemArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "1",
        "--shell_command",
        getListCommand(),
        "--master_resources",
        "memory-mb=invalid"
    );
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> getDSClient().init(invalidMemArgs));

    String[] invalidMasterResArgs = createArgsWithPostFix(appNameCounter++,
        "--num_containers",
        "1",
        "--shell_command",
        getListCommand(),
        "--master_resources"
    );
    LambdaTestUtils.intercept(MissingArgumentException.class,
        () -> getDSClient().init(invalidMasterResArgs));
  }

  @Test
  public void testDSTimelineClientWithConnectionRefuse() throws Exception {
    ApplicationMaster am = new ApplicationMaster();
    final AtomicReference<TimelineWriter> spyTimelineWriterRef =
        new AtomicReference<>(null);
    TimelineClientImpl client = new TimelineClientImpl() {
      @Override
      protected TimelineWriter createTimelineWriter(Configuration conf,
          UserGroupInformation authUgi, com.sun.jersey.api.client.Client client,
          URI resURI) throws IOException {
        TimelineWriter timelineWriter =
            new DirectTimelineWriter(authUgi, client, resURI);
        spyTimelineWriterRef.set(spy(timelineWriter));
        return spyTimelineWriterRef.get();
      }
    };
    client.init(getConfiguration());
    client.start();
    TestTimelineClient.mockEntityClientResponse(spyTimelineWriterRef.get(),
        null, false, true);
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

  @Test
  public void testContainerLaunchFailureHandling() throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "2",
        "--shell_command",
        getListCommand(),
        "--master_memory",
        "512",
        "--container_memory",
        "128"
    );

    LOG.info("Initializing DS Client");
    setAndGetDSClient(ContainerLaunchFailAppMaster.class.getName(),
        new Configuration(getYarnClusterConfiguration()));
    Assert.assertTrue(getDSClient().init(args));
    LOG.info("Running DS Client");
    Assert.assertFalse(getDSClient().run());
  }

  @Test
  public void testDebugFlag() throws Exception {
    String[] args = createArgumentsWithAppName(
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
        "1",
        "--debug"
    );

    LOG.info("Initializing DS Client");
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    Assert.assertTrue(getDSClient().init(args));
    LOG.info("Running DS Client");
    Assert.assertTrue(getDSClient().run());
  }

  private int verifyContainerLog(int containerNum,
      List<String> expectedContent, boolean count, String expectedWord) {
    File logFolder =
        new File(getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                YarnConfiguration.DEFAULT_NM_LOG_DIRS));

    File[] listOfFiles = logFolder.listFiles();
    Assert.assertNotNull(listOfFiles);
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
    for (File containerFile : containerFiles) {
      if (containerFile == null) {
        continue;
      }
      for (File output : containerFile.listFiles()) {
        if (output.getName().trim().contains("stdout")) {
          List<String> stdOutContent = new ArrayList<>();
          try (BufferedReader br = new BufferedReader(new FileReader(output))) {
            String sCurrentLine;

            int numOfline = 0;
            while ((sCurrentLine = br.readLine()) != null) {
              if (count) {
                if (sCurrentLine.contains(expectedWord)) {
                  numOfWords++;
                }
              } else if (output.getName().trim().equals("stdout")) {
                if (!Shell.WINDOWS) {
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
            getListCommand(), "--container_resource_profile",
            "maximum"),
        createArgsWithPostFix(appNameCounter++,
            "--num_containers", "1", "--shell_command",
            getListCommand(), "--master_resource_profile",
            "default"),
        createArgsWithPostFix(appNameCounter++,
            "--num_containers", "1", "--shell_command",
            getListCommand(), "--master_resource_profile",
            "default", "--container_resource_profile", "maximum"),
    };

    for (int i = 0; i < args.length; ++i) {
      LOG.info("Initializing DS Client[{}]", i);
      setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
      Assert.assertTrue(getDSClient().init(args[i]));
      LOG.info("Running DS Client[{}]", i);
      LambdaTestUtils.intercept(Exception.class,
          () -> getDSClient().run());
    }
  }

  @Test
  public void testDSShellWithOpportunisticContainers() throws Exception {
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));

    String[] args = createArgumentsWithAppName(
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
    assertTrue(getDSClient().init(args));
    assertTrue(getDSClient().run());
  }

  @Test(expected = ResourceNotFoundException.class)
  public void testDistributedShellAMResourcesWithUnknownResource()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getListCommand(),
        "--master_resources",
        "unknown-resource=5"
    );
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    assertTrue(getDSClient().init(args));
    getDSClient().run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDistributedShellNonExistentQueue()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getListCommand(),
        "--queue",
        "non-existent-queue"
    );
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    assertTrue(getDSClient().init(args));
    getDSClient().run();
  }

  @Test
  public void testDistributedShellWithSingleFileLocalization()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getCatCommand(),
        "--localize_files",
        "./src/test/resources/a.txt",
        "--shell_args",
        "a.txt"
    );

    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    assertTrue(getDSClient().init(args));
    assertTrue("Client exited with an error", getDSClient().run());
  }

  @Test
  public void testDistributedShellWithMultiFileLocalization()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getCatCommand(),
        "--localize_files",
        "./src/test/resources/a.txt,./src/test/resources/b.txt",
        "--shell_args",
        "a.txt b.txt"
    );

    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    assertTrue(getDSClient().init(args));
    assertTrue("Client exited with an error", getDSClient().run());
  }

  @Test(expected = UncheckedIOException.class)
  public void testDistributedShellWithNonExistentFileLocalization()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getCatCommand(),
        "--localize_files",
        "/non/existing/path/file.txt",
        "--shell_args",
        "file.txt"
    );

    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    assertTrue(getDSClient().init(args));
    assertTrue(getDSClient().run());
  }

  @Test
  public void testDistributedShellCleanup()
      throws Exception {
    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "1",
        "--shell_command",
        getListCommand()
    );
    Configuration config = new Configuration(getYarnClusterConfiguration());
    setAndGetDSClient(config);

    assertTrue(getDSClient().init(args));
    assertTrue(getDSClient().run());
    ApplicationId appId = getDSClient().getAppId();
    String relativePath =
        ApplicationMaster.getRelativePath(generateAppName(),
            appId.toString(), "");
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
  }

  @Override
  protected void customizeConfiguration(
      YarnConfiguration config) throws Exception {
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
  }

  private static File getBaseDirForTest() {
    return new File("target", TestDSTimelineV10.class.getName());
  }
}
