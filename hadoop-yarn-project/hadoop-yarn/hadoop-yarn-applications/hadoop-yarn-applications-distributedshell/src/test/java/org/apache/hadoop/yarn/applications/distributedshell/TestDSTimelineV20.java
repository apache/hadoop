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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEvent;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * Unit tests implementations for distributed shell on TimeLineV2.
 */
public class TestDSTimelineV20 extends DistributedShellBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDSTimelineV20.class);
  private static final String TIMELINE_AUX_SERVICE_NAME = "timeline_collector";

  @Override
  protected float getTimelineVersion() {
    return 2.0f;
  }

  @Override
  protected void customizeConfiguration(
      YarnConfiguration config) throws Exception {
    // disable v1 timeline server since we no longer have a server here
    // enable aux-service based timeline aggregators
    config.set(YarnConfiguration.NM_AUX_SERVICES, TIMELINE_AUX_SERVICE_NAME);
    config.set(YarnConfiguration.NM_AUX_SERVICES + "." +
            TIMELINE_AUX_SERVICE_NAME + ".class",
        PerNodeTimelineCollectorsAuxService.class.getName());
    config.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
        FileSystemTimelineWriterImpl.class,
        org.apache.hadoop.yarn.server.timelineservice.storage.
            TimelineWriter.class);
    setTimelineV2StorageDir();
    // set the file system timeline writer storage directory
    config.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
        getTimelineV2StorageDir());
  }

  @Test
  public void testDSShellWithEnforceExecutionType() throws Exception {
    YarnClient yarnClient = null;
    AtomicReference<Throwable> thrownError = new AtomicReference<>(null);
    AtomicReference<List<ContainerReport>> containersListRef =
        new AtomicReference<>(null);
    AtomicReference<ApplicationAttemptId> appAttemptIdRef =
        new AtomicReference<>(null);
    AtomicReference<ApplicationAttemptReport> appAttemptReportRef =
        new AtomicReference<>(null);
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
        getListCommand(),
        "--container_type",
        "OPPORTUNISTIC",
        "--enforce_execution_type"
    );
    try {
      setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
      getDSClient().init(args);
      Thread dsClientRunner = new Thread(() -> {
        try {
          getDSClient().run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      dsClientRunner.start();

      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(new Configuration(getYarnClusterConfiguration()));
      yarnClient.start();

      // expecting three containers including the AM container.
      waitForContainersLaunch(yarnClient, 3, appAttemptReportRef,
          containersListRef, appAttemptIdRef, thrownError);
      if (thrownError.get() != null) {
        Assert.fail(thrownError.get().getMessage());
      }
      ContainerId amContainerId = appAttemptReportRef.get().getAMContainerId();
      for (ContainerReport container : containersListRef.get()) {
        if (!container.getContainerId().equals(amContainerId)) {
          Assert.assertEquals(container.getExecutionType(),
              ExecutionType.OPPORTUNISTIC);
        }
      }
    } catch (Exception e) {
      LOG.error("Job execution with enforce execution type failed.", e);
      Assert.fail("Exception. " + e.getMessage());
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }
  }

  @Test
  public void testDistributedShellWithResources() throws Exception {
    doTestDistributedShellWithResources(false);
  }

  @Test
  public void testDistributedShellWithResourcesWithLargeContainers()
      throws Exception {
    doTestDistributedShellWithResources(true);
  }

  private void doTestDistributedShellWithResources(boolean largeContainers)
      throws Exception {
    AtomicReference<Throwable> thrownExceptionRef =
        new AtomicReference<>(null);
    AtomicReference<List<ContainerReport>> containersListRef =
        new AtomicReference<>(null);
    AtomicReference<ApplicationAttemptId> appAttemptIdRef =
        new AtomicReference<>(null);
    AtomicReference<ApplicationAttemptReport> appAttemptReportRef =
        new AtomicReference<>(null);
    Resource clusterResource = getYarnCluster().getResourceManager()
        .getResourceScheduler().getClusterResource();
    String masterMemoryString = "1 Gi";
    String containerMemoryString = "512 Mi";
    long[] memVars = {1024, 512};
    YarnClient yarnClient = null;
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

    String[] args = createArgumentsWithAppName(
        "--num_containers",
        "2",
        "--shell_command",
        getListCommand(),
        "--master_resources",
        "memory=" + masterMemoryString + ",vcores=1",
        "--container_resources",
        "memory=" + containerMemoryString + ",vcores=1"
    );

    LOG.info("Initializing DS Client");
    setAndGetDSClient(new Configuration(getYarnClusterConfiguration()));
    Assert.assertTrue(getDSClient().init(args));
    LOG.info("Running DS Client");
    final AtomicBoolean result = new AtomicBoolean(false);
    Thread dsClientRunner = new Thread(() -> {
      try {
        result.set(getDSClient().run());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    dsClientRunner.start();
    try {
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(new Configuration(getYarnClusterConfiguration()));
      yarnClient.start();
      // expecting two containers.
      waitForContainersLaunch(yarnClient, 2, appAttemptReportRef,
          containersListRef, appAttemptIdRef, thrownExceptionRef);
      if (thrownExceptionRef.get() != null) {
        Assert.fail(thrownExceptionRef.get().getMessage());
      }
      ContainerId amContainerId = appAttemptReportRef.get().getAMContainerId();
      ContainerReport report = yarnClient.getContainerReport(amContainerId);
      Resource masterResource = report.getAllocatedResource();
      Assert.assertEquals(memVars[0], masterResource.getMemorySize());
      Assert.assertEquals(1, masterResource.getVirtualCores());
      for (ContainerReport container : containersListRef.get()) {
        if (!container.getContainerId().equals(amContainerId)) {
          Resource containerResource = container.getAllocatedResource();
          Assert.assertEquals(memVars[1],
              containerResource.getMemorySize());
          Assert.assertEquals(1, containerResource.getVirtualCores());
        }
      }
    } finally {
      LOG.info("Signaling Client to Stop");
      if (yarnClient != null) {
        LOG.info("Stopping yarnClient service");
        yarnClient.stop();
      }
    }
  }

  @Test
  public void testDSShellWithoutDomain() throws Exception {
    baseTestDSShell(false);
  }

  @Test
  public void testDSShellWithoutDomainDefaultFlow() throws Exception {
    baseTestDSShell(false, true);
  }

  @Test
  public void testDSShellWithoutDomainCustomizedFlow() throws Exception {
    baseTestDSShell(false, false);
  }

  @Override
  protected String[] appendFlowArgsForTestDSShell(String[] args,
      boolean defaultFlow) {
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
    return args;
  }

  @Override
  protected void checkTimeline(ApplicationId appId, boolean defaultFlow,
      boolean haveDomain, ApplicationReport appReport) throws Exception {
    LOG.info("Started {}#checkTimeline()", getClass().getCanonicalName());
    // For PoC check using the file-based timeline writer (YARN-3264)
    String tmpRoot = getTimelineV2StorageDir() + File.separator + "entities"
        + File.separator;

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
      LOG.info("basePath for appId {}: {}", appId, basePath);
      // for this test, we expect DS_APP_ATTEMPT AND DS_CONTAINER dirs

      // Verify DS_APP_ATTEMPT entities posted by the client
      // there will be at least one attempt, look for that file
      String appTimestampFileName =
          String.format("appattempt_%d_000%d_000001%s",
              appId.getClusterTimestamp(), appId.getId(),
              FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION);
      File dsAppAttemptEntityFile = verifyEntityTypeFileExists(basePath,
          "DS_APP_ATTEMPT", appTimestampFileName);
      // Check if required events are published and same idprefix is sent for
      // on each publish.
      verifyEntityForTimeline(dsAppAttemptEntityFile,
          DSEvent.DS_APP_ATTEMPT_START.toString(), 1, 1, 0, true);
      // to avoid race condition of testcase, at least check 40 times with
      // sleep of 50ms
      verifyEntityForTimeline(dsAppAttemptEntityFile,
          DSEvent.DS_APP_ATTEMPT_END.toString(), 1, 40, 50, true);

      // Verify DS_CONTAINER entities posted by the client.
      String containerTimestampFileName =
          String.format("container_%d_000%d_01_000002.thist",
              appId.getClusterTimestamp(), appId.getId());
      File dsContainerEntityFile = verifyEntityTypeFileExists(basePath,
          "DS_CONTAINER", containerTimestampFileName);
      // Check if required events are published and same idprefix is sent for
      // on each publish.
      verifyEntityForTimeline(dsContainerEntityFile,
          DSEvent.DS_CONTAINER_START.toString(), 1, 1, 0, true);
      // to avoid race condition of testcase, at least check 40 times with
      // sleep of 50ms.
      verifyEntityForTimeline(dsContainerEntityFile,
          DSEvent.DS_CONTAINER_END.toString(), 1, 40, 50, true);

      // Verify NM posting container metrics info.
      String containerMetricsTimestampFileName =
          String.format("container_%d_000%d_01_000001%s",
              appId.getClusterTimestamp(), appId.getId(),
              FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION);
      File containerEntityFile = verifyEntityTypeFileExists(basePath,
          TimelineEntityType.YARN_CONTAINER.toString(),
          containerMetricsTimestampFileName);
      verifyEntityForTimeline(containerEntityFile,
          ContainerMetricsConstants.CREATED_EVENT_TYPE, 1, 1, 0, true);

      // to avoid race condition of testcase, at least check 40 times with
      // sleep of 50ms
      verifyEntityForTimeline(containerEntityFile,
          ContainerMetricsConstants.FINISHED_EVENT_TYPE, 1, 40, 50, true);

      // Verify RM posting Application life cycle Events are getting published
      String appMetricsTimestampFileName =
          String.format("application_%d_000%d%s",
              appId.getClusterTimestamp(), appId.getId(),
              FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION);
      File appEntityFile =
          verifyEntityTypeFileExists(basePath,
              TimelineEntityType.YARN_APPLICATION.toString(),
              appMetricsTimestampFileName);
      // No need to check idprefix for app.
      verifyEntityForTimeline(appEntityFile,
          ApplicationMetricsConstants.CREATED_EVENT_TYPE, 1, 1, 0, false);

      // to avoid race condition of testcase, at least check 40 times with
      // sleep of 50ms
      verifyEntityForTimeline(appEntityFile,
          ApplicationMetricsConstants.FINISHED_EVENT_TYPE, 1, 40, 50, false);

      // Verify RM posting AppAttempt life cycle Events are getting published
      String appAttemptMetricsTimestampFileName =
          String.format("appattempt_%d_000%d_000001%s",
              appId.getClusterTimestamp(), appId.getId(),
              FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_EXTENSION);

      File appAttemptEntityFile =
          verifyEntityTypeFileExists(basePath,
              TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
              appAttemptMetricsTimestampFileName);
      verifyEntityForTimeline(appAttemptEntityFile,
          AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE, 1, 1, 0, true);
      verifyEntityForTimeline(appAttemptEntityFile,
          AppAttemptMetricsConstants.FINISHED_EVENT_TYPE, 1, 1, 0, true);
    } finally {
      try {
        FileUtils.deleteDirectory(tmpRootFolder.getParentFile());
      } catch (Exception ex) {
        // the recursive delete can throw an exception when one of the file
        // does not exist.
        LOG.warn("Exception deleting a file/subDirectory: {}", ex.getMessage());
      }
    }
  }

  /**
   * Checks the events and idprefix published for an entity.
   *
   * @param entityFile Entity file.
   * @param expectedEvent Expected event Id.
   * @param numOfExpectedEvent Number of expected occurrences of expected event
   *                           id.
   * @param checkTimes Number of times to check.
   * @param sleepTime Sleep time for each iteration.
   * @param checkIdPrefix Whether to check idprefix.
   * @throws IOException if entity file reading fails.
   * @throws InterruptedException if sleep is interrupted.
   */
  private void verifyEntityForTimeline(File entityFile, String expectedEvent,
      long numOfExpectedEvent, int checkTimes, long sleepTime,
      boolean checkIdPrefix) throws Exception  {
    AtomicReference<Throwable> thrownExceptionRef = new AtomicReference<>(null);
    GenericTestUtils.waitFor(() -> {
      String strLine;
      long actualCount = 0;
      long idPrefix = -1;
      try (BufferedReader reader =
               new BufferedReader(new FileReader(entityFile))) {
        while ((strLine = reader.readLine()) != null) {
          String entityLine = strLine.trim();
          if (entityLine.isEmpty()) {
            continue;
          }
          if (entityLine.contains(expectedEvent)) {
            actualCount++;
          }
          if (expectedEvent.equals(DSEvent.DS_CONTAINER_END.toString())
              && entityLine.contains(expectedEvent)) {
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
              Assert.assertEquals(
                  "Entity ID prefix should be same across each publish of "
                      + "same entity", idPrefix, entity.getIdPrefix());
            }
          }
        }
      } catch (Throwable e) {
        LOG.error("Exception is waiting on application report", e);
        thrownExceptionRef.set(e);
        return true;
      }
      return (numOfExpectedEvent == actualCount);
    }, sleepTime, (checkTimes + 1) * sleepTime);

    if (thrownExceptionRef.get() != null) {
      Assert.fail("verifyEntityForTimeline failed "
          + thrownExceptionRef.get().getMessage());
    }
  }

  private File verifyEntityTypeFileExists(String basePath, String entityType,
      String entityFileName) {
    String outputDirPathForEntity =
        basePath + File.separator + entityType + File.separator;
    LOG.info("verifyEntityTypeFileExists output path for entityType {}: {}",
        entityType, outputDirPathForEntity);
    File outputDirForEntity = new File(outputDirPathForEntity);
    Assert.assertTrue(outputDirForEntity.isDirectory());
    String entityFilePath = outputDirPathForEntity + entityFileName;
    File entityFile = new File(entityFilePath);
    Assert.assertTrue(entityFile.exists());
    return entityFile;
  }
}
