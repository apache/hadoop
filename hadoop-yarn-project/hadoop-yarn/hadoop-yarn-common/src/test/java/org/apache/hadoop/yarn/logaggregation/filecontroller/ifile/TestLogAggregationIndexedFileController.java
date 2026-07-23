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

package org.apache.hadoop.yarn.logaggregation.filecontroller.ifile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.ExtendedLogMetaRequest;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.TestContainerLogsUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Function test for {@link LogAggregationIndexedFileController}.
 *
 */
public class TestLogAggregationIndexedFileController
      extends Configured {

  private final String rootLocalLogDir = "target/LocalLogs";
  private final Path rootLocalLogDirPath = new Path(rootLocalLogDir);
  private final String remoteLogDir = "target/remote-app";
  private static final FsPermission LOG_FILE_UMASK = FsPermission
      .createImmutable((short) (0777));
  private static final UserGroupInformation USER_UGI = UserGroupInformation
      .createRemoteUser("testUser");
  private static final String ZERO_FILE = "zero";
  private FileSystem fs;
  private ApplicationId appId;
  private ContainerId containerId;
  private NodeId nodeId;

  private ByteArrayOutputStream sysOutStream;

  private Configuration getTestConf() {
    Configuration conf = new Configuration();
    conf.set("yarn.log-aggregation.Indexed.remote-app-log-dir",
            remoteLogDir);
    conf.set("yarn.log-aggregation.Indexed.remote-app-log-dir-suffix",
            "logs");
    conf.set(YarnConfiguration.NM_LOG_AGG_COMPRESSION_TYPE, "gz");
    return conf;
  }

  @BeforeEach
  public void setUp() throws IOException {
    setConf(getTestConf());
    appId = ApplicationId.newInstance(123456, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    containerId = ContainerId.newContainerId(attemptId, 1);
    nodeId = NodeId.newInstance("localhost", 9999);
    fs = FileSystem.get(getConf());
    sysOutStream = new ByteArrayOutputStream();
    PrintStream sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);

    ByteArrayOutputStream sysErrStream = new ByteArrayOutputStream();
    PrintStream sysErr = new PrintStream(sysErrStream);
    System.setErr(sysErr);
  }

  @AfterEach
  public void teardown() throws Exception {
    fs.delete(rootLocalLogDirPath, true);
    fs.delete(new Path(remoteLogDir), true);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testLogAggregationIndexFileFormat() throws Exception {
    if (fs.exists(rootLocalLogDirPath)) {
      fs.delete(rootLocalLogDirPath, true);
    }
    assertTrue(fs.mkdirs(rootLocalLogDirPath));

    Path appLogsDir = new Path(rootLocalLogDirPath, appId.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));

    List<String> logTypes = new ArrayList<String>();
    logTypes.add("syslog");
    logTypes.add("stdout");
    logTypes.add("stderr");

    Set<File> files = new HashSet<>();

    LogKey key1 = new LogKey(containerId.toString());

    for (String logType : logTypes) {
      File file = createAndWriteLocalLogFile(containerId, appLogsDir,
          logType);
      files.add(file);
    }
    files.add(createZeroLocalLogFile(appLogsDir));

    LogValue value = mock(LogValue.class);
    when(value.getPendingLogFilesToUploadForThisContainer()).thenReturn(files);

    final ControlledClock clock = new ControlledClock();
    clock.setTime(System.currentTimeMillis());
    LogAggregationIndexedFileController fileFormat = new LogAggregationIndexedFileController() {
      private int rollOverCheck = 0;

      @Override
      public Clock getSystemClock() {
        return clock;
      }

      @Override
      public boolean isRollover(final FileContext fc, final Path candidate) throws IOException {
        rollOverCheck++;
        if (rollOverCheck >= 3) {
          return true;
        }
        return false;
      }
    };

    fileFormat.initialize(getConf(), "Indexed");

    Map<ApplicationAccessType, String> appAcls = new HashMap<>();
    Path appDir = fileFormat.getRemoteAppLogDir(appId,
        USER_UGI.getShortUserName());
    if (fs.exists(appDir)) {
      fs.delete(appDir, true);
    }
    assertTrue(fs.mkdirs(appDir));

    Path logPath = fileFormat.getRemoteNodeLogFileForApp(
        appId, USER_UGI.getShortUserName(), nodeId);
    LogAggregationFileControllerContext context =
        new LogAggregationFileControllerContext(
            logPath, logPath, true, 1000, appId, appAcls, nodeId, USER_UGI);
    // initialize the writer
    fileFormat.initializeWriter(context);

    fileFormat.write(key1, value);
    fileFormat.postWrite(context);
    fileFormat.closeWriter();

    ContainerLogsRequest logRequest = new ContainerLogsRequest();
    logRequest.setAppId(appId);
    logRequest.setNodeId(nodeId.toString());
    logRequest.setAppOwner(USER_UGI.getShortUserName());
    logRequest.setContainerId(containerId.toString());
    logRequest.setBytes(Long.MAX_VALUE);
    List<ContainerLogMeta> meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertEquals(1, meta.size());
    List<String> fileNames = new ArrayList<>();
    for (ContainerLogMeta log : meta) {
      assertEquals(containerId.toString(), log.getContainerId());
      assertEquals(nodeId.toString(), log.getNodeId());
      assertEquals(4, log.getContainerLogMeta().size());
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(logTypes);
    fileNames.remove(ZERO_FILE);
    assertTrue(fileNames.isEmpty());

    boolean foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : logTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    assertZeroFileIsContained(sysOutStream.toString());
    sysOutStream.reset();

    Configuration factoryConf = new Configuration(getConf());
    factoryConf.set("yarn.log-aggregation.file-formats", "Indexed");
    factoryConf.set("yarn.log-aggregation.file-controller.Indexed.class",
        "org.apache.hadoop.yarn.logaggregation.filecontroller.ifile"
            + ".LogAggregationIndexedFileController");
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(factoryConf);
    LogAggregationFileController fileController = factory
        .getFileControllerForRead(appId, USER_UGI.getShortUserName());
    assertTrue(fileController instanceof
        LogAggregationIndexedFileController);
    foundLogs = fileController.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : logTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    sysOutStream.reset();

    // create a checksum file
    Path checksumFile = new Path(fileFormat.getRemoteAppLogDir(
            appId, USER_UGI.getShortUserName()),
        LogAggregationUtils.getNodeString(nodeId)
            + LogAggregationIndexedFileController.CHECK_SUM_FILE_SUFFIX);
    FSDataOutputStream fInput = null;
    try {
      String nodeName = logPath.getName() + "_" + clock.getTime();
      fInput = FileSystem.create(fs, checksumFile, LOG_FILE_UMASK);
      fInput.writeInt(nodeName.length());
      fInput.write(nodeName.getBytes(
          StandardCharsets.UTF_8));
      fInput.writeLong(0);
    } finally {
      IOUtils.closeStream(fInput);
    }
    meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertTrue(meta.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertFalse(foundLogs);
    sysOutStream.reset();
    fs.delete(checksumFile, false);
    assertFalse(fs.exists(checksumFile));

    List<String> newLogTypes = new ArrayList<>(logTypes);
    files.clear();
    newLogTypes.add("test1");
    files.add(createAndWriteLocalLogFile(containerId, appLogsDir,
        "test1"));
    newLogTypes.add("test2");
    files.add(createAndWriteLocalLogFile(containerId, appLogsDir,
        "test2"));
    LogValue value2 = mock(LogValue.class);
    when(value2.getPendingLogFilesToUploadForThisContainer())
        .thenReturn(files);

    // initialize the writer
    fileFormat.initializeWriter(context);
    fileFormat.write(key1, value2);
    fileFormat.closeWriter();

    // We did not call postWriter which we would keep the checksum file.
    // We can only get the logs/logmeta from the first write.
    meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertThat(meta.size()).isEqualTo(1);
    for (ContainerLogMeta log : meta) {
      assertEquals(containerId.toString(), log.getContainerId());
      assertEquals(nodeId.toString(), log.getNodeId());
      assertEquals(4, log.getContainerLogMeta().size());
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(logTypes);
    fileNames.remove(ZERO_FILE);
    assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : logTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    assertFalse(sysOutStream.toString().contains(logMessage(
        containerId, "test1")));
    assertFalse(sysOutStream.toString().contains(logMessage(
        containerId, "test2")));
    sysOutStream.reset();

    // Call postWrite and we should get all logs/logmetas for both
    // first write and second write
    fileFormat.initializeWriter(context);
    fileFormat.write(key1, value2);
    fileFormat.postWrite(context);
    fileFormat.closeWriter();
    meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertThat(meta.size()).isEqualTo(2);
    for (ContainerLogMeta log : meta) {
      assertEquals(containerId.toString(), log.getContainerId());
      assertEquals(nodeId.toString(), log.getNodeId());
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(newLogTypes);
    fileNames.remove(ZERO_FILE);
    assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : newLogTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    sysOutStream.reset();

    // start to roll over old logs
    clock.setTime(System.currentTimeMillis());
    fileFormat.initializeWriter(context);
    fileFormat.write(key1, value2);
    fileFormat.postWrite(context);
    fileFormat.closeWriter();
    FileStatus[] status = fs.listStatus(logPath.getParent());
    assertEquals(2, status.length);
    meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertThat(meta.size()).isEqualTo(3);
    for (ContainerLogMeta log : meta) {
      assertEquals(containerId.toString(), log.getContainerId());
      assertEquals(nodeId.toString(), log.getNodeId());
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(newLogTypes);
    fileNames.remove(ZERO_FILE);
    assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : newLogTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    sysOutStream.reset();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testFetchApplicationLogsHar() throws Exception {
    List<String> newLogTypes = new ArrayList<>();
    newLogTypes.add("syslog");
    newLogTypes.add("stdout");
    newLogTypes.add("stderr");
    newLogTypes.add("test1");
    newLogTypes.add("test2");
    URL harUrl = ClassLoader.getSystemClassLoader()
        .getResource("application_123456_0001.har");
    assertNotNull(harUrl);

    Path path = new Path(remoteLogDir + "/" + USER_UGI.getShortUserName()
        + "/logs/application_123456_0001");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    assertTrue(fs.mkdirs(path));
    Path harPath = new Path(path, "application_123456_0001.har");
    fs.copyFromLocalFile(false, new Path(harUrl.toURI()), harPath);
    assertTrue(fs.exists(harPath));
    LogAggregationIndexedFileController fileFormat
        = new LogAggregationIndexedFileController();
    fileFormat.initialize(getConf(), "Indexed");
    ContainerLogsRequest logRequest = new ContainerLogsRequest();
    logRequest.setAppId(appId);
    logRequest.setNodeId(nodeId.toString());
    logRequest.setAppOwner(USER_UGI.getShortUserName());
    logRequest.setContainerId(containerId.toString());
    logRequest.setBytes(Long.MAX_VALUE);
    List<ContainerLogMeta> meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    assertEquals(3, meta.size());
    List<String> fileNames = new ArrayList<>();
    for (ContainerLogMeta log : meta) {
      assertEquals(containerId.toString(), log.getContainerId());
      assertEquals(nodeId.toString(), log.getNodeId());
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(newLogTypes);
    assertTrue(fileNames.isEmpty());
    boolean foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    assertTrue(foundLogs);
    for (String logType : newLogTypes) {
      assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    sysOutStream.reset();
  }

  private void assertZeroFileIsContained(String outStream) {
    assertTrue(outStream.contains(
        "LogContents:\n" +
        "\n" +
        "End of LogType:zero"));
  }

  private File createZeroLocalLogFile(Path localLogDir) throws IOException {
    return createAndWriteLocalLogFile(localLogDir, ZERO_FILE, "");
  }

  private File createAndWriteLocalLogFile(ContainerId containerId,
      Path localLogDir, String logType) throws IOException {
    return createAndWriteLocalLogFile(localLogDir, logType,
        logMessage(containerId, logType));
  }

  private File createAndWriteLocalLogFile(Path localLogDir, String logType,
      String message) throws IOException {
    File file = new File(localLogDir.toString(), logType);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    Writer writer = null;
    try {
      writer = new FileWriter(file);
      writer.write(message);
      writer.close();
      return file;
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  private String logMessage(ContainerId containerId, String logType) {
    return "Hello " + containerId + " in " + logType + "!";
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testReadAggregatedLogsMetaForMultipleAppsWithReusedController()
      throws Exception {
    TwoAppIFileFixture fixture = prepareTwoAppIFileFixture();
    LogAggregationFileController controller =
        fixture.factory.getFileControllerForRead(fixture.appId1, fixture.user);

    assertReadAggregatedLogsMeta(controller, fixture.appId1, fixture.user,
        fixture.containerId1);
    assertReadAggregatedLogsMeta(controller, fixture.appId2, fixture.user,
        fixture.containerId2);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testGetApplicationOwnerAndAclsForMultipleAppsWithReusedController()
      throws Exception {
    TwoAppIFileFixture fixture = prepareTwoAppIFileFixture();
    LogAggregationFileController controller =
        fixture.factory.getFileControllerForRead(fixture.appId1, fixture.user);

    Path app1LogFile = findAggregatedLogFile(
        controller.getRemoteAppLogDir(fixture.appId1, fixture.user));
    Path app2LogFile = findAggregatedLogFile(
        controller.getRemoteAppLogDir(fixture.appId2, fixture.user));

    assertEquals(fixture.user,
        controller.getApplicationOwner(app1LogFile, fixture.appId1),
        "Application owner mismatch for app1");
    assertNotNull(controller.getApplicationAcls(app1LogFile, fixture.appId1),
        "Application ACLs should not be null for app1");
    assertEquals(fixture.user,
        controller.getApplicationOwner(app2LogFile, fixture.appId2),
        "Application owner mismatch for app2");
    assertNotNull(controller.getApplicationAcls(app2LogFile, fixture.appId2),
        "Application ACLs should not be null for app2");
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testReadAggregatedLogsMetaReverseOrderWithReusedController()
      throws Exception {
    TwoAppIFileFixture fixture = prepareTwoAppIFileFixture();
    LogAggregationFileController controller =
        fixture.factory.getFileControllerForRead(fixture.appId1, fixture.user);

    assertReadAggregatedLogsMeta(controller, fixture.appId2, fixture.user,
        fixture.containerId2);
    assertReadAggregatedLogsMeta(controller, fixture.appId1, fixture.user,
        fixture.containerId1);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testConcurrentReadAggregatedLogsMetaWithReusedController()
      throws Exception {
    TwoAppIFileFixture fixture = prepareTwoAppIFileFixture();
    LogAggregationFileController controller =
        fixture.factory.getFileControllerForRead(fixture.appId1, fixture.user);

    ContainerLogsRequest app1Request = new ContainerLogsRequest();
    app1Request.setAppId(fixture.appId1);
    app1Request.setAppOwner(fixture.user);
    ContainerLogsRequest app2Request = new ContainerLogsRequest();
    app2Request.setAppId(fixture.appId2);
    app2Request.setAppOwner(fixture.user);

    // Use a barrier to guarantee both threads are in-flight simultaneously.
    CountDownLatch barrier = new CountDownLatch(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<List<ContainerLogMeta>> app1Future = executor.submit(() -> {
        barrier.countDown();
        barrier.await();
        return controller.readAggregatedLogsMeta(app1Request);
      });
      Future<List<ContainerLogMeta>> app2Future = executor.submit(() -> {
        barrier.countDown();
        barrier.await();
        return controller.readAggregatedLogsMeta(app2Request);
      });
      List<ContainerLogMeta> app1Meta = app1Future.get();
      List<ContainerLogMeta> app2Meta = app2Future.get();
      assertEquals(1, app1Meta.size(),
          "Expected one container log meta entry for app1");
      assertEquals(fixture.containerId1.toString(), app1Meta.get(0).getContainerId(),
          "Unexpected container id for app1");
      assertEquals(1, app2Meta.size(),
          "Expected one container log meta entry for app2");
      assertEquals(fixture.containerId2.toString(), app2Meta.get(0).getContainerId(),
          "Unexpected container id for app2");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testWriteSessionResetForMultipleAppsWithReusedController()
      throws Exception {
    Configuration ifileConf = newIFileConfiguration();
    ApplicationId appId1 = ApplicationId.newInstance(20, 1);
    ApplicationId appId2 = ApplicationId.newInstance(20, 2);
    ContainerId containerId1 = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId1, 1), 1);
    ContainerId containerId2 = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId2, 1), 1);
    String user = USER_UGI.getShortUserName();

    LogAggregationIndexedFileController controller =
        new LogAggregationIndexedFileController();
    controller.initialize(ifileConf, "IFile");
    uploadAppLogsWithController(controller, appId1, containerId1, user, true);
    uploadAppLogsWithController(controller, appId2, containerId2, user, false);

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(ifileConf);
    LogAggregationFileController reader =
        factory.getFileControllerForRead(appId1, user);
    assertReadAggregatedLogsMeta(reader, appId1, user, containerId1);
    assertReadAggregatedLogsMeta(reader, appId2, user, containerId2);
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  void testLoadUUIDFromLogFilePreservesValidAggregatedLogFile()
      throws Exception {
    Configuration ifileConf = newIFileConfiguration();
    ApplicationId appId1 = ApplicationId.newInstance(30, 1);
    ContainerId containerId1 = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId1, 1), 1);
    String user = USER_UGI.getShortUserName();

    LogAggregationIndexedFileController writer1 =
        new LogAggregationIndexedFileController();
    writer1.initialize(ifileConf, "IFile");
    uploadAppLogsWithController(writer1, appId1, containerId1, user, true);

    Path logFile = findAggregatedLogFile(
        writer1.getRemoteAppLogDir(appId1, user));
    assertTrue(fs.exists(logFile),
        "Aggregated log file should exist before rolling init");

    LogAggregationIndexedFileController writer2 =
        new LogAggregationIndexedFileController();
    writer2.initialize(ifileConf, "IFile");
    Path nodePath = new Path(writer2.getRemoteAppLogDir(appId1, user),
        LogAggregationUtils.getNodeString(nodeId));
    Map<ApplicationAccessType, String> appAcls = new HashMap<>();
    LogAggregationFileControllerContext context =
        new LogAggregationFileControllerContext(
            nodePath, nodePath, true, 1000, appId1, appAcls, nodeId, USER_UGI);
    writer2.initializeWriter(context);
    writer2.closeWriter();

    assertTrue(fs.exists(logFile),
        "Aggregated log file should exist after rolling init");
    ContainerLogsRequest request = new ContainerLogsRequest();
    request.setAppId(appId1);
    request.setAppOwner(user);
    assertEquals(1, writer1.readAggregatedLogsMeta(request).size(),
        "Expected one container log meta entry after rolling init");
  }

  private Configuration newIFileConfiguration() {
    Configuration ifileConf = new YarnConfiguration();
    ifileConf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    ifileConf.setStrings(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "IFile");
    ifileConf.setClass(String.format(
        YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT, "IFile"),
        LogAggregationIndexedFileController.class,
        LogAggregationFileController.class);
    ifileConf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogDir);
    ifileConf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, "logs");
    return ifileConf;
  }

  private static final class TwoAppIFileFixture {
    private final LogAggregationFileControllerFactory factory;
    private final ApplicationId appId1;
    private final ApplicationId appId2;
    private final ContainerId containerId1;
    private final ContainerId containerId2;
    private final String user;

    private TwoAppIFileFixture(LogAggregationFileControllerFactory factory,
        ApplicationId appId1, ApplicationId appId2, ContainerId containerId1,
        ContainerId containerId2, String user) {
      this.factory = factory;
      this.appId1 = appId1;
      this.appId2 = appId2;
      this.containerId1 = containerId1;
      this.containerId2 = containerId2;
      this.user = user;
    }
  }

  private TwoAppIFileFixture prepareTwoAppIFileFixture() throws Exception {
    Configuration ifileConf = newIFileConfiguration();
    // Use cluster-time 100/101 to avoid colliding with the IDs used by
    // TestHsWebServicesIFileAggregatedLogs (1,1) and (10,2).
    ApplicationId appId1 = ApplicationId.newInstance(100, 1);
    ApplicationId appId2 = ApplicationId.newInstance(101, 2);
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(appId1, 1);
    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(appId2, 1);
    ContainerId containerId1 =
        ContainerId.newContainerId(attemptId1, 1);
    ContainerId containerId2 =
        ContainerId.newContainerId(attemptId2, 1);
    String user = USER_UGI.getShortUserName();
    String fileName = "syslog";

    Map<ContainerId, String> app1Logs = new HashMap<>();
    app1Logs.put(containerId1, logMessage(containerId1, fileName));
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(ifileConf, fs,
        rootLocalLogDir, appId1, app1Logs, nodeId, fileName, user, true);
    Map<ContainerId, String> app2Logs = new HashMap<>();
    app2Logs.put(containerId2, logMessage(containerId2, fileName));
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(ifileConf, fs,
        rootLocalLogDir, appId2, app2Logs, nodeId, fileName, user, false);

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(ifileConf);
    return new TwoAppIFileFixture(factory, appId1, appId2, containerId1,
        containerId2, user);
  }

  private void assertReadAggregatedLogsMeta(
      LogAggregationFileController controller, ApplicationId targetAppId, String user,
      ContainerId expectedContainerId) throws IOException {
    ContainerLogsRequest request = new ContainerLogsRequest();
    request.setAppId(targetAppId);
    request.setAppOwner(user);
    List<ContainerLogMeta> meta = controller.readAggregatedLogsMeta(request);
    assertEquals(1, meta.size(), "Expected one container log meta entry");
    assertEquals(expectedContainerId.toString(), meta.get(0).getContainerId(),
        "Unexpected container id");
  }

  private void uploadAppLogsWithController(
      LogAggregationIndexedFileController controller, ApplicationId targetAppId,
      ContainerId targetContainerId, String user, boolean deleteRemoteLogDir)
      throws Exception {
    Path localAppDir = new Path(rootLocalLogDirPath, targetAppId.toString());
    if (fs.exists(localAppDir)) {
      fs.delete(localAppDir, true);
    }
    assertTrue(fs.mkdirs(localAppDir), "Failed to create local app log directory");
    Path containerLogDir = new Path(localAppDir, targetContainerId.toString());
    assertTrue(fs.mkdirs(containerLogDir),
        "Failed to create local container log directory");
    createAndWriteLocalLogFile(containerLogDir, "syslog",
        logMessage(targetContainerId, "syslog"));

    List<String> rootLogDirList = Collections.singletonList(rootLocalLogDir);
    Path appDir = controller.getRemoteAppLogDir(targetAppId, user);
    if (fs.exists(appDir) && deleteRemoteLogDir) {
      fs.delete(appDir, true);
    }
    assertTrue(fs.mkdirs(appDir), "Failed to create remote app log directory");
    Path nodePath = new Path(appDir, LogAggregationUtils.getNodeString(nodeId));

    Map<ApplicationAccessType, String> appAcls = new HashMap<>();
    appAcls.put(ApplicationAccessType.VIEW_APP, user);
    LogAggregationFileControllerContext context =
        new LogAggregationFileControllerContext(
            nodePath, nodePath, true, 1000, targetAppId, appAcls, nodeId, USER_UGI);
    try {
      controller.initializeWriter(context);
      controller.write(new LogKey(targetContainerId.toString()),
          new LogValue(rootLogDirList, targetContainerId, user));
      controller.postWrite(context);
    } finally {
      controller.closeWriter();
    }
  }

  private Path findAggregatedLogFile(Path appDir) throws IOException {
    for (FileStatus nodeDir : fs.listStatus(appDir)) {
      String nodeName = nodeDir.getPath().getName();
      if (nodeName.endsWith(LogAggregationIndexedFileController
          .CHECK_SUM_FILE_SUFFIX)) {
        continue;
      }
      for (FileStatus logFile : fs.listStatus(nodeDir.getPath())) {
        if (!logFile.getPath().getName().endsWith(
            LogAggregationIndexedFileController.CHECK_SUM_FILE_SUFFIX)) {
          return logFile.getPath();
        }
      }
    }
    throw new IOException("No aggregated log file found under " + appDir);
  }

  @Test
  void testGetRollOverLogMaxSize() {
    String fileControllerName = "testController";
    String remoteDirConf = String.format(
        YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
        fileControllerName);
    Configuration conf = new Configuration();
    LogAggregationIndexedFileController fileFormat
        = new LogAggregationIndexedFileController();
    long defaultRolloverSize = 10L * 1024 * 1024 * 1024;

    // test local filesystem
    fileFormat.initialize(conf, fileControllerName);
    assertThat(fileFormat.getRollOverLogMaxSize(conf))
        .isEqualTo(defaultRolloverSize);

    // test file system supporting append
    conf.set(remoteDirConf, "webhdfs://localhost/path");
    fileFormat.initialize(conf, fileControllerName);
    assertThat(fileFormat.getRollOverLogMaxSize(conf))
        .isEqualTo(defaultRolloverSize);

    // test file system not supporting append
    conf.set(remoteDirConf, "s3a://test/path");
    fileFormat.initialize(conf, fileControllerName);
    assertThat(fileFormat.getRollOverLogMaxSize(conf)).isZero();
  }

  @Test
  void testGetLogMetaFilesOfNode() throws Exception {
    if (fs.exists(rootLocalLogDirPath)) {
      fs.delete(rootLocalLogDirPath, true);
    }
    assertTrue(fs.mkdirs(rootLocalLogDirPath));

    Path appLogsDir = new Path(rootLocalLogDirPath, appId.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));

    List<String> logTypes = new ArrayList<String>();
    logTypes.add("syslog");
    logTypes.add("stdout");
    logTypes.add("stderr");

    Set<File> files = new HashSet<>();

    LogKey key1 = new LogKey(containerId.toString());

    for (String logType : logTypes) {
      File file = createAndWriteLocalLogFile(containerId, appLogsDir,
          logType);
      files.add(file);
    }
    files.add(createZeroLocalLogFile(appLogsDir));

    LogValue value = mock(LogValue.class);
    when(value.getPendingLogFilesToUploadForThisContainer()).thenReturn(files);

    LogAggregationIndexedFileController fileFormat =
        new LogAggregationIndexedFileController();

    fileFormat.initialize(getConf(), "Indexed");

    Map<ApplicationAccessType, String> appAcls = new HashMap<>();
    Path appDir = fileFormat.getRemoteAppLogDir(appId,
        USER_UGI.getShortUserName());
    if (fs.exists(appDir)) {
      fs.delete(appDir, true);
    }
    assertTrue(fs.mkdirs(appDir));

    Path logPath = fileFormat.getRemoteNodeLogFileForApp(
        appId, USER_UGI.getShortUserName(), nodeId);
    LogAggregationFileControllerContext context =
        new LogAggregationFileControllerContext(
            logPath, logPath, true, 1000, appId, appAcls, nodeId, USER_UGI);
    // initialize the writer
    fileFormat.initializeWriter(context);

    fileFormat.write(key1, value);
    fileFormat.postWrite(context);
    fileFormat.closeWriter();

    ContainerLogsRequest logRequest = new ContainerLogsRequest();
    logRequest.setAppId(appId);
    logRequest.setNodeId(nodeId.toString());
    logRequest.setAppOwner(USER_UGI.getShortUserName());
    logRequest.setContainerId(containerId.toString());
    logRequest.setBytes(Long.MAX_VALUE);
    // create a checksum file
    final ControlledClock clock = new ControlledClock();
    clock.setTime(System.currentTimeMillis());
    Path checksumFile = new Path(fileFormat.getRemoteAppLogDir(
            appId, USER_UGI.getShortUserName()),
        LogAggregationUtils.getNodeString(nodeId)
            + LogAggregationIndexedFileController.CHECK_SUM_FILE_SUFFIX);
    FSDataOutputStream fInput = null;
    try {
      String nodeName = logPath.getName() + "_" + clock.getTime();
      fInput = FileSystem.create(fs, checksumFile, LOG_FILE_UMASK);
      fInput.writeInt(nodeName.length());
      fInput.write(nodeName.getBytes(
          StandardCharsets.UTF_8));
      fInput.writeLong(0);
    } finally {
      IOUtils.closeStream(fInput);
    }

    Path nodePath = LogAggregationUtils.getRemoteAppLogDir(
        fileFormat.getRemoteRootLogDir(), appId, USER_UGI.getShortUserName(),
        fileFormat.getRemoteRootLogDirSuffix());
    FileStatus[] nodes = fs.listStatus(nodePath);
    ExtendedLogMetaRequest req =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder().build();
    for (FileStatus node : nodes) {
      Map<String, List<ContainerLogFileInfo>> metas =
          fileFormat.getLogMetaFilesOfNode(req, node, appId);

      if (node.getPath().getName().contains(
          LogAggregationIndexedFileController.CHECK_SUM_FILE_SUFFIX)) {
        assertTrue(metas.isEmpty(),
            "Checksum node files should not contain any logs");
      } else {
        assertFalse(metas.isEmpty(),
            "Non-checksum node files should contain log files");
        assertEquals(4, metas.values().stream().findFirst().get().size());
      }
    }

  }
}
