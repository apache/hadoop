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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
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

  @Before
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

  @After
  public void teardown() throws Exception {
    fs.delete(rootLocalLogDirPath, true);
    fs.delete(new Path(remoteLogDir), true);
  }

  @Test(timeout = 15000)
  public void testLogAggregationIndexFileFormat() throws Exception {
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

    for(String logType : logTypes) {
      File file = createAndWriteLocalLogFile(containerId, appLogsDir,
          logType);
      files.add(file);
    }
    files.add(createZeroLocalLogFile(appLogsDir));

    LogValue value = mock(LogValue.class);
    when(value.getPendingLogFilesToUploadForThisContainer()).thenReturn(files);

    final ControlledClock clock = new ControlledClock();
    clock.setTime(System.currentTimeMillis());
    LogAggregationIndexedFileController fileFormat
        = new LogAggregationIndexedFileController() {
          private int rollOverCheck = 0;
          @Override
          public Clock getSystemClock() {
            return clock;
          }

          @Override
          public boolean isRollover(final FileContext fc,
              final Path candidate) throws IOException {
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
          Charset.forName("UTF-8")));
      fInput.writeLong(0);
    } finally {
      IOUtils.closeQuietly(fInput);
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

  @Test(timeout = 15000)
  public void testFetchApplictionLogsHar() throws Exception {
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
      IOUtils.closeQuietly(writer);
    }
  }

  private String logMessage(ContainerId containerId, String logType) {
    return "Hello " + containerId + " in " + logType + "!";
  }

  @Test
  public void testGetRollOverLogMaxSize() {
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
}
