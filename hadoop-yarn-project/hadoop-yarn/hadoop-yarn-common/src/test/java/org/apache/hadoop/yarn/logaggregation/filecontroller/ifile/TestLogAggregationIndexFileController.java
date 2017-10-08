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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Function test for {@link LogAggregationIndexFileController}.
 *
 */
public class TestLogAggregationIndexFileController {

  private final String rootLocalLogDir = "target/LocalLogs";
  private final Path rootLocalLogDirPath = new Path(rootLocalLogDir);
  private final String remoteLogDir = "target/remote-app";
  private static final FsPermission LOG_FILE_UMASK = FsPermission
      .createImmutable((short) (0777));
  private static final UserGroupInformation USER_UGI = UserGroupInformation
      .createRemoteUser("testUser");
  private FileSystem fs;
  private Configuration conf;
  private ApplicationId appId;
  private ContainerId containerId;
  private NodeId nodeId;

  private ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;

  private ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  @Before
  public void setUp() throws IOException {
    appId = ApplicationId.newInstance(123456, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    containerId = ContainerId.newContainerId(attemptId, 1);
    nodeId = NodeId.newInstance("localhost", 9999);
    conf = new Configuration();
    conf.set("yarn.log-aggregation.Indexed.remote-app-log-dir",
        remoteLogDir);
    conf.set("yarn.log-aggregation.Indexed.remote-app-log-dir-suffix",
        "logs");
    conf.set(YarnConfiguration.NM_LOG_AGG_COMPRESSION_TYPE, "gz");
    fs = FileSystem.get(conf);
    sysOutStream = new ByteArrayOutputStream();
    sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);

    sysErrStream = new ByteArrayOutputStream();
    sysErr = new PrintStream(sysErrStream);
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

    fileFormat.initialize(conf, "Indexed");

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
    Assert.assertTrue(meta.size() == 1);
    List<String> fileNames = new ArrayList<>();
    for (ContainerLogMeta log : meta) {
      Assert.assertTrue(log.getContainerId().equals(containerId.toString()));
      Assert.assertTrue(log.getNodeId().equals(nodeId.toString()));
      Assert.assertTrue(log.getContainerLogMeta().size() == 3);
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(logTypes);
    Assert.assertTrue(fileNames.isEmpty());

    boolean foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    Assert.assertTrue(foundLogs);
    for (String logType : logTypes) {
      Assert.assertTrue(sysOutStream.toString().contains(logMessage(
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
    Assert.assertTrue(meta.size() == 0);
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    Assert.assertFalse(foundLogs);
    sysOutStream.reset();
    fs.delete(checksumFile, false);
    Assert.assertFalse(fs.exists(checksumFile));

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
    Assert.assertEquals(meta.size(), 1);
    for (ContainerLogMeta log : meta) {
      Assert.assertTrue(log.getContainerId().equals(containerId.toString()));
      Assert.assertTrue(log.getNodeId().equals(nodeId.toString()));
      Assert.assertTrue(log.getContainerLogMeta().size() == 3);
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(logTypes);
    Assert.assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    Assert.assertTrue(foundLogs);
    for (String logType : logTypes) {
      Assert.assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    Assert.assertFalse(sysOutStream.toString().contains(logMessage(
        containerId, "test1")));
    Assert.assertFalse(sysOutStream.toString().contains(logMessage(
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
    Assert.assertEquals(meta.size(), 2);
    for (ContainerLogMeta log : meta) {
      Assert.assertTrue(log.getContainerId().equals(containerId.toString()));
      Assert.assertTrue(log.getNodeId().equals(nodeId.toString()));
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(newLogTypes);
    Assert.assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    Assert.assertTrue(foundLogs);
    for (String logType : newLogTypes) {
      Assert.assertTrue(sysOutStream.toString().contains(logMessage(
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
    Assert.assertTrue(status.length == 2);
    meta = fileFormat.readAggregatedLogsMeta(
        logRequest);
    Assert.assertEquals(meta.size(), 3);
    for (ContainerLogMeta log : meta) {
      Assert.assertTrue(log.getContainerId().equals(containerId.toString()));
      Assert.assertTrue(log.getNodeId().equals(nodeId.toString()));
      for (ContainerLogFileInfo file : log.getContainerLogMeta()) {
        fileNames.add(file.getFileName());
      }
    }
    fileNames.removeAll(newLogTypes);
    Assert.assertTrue(fileNames.isEmpty());
    foundLogs = fileFormat.readAggregatedLogs(logRequest, System.out);
    Assert.assertTrue(foundLogs);
    for (String logType : newLogTypes) {
      Assert.assertTrue(sysOutStream.toString().contains(logMessage(
          containerId, logType)));
    }
    sysOutStream.reset();
  }

  private File createAndWriteLocalLogFile(ContainerId containerId,
      Path localLogDir, String logType) throws IOException {
    File file = new File(localLogDir.toString(), logType);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    Writer writer = null;
    try {
      writer = new FileWriter(file);
      writer.write(logMessage(containerId, logType));
      writer.close();
      return file;
    } finally {
      IOUtils.closeQuietly(writer);
    }
  }

  private String logMessage(ContainerId containerId, String logType) {
    StringBuilder sb = new StringBuilder();
    sb.append("Hello " + containerId + " in " + logType + "!");
    return sb.toString();
  }
}
