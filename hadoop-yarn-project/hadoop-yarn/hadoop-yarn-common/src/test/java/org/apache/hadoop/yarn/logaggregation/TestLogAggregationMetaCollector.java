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

package org.apache.hadoop.yarn.logaggregation;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.FakeLogAggregationFileController;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLogAggregationMetaCollector {
  private static final String TEST_NODE = "TEST_NODE_1";
  private static final String TEST_NODE_2 = "TEST_NODE_2";
  private static final String BIG_FILE_NAME = "TEST_BIG";
  private static final String SMALL_FILE_NAME = "TEST_SMALL";

  private static ApplicationId app = ApplicationId.newInstance(
      Clock.systemDefaultZone().millis(), 1);
  private static ApplicationId app2 = ApplicationId.newInstance(
      Clock.systemDefaultZone().millis(), 2);

  private static ApplicationAttemptId appAttempt =
      ApplicationAttemptId.newInstance(app, 1);
  private static ApplicationAttemptId app2Attempt =
      ApplicationAttemptId.newInstance(app2, 1);

  private static ContainerId attemptContainer =
      ContainerId.newContainerId(appAttempt, 1);
  private static ContainerId attemptContainer2 =
      ContainerId.newContainerId(appAttempt, 2);

  private static ContainerId attempt2Container =
      ContainerId.newContainerId(app2Attempt, 1);
  private static ContainerId attempt2Container2 =
      ContainerId.newContainerId(app2Attempt, 2);

  private FakeNodeFileController fileController;

  private static class FakeNodeFileController
      extends FakeLogAggregationFileController {
    private Map<ImmutablePair<String, String>,
        Map<String, List<ContainerLogFileInfo>>> logFiles;
    private List<FileStatus> appDirs;
    private List<FileStatus> nodeFiles;

    FakeNodeFileController(
        Map<ImmutablePair<String, String>, Map<String,
            List<ContainerLogFileInfo>>> logFiles, List<FileStatus> appDirs,
        List<FileStatus> nodeFiles) {
      this.logFiles = logFiles;
      this.appDirs = appDirs;
      this.nodeFiles = nodeFiles;
    }

    @Override
    public RemoteIterator<FileStatus> getApplicationDirectoriesOfUser(
        String user) throws IOException {
      return new RemoteIterator<FileStatus>() {
        private Iterator<FileStatus> iter = appDirs.iterator();

        @Override
        public boolean hasNext() throws IOException {
          return iter.hasNext();
        }

        @Override
        public FileStatus next() throws IOException {
          return iter.next();
        }
      };
    }

    @Override
    public RemoteIterator<FileStatus> getNodeFilesOfApplicationDirectory(
        FileStatus appDir) throws IOException {
      return new RemoteIterator<FileStatus>() {
        private Iterator<FileStatus> iter = nodeFiles.iterator();

        @Override
        public boolean hasNext() throws IOException {
          return iter.hasNext();
        }

        @Override
        public FileStatus next() throws IOException {
          return iter.next();
        }
      };
    }

    @Override
    public Map<String, List<ContainerLogFileInfo>> getLogMetaFilesOfNode(
        ExtendedLogMetaRequest logRequest, FileStatus currentNodeFile,
        ApplicationId appId) throws IOException {
      return logFiles.get(new ImmutablePair<>(appId.toString(),
          currentNodeFile.getPath().getName()));
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    fileController = createFileController();
  }

  @AfterEach
  public void tearDown() throws Exception {
  }

  @Test
  void testAllNull() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    request.setAppId(null);
    request.setContainerId(null);
    request.setFileName(null);
    request.setFileSize(null);
    request.setModificationTime(null);
    request.setNodeId(null);
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(8, allFile.size());
  }

  @Test
  void testAllSet() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    Set<String> fileSizeExpressions = new HashSet<>();
    fileSizeExpressions.add("<51");
    Set<String> modificationTimeExpressions = new HashSet<>();
    modificationTimeExpressions.add("<1000");
    request.setAppId(app.toString());
    request.setContainerId(attemptContainer.toString());
    request.setFileName(String.format("%s.*", SMALL_FILE_NAME));
    request.setFileSize(fileSizeExpressions);
    request.setModificationTime(modificationTimeExpressions);
    request.setNodeId(TEST_NODE);
    request.setUser("TEST");

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(1, allFile.size());
  }

  @Test
  void testSingleNodeRequest() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    request.setAppId(null);
    request.setContainerId(null);
    request.setFileName(null);
    request.setFileSize(null);
    request.setModificationTime(null);
    request.setNodeId(TEST_NODE);
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(4, allFile.stream().
        filter(f -> f.getFileName().contains(TEST_NODE)).count());
  }

  @Test
  void testMultipleNodeRegexRequest() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    request.setAppId(null);
    request.setContainerId(null);
    request.setFileName(null);
    request.setFileSize(null);
    request.setModificationTime(null);
    request.setNodeId("TEST_NODE_.*");
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(8, allFile.size());
  }

  @Test
  void testMultipleFileRegex() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    request.setAppId(null);
    request.setContainerId(null);
    request.setFileName(String.format("%s.*", BIG_FILE_NAME));
    request.setFileSize(null);
    request.setModificationTime(null);
    request.setNodeId(null);
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(4, allFile.size());
    assertTrue(allFile.stream().allMatch(
        f -> f.getFileName().contains(BIG_FILE_NAME)));
  }

  @Test
  void testContainerIdExactMatch() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    request.setAppId(null);
    request.setContainerId(attemptContainer.toString());
    request.setFileName(null);
    request.setFileSize(null);
    request.setModificationTime(null);
    request.setNodeId(null);
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(2, allFile.size());
    assertTrue(allFile.stream().allMatch(
        f -> f.getFileName().contains(attemptContainer.toString())));
  }

  @Test
  void testMultipleFileBetweenSize() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    Set<String> fileSizeExpressions = new HashSet<>();
    fileSizeExpressions.add(">50");
    fileSizeExpressions.add("<101");
    request.setAppId(null);
    request.setContainerId(null);
    request.setFileName(null);
    request.setFileSize(fileSizeExpressions);
    request.setModificationTime(null);
    request.setNodeId(null);
    request.setUser(null);

    LogAggregationMetaCollector collector = new LogAggregationMetaCollector(
        request.build(), new YarnConfiguration());
    List<ContainerLogMeta> res = collector.collect(fileController);

    List<ContainerLogFileInfo> allFile = res.stream()
        .flatMap(m -> m.getContainerLogMeta().stream())
        .collect(Collectors.toList());
    assertEquals(4, allFile.size());
    assertTrue(allFile.stream().allMatch(
        f -> f.getFileSize().equals("100")));
  }

  @Test
  void testInvalidQueryStrings() throws IOException {
    ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder request =
        new ExtendedLogMetaRequest.ExtendedLogMetaRequestBuilder();
    Set<String> fileSizeExpressions = new HashSet<>();
    fileSizeExpressions.add("50");
    fileSizeExpressions.add("101");
    try {
      request.setFileName("*");
      fail("An error should be thrown due to an invalid regex");
    } catch (IllegalArgumentException ignored) {
    }

    try {
      request.setFileSize(fileSizeExpressions);
      fail("An error should be thrown due to multiple exact match expression");
    } catch (IllegalArgumentException ignored) {
    }
  }

  private FakeNodeFileController createFileController() {
    FileStatus appDir = new FileStatus();
    appDir.setPath(new Path(String.format("test/%s", app.toString())));
    FileStatus appDir2 = new FileStatus();
    appDir2.setPath(new Path(String.format("test/%s", app2.toString())));
    List<FileStatus> appDirs = new ArrayList<>();
    appDirs.add(appDir);
    appDirs.add(appDir2);

    FileStatus nodeFile = new FileStatus();
    nodeFile.setPath(new Path(String.format("test/%s", TEST_NODE)));
    FileStatus nodeFile2 = new FileStatus();
    nodeFile2.setPath(new Path(String.format("test/%s", TEST_NODE_2)));
    List<FileStatus> nodeFiles = new ArrayList<>();
    nodeFiles.add(nodeFile);
    nodeFiles.add(nodeFile2);

    Map<ImmutablePair<String, String>, Map<String,
        List<ContainerLogFileInfo>>> internal = new HashMap<>();
    internal.put(new ImmutablePair<>(app.toString(), TEST_NODE),
        createLogFiles(TEST_NODE, attemptContainer));
    internal.put(new ImmutablePair<>(app.toString(), TEST_NODE_2),
        createLogFiles(TEST_NODE_2, attemptContainer2));
    internal.put(new ImmutablePair<>(app2.toString(), TEST_NODE),
        createLogFiles(TEST_NODE, attempt2Container));
    internal.put(new ImmutablePair<>(app2.toString(), TEST_NODE_2),
        createLogFiles(TEST_NODE_2, attempt2Container2));
    return new FakeNodeFileController(internal, appDirs, nodeFiles);
  }

  private Map<String, List<ContainerLogFileInfo>> createLogFiles(
      String nodeId, ContainerId... containerId) {
    Map<String, List<ContainerLogFileInfo>> logFiles = new HashMap<>();
    for (ContainerId c : containerId) {

      List<ContainerLogFileInfo> files = new ArrayList<>();
      ContainerLogFileInfo bigFile = new ContainerLogFileInfo();
      bigFile.setFileName(generateFileName(
          BIG_FILE_NAME, nodeId, c.toString()));
      bigFile.setFileSize("100");
      bigFile.setLastModifiedTime("1000");
      ContainerLogFileInfo smallFile = new ContainerLogFileInfo();
      smallFile.setFileName(generateFileName(
          SMALL_FILE_NAME, nodeId, c.toString()));
      smallFile.setFileSize("50");
      smallFile.setLastModifiedTime("100");
      files.add(bigFile);
      files.add(smallFile);

      logFiles.put(c.toString(), files);
    }
    return logFiles;
  }

  private String generateFileName(
      String name, String nodeId, String containerId) {
    return String.format("%s_%s_%s", name, nodeId, containerId);
  }
}