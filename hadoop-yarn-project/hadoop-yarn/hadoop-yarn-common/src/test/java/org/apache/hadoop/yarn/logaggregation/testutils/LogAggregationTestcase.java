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

package org.apache.hadoop.yarn.logaggregation.testutils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService.LogDeletionTask;
import org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcaseBuilder.AppDescriptor;

import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.createDirBucketDirLogPathWithFileStatus;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.createDirLogPathWithFileStatus;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.createFileLogPathWithFileStatus;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.createPathWithFileStatusForAppId;
import static org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcaseBuilder.NO_TIMEOUT;
import static org.apache.hadoop.yarn.logaggregation.testutils.MockRMClientUtils.createMockRMClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogAggregationTestcase {
  private static final Logger LOG = LoggerFactory.getLogger(LogAggregationTestcase.class);

  private final Configuration conf;
  private final long now;
  private PathWithFileStatus bucketDir;
  private final long bucketDirModTime;
  private PathWithFileStatus userDir;
  private final String userDirName;
  private final long userDirModTime;
  private PathWithFileStatus suffixDir;
  private final String suffix;
  private final String suffixDirName;
  private final long suffixDirModTime;
  private final String bucketId;
  private final Path remoteRootLogPath;
  private final Map<Integer, Exception> injectedAppDirDeletionExceptions;
  private final List<String> fileControllers;
  private final List<Pair<String, Long>> additionalAppDirs;

  private final List<ApplicationId> applicationIds = new ArrayList<>();
  private final int[] runningAppIds;
  private final int[] finishedAppIds;
  private final List<List<PathWithFileStatus>> appFiles = new ArrayList<>();
  private final FileSystem mockFs;
  private List<PathWithFileStatus> appDirs;
  private final List<AppDescriptor> appDescriptors;
  private AggregatedLogDeletionServiceForTest deletionService;
  private ApplicationClientProtocol rmClient;

  public LogAggregationTestcase(LogAggregationTestcaseBuilder builder) throws IOException {
    conf = builder.conf;
    now = builder.now;
    bucketDir = builder.bucketDir;
    bucketDirModTime = builder.bucketDirModTime;
    userDir = builder.userDir;
    userDirName = builder.userDirName;
    userDirModTime = builder.userDirModTime;
    suffix = builder.suffix;
    suffixDir = builder.suffixDir;
    suffixDirName = builder.suffixDirName;
    suffixDirModTime = builder.suffixDirModTime;
    bucketId = builder.bucketId;
    appDescriptors = builder.apps;
    runningAppIds = builder.runningAppIds;
    finishedAppIds = builder.finishedAppIds;
    remoteRootLogPath = builder.remoteRootLogPath;
    injectedAppDirDeletionExceptions = builder.injectedAppDirDeletionExceptions;
    fileControllers = builder.fileControllers;
    additionalAppDirs = builder.additionalAppDirs;

    mockFs = ((FilterFileSystem) builder.rootFs).getRawFileSystem();
    validateAppControllers();
    setupMocks();

    setupDeletionService();
  }

  private void validateAppControllers() {
    Set<String> controllers = appDescriptors.stream()
            .map(a -> a.fileController)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    Set<String> availableControllers = fileControllers != null ?
            new HashSet<>(this.fileControllers) : Sets.newHashSet();
    Set<String> difference = Sets.difference(controllers, availableControllers);
    if (!difference.isEmpty()) {
      throw new IllegalStateException(String.format("Invalid controller defined!" +
                      " Available: %s, Actual: %s", availableControllers, controllers));
    }
  }

  private void setupMocks() throws IOException {
    createApplicationsByDescriptors();

    List<Path> rootPaths = determineRootPaths();
    for (Path rootPath : rootPaths) {
      String controllerName = rootPath.getName();
      ApplicationId arbitraryAppIdForBucketDir = this.applicationIds.get(0);
      userDir = createDirLogPathWithFileStatus(rootPath, userDirName, userDirModTime);
      suffixDir = createDirLogPathWithFileStatus(userDir.path, suffixDirName, suffixDirModTime);
      if (bucketId != null) {
        bucketDir = createDirLogPathWithFileStatus(suffixDir.path, bucketId, bucketDirModTime);
      } else {
        bucketDir = createDirBucketDirLogPathWithFileStatus(rootPath, userDirName, suffix,
                arbitraryAppIdForBucketDir, bucketDirModTime);
      }
      setupListStatusForPath(rootPath, userDir);
      initFileSystemListings(controllerName);
    }
  }

  private List<Path> determineRootPaths() {
    List<Path> rootPaths = new ArrayList<>();
    if (fileControllers != null && !fileControllers.isEmpty()) {
      for (String fileController : fileControllers) {
        //Generic path: <remote-app-log-dir>/<user>/bucket-<suffix>/<bucket id>/
        // <application id>/<NodeManager id>

        //remoteRootLogPath: <remote-app-log-dir>/
        //example: mockfs://foo/tmp/logs/

        //userDir: <remote-app-log-dir>/<user>/
        //example: mockfs://foo/tmp/logs/me/

        //suffixDir: <remote-app-log-dir>/<user>/bucket-<suffix>/
        //example: mockfs://foo/tmp/logs/me/bucket-logs/

        //bucketDir: <remote-app-log-dir>/<user>/bucket-<suffix>/<bucket id>/
        //example: mockfs://foo/tmp/logs/me/bucket-logs/0001/

        //remoteRootLogPath with controller: <remote-app-log-dir>/<controllerName>
        //example: mockfs://foo/tmp/logs/IFile
        rootPaths.add(new Path(remoteRootLogPath, fileController));
      }
    } else {
      rootPaths.add(remoteRootLogPath);
    }
    return rootPaths;
  }

  private void initFileSystemListings(String controllerName) throws IOException {
    setupListStatusForPath(userDir, suffixDir);
    setupListStatusForPath(suffixDir, bucketDir);
    setupListStatusForPath(bucketDir, appDirs.stream()
            .filter(app -> app.path.toString().contains(controllerName))
            .map(app -> app.fileStatus)
            .toArray(FileStatus[]::new));

    for (Pair<String, Long> appDirPair : additionalAppDirs) {
      PathWithFileStatus appDir = createDirLogPathWithFileStatus(bucketDir.path,
              appDirPair.getLeft(), appDirPair.getRight());
      setupListStatusForPath(appDir, new FileStatus[] {});
    }
  }

  private void createApplicationsByDescriptors() throws IOException {
    int len = appDescriptors.size();
    appDirs = new ArrayList<>(len);

    for (int i = 0; i < len; i++) {
      AppDescriptor appDesc = appDescriptors.get(i);
      ApplicationId applicationId = appDesc.createApplicationId(now, i + 1);
      applicationIds.add(applicationId);
      Path basePath = this.remoteRootLogPath;
      if (appDesc.fileController != null) {
        basePath = new Path(basePath, appDesc.fileController);
      }

      PathWithFileStatus appDir = createPathWithFileStatusForAppId(
              basePath, applicationId, userDirName, suffix, appDesc.modTimeOfAppDir);
      LOG.debug("Created application with ID '{}' to path '{}'", applicationId, appDir.path);
      appDirs.add(appDir);
      addAppChildrenFiles(appDesc, appDir);
    }

    setupFsMocksForAppsAndChildrenFiles();

    for (Map.Entry<Integer, Exception> e : injectedAppDirDeletionExceptions.entrySet()) {
      when(mockFs.delete(this.appDirs.get(e.getKey()).path, true)).thenThrow(e.getValue());
    }
  }

  private void setupFsMocksForAppsAndChildrenFiles() throws IOException {
    for (int i = 0; i < appDirs.size(); i++) {
      List<PathWithFileStatus> appChildren = appFiles.get(i);
      Path appPath = appDirs.get(i).path;
      setupListStatusForPath(appPath,
              appChildren.stream()
                      .map(child -> child.fileStatus)
                      .toArray(FileStatus[]::new));
    }
  }

  private void setupListStatusForPath(Path dir, PathWithFileStatus pathWithFileStatus)
          throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(PathWithFileStatus dir, PathWithFileStatus pathWithFileStatus)
          throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(Path dir, FileStatus[] fileStatuses) throws IOException {
    LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir, fileStatuses);
    when(mockFs.listStatus(dir)).thenReturn(fileStatuses);
  }

  private void setupListStatusForPath(PathWithFileStatus dir, FileStatus[] fileStatuses)
          throws IOException {
    LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir.path, fileStatuses);
    when(mockFs.listStatus(dir.path)).thenReturn(fileStatuses);
  }

  private void setupDeletionService() {
    List<ApplicationId> finishedApps = createFinishedAppsList();
    List<ApplicationId> runningApps = createRunningAppsList();
    deletionService = new AggregatedLogDeletionServiceForTest(runningApps, finishedApps, conf);
  }

  public LogAggregationTestcase startDeletionService() {
    deletionService.init(conf);
    deletionService.start();
    return this;
  }

  private List<ApplicationId> createRunningAppsList() {
    List<ApplicationId> runningApps = new ArrayList<>();
    for (int i : runningAppIds) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      runningApps.add(appId);
    }
    return runningApps;
  }

  private List<ApplicationId> createFinishedAppsList() {
    List<ApplicationId> finishedApps = new ArrayList<>();
    for (int i : finishedAppIds) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      finishedApps.add(appId);
    }
    return finishedApps;
  }

  public LogAggregationTestcase runDeletionTask(long retentionSeconds) throws Exception {
    List<ApplicationId> finishedApps = createFinishedAppsList();
    List<ApplicationId> runningApps = createRunningAppsList();
    rmClient = createMockRMClient(finishedApps, runningApps);
    List<LogDeletionTask> tasks = deletionService.createLogDeletionTasks(conf, retentionSeconds,
            rmClient);
    for (LogDeletionTask deletionTask : tasks) {
      deletionTask.run();
    }

    return this;
  }

  private void addAppChildrenFiles(AppDescriptor appDesc, PathWithFileStatus appDir) {
    List<PathWithFileStatus> appChildren = new ArrayList<>();
    for (Pair<String, Long> fileWithModDate : appDesc.filesWithModDate) {
      PathWithFileStatus appChildFile = createFileLogPathWithFileStatus(appDir.path,
              fileWithModDate.getLeft(),
              fileWithModDate.getRight());
      appChildren.add(appChildFile);
    }
    this.appFiles.add(appChildren);
  }

  public LogAggregationTestcase verifyAppDirsDeleted(long timeout, int... ids) throws IOException {
    for (int id : ids) {
      verifyAppDirDeleted(id, timeout);
    }
    return this;
  }

  public LogAggregationTestcase verifyAppDirsNotDeleted(long timeout, int... ids)
          throws IOException {
    for (int id : ids) {
      verifyAppDirNotDeleted(id, timeout);
    }
    return this;
  }

  public LogAggregationTestcase verifyAppDirDeleted(int id, long timeout) throws IOException {
    verifyAppDirDeletion(id, 1, timeout);
    return this;
  }

  public LogAggregationTestcase verifyAppDirNotDeleted(int id, long timeout) throws IOException {
    verifyAppDirDeletion(id, 0, timeout);
    return this;
  }

  public LogAggregationTestcase verifyAppFilesDeleted(long timeout,
                                                      List<Pair<Integer, Integer>> pairs)
          throws IOException {
    for (Pair<Integer, Integer> pair : pairs) {
      verifyAppFileDeleted(pair.getLeft(), pair.getRight(), timeout);
    }
    return this;
  }

  public LogAggregationTestcase verifyAppFilesNotDeleted(long timeout,
                                                         List<Pair<Integer, Integer>> pairs)
          throws IOException {
    for (Pair<Integer, Integer> pair : pairs) {
      verifyAppFileNotDeleted(pair.getLeft(), pair.getRight(), timeout);
    }
    return this;
  }

  public LogAggregationTestcase verifyAppFileDeleted(int id, int fileNo, long timeout)
          throws IOException {
    verifyAppFileDeletion(id, fileNo, 1, timeout);
    return this;
  }

  public LogAggregationTestcase verifyAppFileNotDeleted(int id, int fileNo, long timeout)
          throws IOException {
    verifyAppFileDeletion(id, fileNo, 0, timeout);
    return this;
  }

  private void verifyAppDirDeletion(int id, int times, long timeout) throws IOException {
    if (timeout == NO_TIMEOUT) {
      verify(mockFs, times(times)).delete(this.appDirs.get(id - 1).path, true);
    } else {
      verify(mockFs, timeout(timeout).times(times)).delete(this.appDirs.get(id - 1).path, true);
    }
  }

  private void verifyAppFileDeletion(int appId, int fileNo, int times, long timeout)
          throws IOException {
    List<PathWithFileStatus> childrenFiles = this.appFiles.get(appId - 1);
    PathWithFileStatus file = childrenFiles.get(fileNo - 1);
    verify(mockFs, timeout(timeout).times(times)).delete(file.path, true);
  }

  private void verifyMockRmClientWasClosedNTimes(int expectedRmClientCloses)
      throws IOException {
    ApplicationClientProtocol mockRMClient;
    if (deletionService != null) {
      mockRMClient = deletionService.getMockRMClient();
    } else {
      mockRMClient = rmClient;
    }
    verify((Closeable)mockRMClient, times(expectedRmClientCloses)).close();
  }

  public void teardown(int expectedRmClientCloses) throws IOException {
    deletionService.stop();
    verifyMockRmClientWasClosedNTimes(expectedRmClientCloses);
  }

  public LogAggregationTestcase refreshLogRetentionSettings() throws IOException {
    deletionService.refreshLogRetentionSettings();
    return this;
  }

  public AggregatedLogDeletionService getDeletionService() {
    return deletionService;
  }

  public LogAggregationTestcase verifyCheckIntervalMilliSecondsEqualTo(
          int checkIntervalMilliSeconds) {
    assertEquals(checkIntervalMilliSeconds, deletionService.getCheckIntervalMsecs());
    return this;
  }

  public LogAggregationTestcase verifyCheckIntervalMilliSecondsNotEqualTo(
          int checkIntervalMilliSeconds) {
    assertTrue(checkIntervalMilliSeconds != deletionService.getCheckIntervalMsecs());
    return this;
  }

  public LogAggregationTestcase verifyAnyPathListedAtLeast(int atLeast, long timeout)
          throws IOException {
    verify(mockFs, timeout(timeout).atLeast(atLeast)).listStatus(any(Path.class));
    return this;
  }

  public LogAggregationTestcase changeModTimeOfApp(int appId, long modTime) {
    PathWithFileStatus appDir = appDirs.get(appId - 1);
    appDir.changeModificationTime(modTime);
    return this;
  }

  public LogAggregationTestcase changeModTimeOfAppLogDir(int appId, int fileNo, long modTime) {
    List<PathWithFileStatus> childrenFiles = this.appFiles.get(appId - 1);
    PathWithFileStatus file = childrenFiles.get(fileNo - 1);
    file.changeModificationTime(modTime);
    return this;
  }

  public LogAggregationTestcase changeModTimeOfBucketDir(long modTime) {
    bucketDir.changeModificationTime(modTime);
    return this;
  }

  public LogAggregationTestcase reinitAllPaths() throws IOException {
    List<Path> rootPaths = determineRootPaths();
    for (Path rootPath : rootPaths) {
      String controllerName = rootPath.getName();
      initFileSystemListings(controllerName);
    }
    setupFsMocksForAppsAndChildrenFiles();
    return this;
  }

}
