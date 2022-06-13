package org.apache.hadoop.yarn.logaggregation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.AggregatedLogDeletionServiceForTest;
import org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.PathWithFileStatus;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.*;
import static org.mockito.Mockito.*;

class LogAggregationFilesBuilder {
  private final long now;
  private final Configuration conf;
  private FileSystem mockFs;
  private Path remoteRootLogPath;
  private PathWithFileStatus userDir;
  private String suffix;
  private PathWithFileStatus suffixDir;
  private PathWithFileStatus bucketDir;
  private String userDirName;
  private List<ApplicationId> applicationIds;
  private List<PathWithFileStatus> appDirs;
  private List<List<PathWithFileStatus>> appFiles = new ArrayList<>();
  private List<Exception> injectedAppDirDeletionExceptions;
  private List<ApplicationId> finishedApps;
  private List<ApplicationId> runningApps;
  private AggregatedLogDeletionServiceForTest deletionService;
  private List<String> fileControllers;

  public LogAggregationFilesBuilder(Configuration conf) {
    this.conf = conf;
    this.now = System.currentTimeMillis();
  }

  public static LogAggregationFilesBuilder create(Configuration conf) {
    return new LogAggregationFilesBuilder(conf);
  }

  public LogAggregationFilesBuilder withRootPath(String root) throws IOException {
    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    mockFs = ((FilterFileSystem) rootFs).getRawFileSystem();
    return this;
  }

  public LogAggregationFilesBuilder withRemoteRootLogPath(String remoteRootLogDir) {
    remoteRootLogPath = new Path(remoteRootLogDir);
    return this;
  }

  public LogAggregationFilesBuilder withUserDir(String userDirName, long modTime) throws IOException {
    this.userDirName = userDirName;
    userDir = createDirLogPathWithFileStatus(remoteRootLogPath, userDirName, modTime);
    return this;
  }

  public LogAggregationFilesBuilder withSuffixDir(String suffix, long modTime) {
    if (userDir == null) {
      throw new IllegalStateException("Please invoke 'withUserDir' with a valid Path first!");
    }
    this.suffix = suffix;
    suffixDir = createDirLogPathWithFileStatus(userDir.path, this.suffix, modTime);
    return this;
  }

  public LogAggregationFilesBuilder withBucketDir(String suffix, long modTime) {
    //TODO
//      if (suffix == null) {
//        throw new IllegalStateException("Please invoke 'withSuffixDir' with a valid Path first!");
//      }
    bucketDir = createDirLogPathWithFileStatus(remoteRootLogPath, suffix, modTime);
    return this;
  }

  public LogAggregationFilesBuilder withApps(AppDescriptor... apps) {
    Set<String> controllers = Arrays.stream(apps)
            .map(a -> a.fileController)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    Set<String> availableControllers = fileControllers != null ? new HashSet<>(this.fileControllers) : Sets.newHashSet();
    Set<String> difference = Sets.difference(controllers, availableControllers);
    if (!difference.isEmpty()) {
      throw new IllegalStateException(String.format("Invalid controller defined! Available: %s, Actual: %s",
              availableControllers, controllers));
    }

    int len = apps.length;
    appDirs = new ArrayList<>(len);
    applicationIds = new ArrayList<>(len);

    for (int i = 0; i < len; i++) {
      AppDescriptor appDesc = apps[i];
      ApplicationId applicationId = appDesc.createApplicationId(now, i + 1);
      applicationIds.add(applicationId);
      Path basePath = this.remoteRootLogPath;
      if (appDesc.fileController != null) {
        basePath = new Path(basePath, appDesc.fileController);
      }

      PathWithFileStatus appDir = createPathWithFileStatusForAppId(
              basePath, applicationId, userDirName, suffix, appDesc.modTimeOfAppDir);
      TestAggregatedLogDeletionService.LOG.debug("Created application with id '{}' to path '{}'", applicationId, appDir.path);
      appDirs.add(appDir);
      addAppChildrenFiles(appDesc, appDir);
    }
    this.injectedAppDirDeletionExceptions = new ArrayList<>(len);
    for (int i = 0; i < len; i++) {
      injectedAppDirDeletionExceptions.add(null);
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

  public LogAggregationFilesBuilder injectExceptionForAppDirDeletion(int... indices) {
    for (int i : indices) {
      AccessControlException e = new AccessControlException("Injected Error\nStack Trace :(");
      this.injectedAppDirDeletionExceptions.add(i, e);
    }
    return this;
  }

  LogAggregationFilesBuilder setupMocks() throws IOException {
    if (fileControllers != null && !fileControllers.isEmpty()) {
      for (String fileController : fileControllers) {
        Path controllerPath = new Path(remoteRootLogPath, fileController);
        setupListStatusForPath(controllerPath, userDir);
      }
    } else {
      setupListStatusForPath(remoteRootLogPath, userDir);
    }
    setupListStatusForPath(userDir, suffixDir);
    setupListStatusForPath(suffixDir, bucketDir);
    setupListStatusForPath(bucketDir, appDirs.stream().map(
                    app -> app.fileStatus)
            .toArray(FileStatus[]::new));

    for (int i = 0; i < appDirs.size(); i++) {
      List<PathWithFileStatus> appChildren = appFiles.get(i);
      Path appPath = appDirs.get(i).path;
      setupListStatusForPath(appPath,
              appChildren.stream()
                      .map(child -> child.fileStatus)
                      .toArray(FileStatus[]::new));
    }

    for (int i = 0; i < injectedAppDirDeletionExceptions.size(); i++) {
      Exception e = injectedAppDirDeletionExceptions.get(i);
      if (e != null) {
        when(mockFs.delete(this.appDirs.get(i).path, true)).thenThrow(e);
      }
    }

    return this;
  }

  private void setupListStatusForPath(Path dir, PathWithFileStatus pathWithFileStatus) throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(PathWithFileStatus dir, PathWithFileStatus pathWithFileStatus) throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(Path dir, FileStatus[] fileStatuses) throws IOException {
    TestAggregatedLogDeletionService.LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir, fileStatuses);
    when(mockFs.listStatus(dir)).thenReturn(fileStatuses);
  }

  private void setupListStatusForPath(PathWithFileStatus dir, FileStatus[] fileStatuses) throws IOException {
    TestAggregatedLogDeletionService.LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir.path, fileStatuses);
    when(mockFs.listStatus(dir.path)).thenReturn(fileStatuses);
  }

  public LogAggregationFilesBuilder withFinishedApps(int... apps) {
    this.finishedApps = new ArrayList<>();
    for (int i : apps) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      this.finishedApps.add(appId);
    }
    return this;
  }

  public LogAggregationFilesBuilder withRunningApps(int... apps) {
    this.runningApps = new ArrayList<>();
    for (int i : apps) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      this.runningApps.add(appId);
    }
    return this;
  }

  public LogAggregationFilesBuilder withBothFileControllers() {
    this.fileControllers = ALL_FILE_CONTROLLER_NAMES;
    return this;
  }

  public LogAggregationFilesBuilder setupAndRunDeletionService() {
    deletionService = new AggregatedLogDeletionServiceForTest(runningApps, finishedApps);
    deletionService.init(conf);
    deletionService.start();
    return this;
  }

  public LogAggregationFilesBuilder verifyAppDirsDeleted(long timeout, int... ids) throws IOException {
    for (int id : ids) {
      verifyAppDirDeleted(id, timeout);
    }
    return this;
  }

  public LogAggregationFilesBuilder verifyAppDirsNotDeleted(long timeout, int... ids) throws IOException {
    for (int id : ids) {
      verifyAppDirNotDeleted(id, timeout);
    }
    return this;
  }

  public LogAggregationFilesBuilder verifyAppDirDeleted(int id, long timeout) throws IOException {
    verifyAppDirDeletion(id, 1, timeout);
    return this;
  }

  public LogAggregationFilesBuilder verifyAppDirNotDeleted(int id, long timeout) throws IOException {
    verifyAppDirDeletion(id, 0, timeout);
    return this;
  }

  public LogAggregationFilesBuilder verifyAppFilesDeleted(long timeout, Pair<Integer, Integer>... pairs) throws IOException {
    for (Pair<Integer, Integer> pair : pairs) {
      verifyAppFileDeleted(pair.getLeft(), pair.getRight(), timeout);
    }
    return this;
  }

  public LogAggregationFilesBuilder verifyAppFilesNotDeleted(long timeout, Pair<Integer, Integer>... pairs) throws IOException {
    for (Pair<Integer, Integer> pair : pairs) {
      verifyAppFileNotDeleted(pair.getLeft(), pair.getRight(), timeout);
    }
    return this;
  }

  public LogAggregationFilesBuilder verifyAppFileDeleted(int id, int fileNo, long timeout) throws IOException {
    verifyAppFileDeletion(id, fileNo, 1, timeout);
    return this;
  }

  public LogAggregationFilesBuilder verifyAppFileNotDeleted(int id, int fileNo, long timeout) throws IOException {
    verifyAppFileDeletion(id, fileNo, 0, timeout);
    return this;
  }

  private void verifyAppDirDeletion(int id, int times, long timeout) throws IOException {
    verify(mockFs, timeout(timeout).times(times)).delete(this.appDirs.get(id - 1).path, true);
  }

  private void verifyAppFileDeletion(int appId, int fileNo, int times, long timeout) throws IOException {
    List<PathWithFileStatus> childrenFiles = this.appFiles.get(appId - 1);
    PathWithFileStatus file = childrenFiles.get(fileNo - 1);
    verify(mockFs, timeout(timeout).times(times)).delete(file.path, true);
  }

  public void teardown() {
    deletionService.stop();
  }

  public static class AppDescriptor {
    private final long modTimeOfAppDir;
    private final ArrayList<Pair<String, Long>> filesWithModDate;
    private String fileController;

    public AppDescriptor(String fileController, long modTimeOfAppDir, Pair<String, Long>... filesWithModDate) {
      this(modTimeOfAppDir, filesWithModDate);
      this.fileController = fileController;
    }

    public AppDescriptor(long modTimeOfAppDir, Pair<String, Long>... filesWithModDate) {
      this.modTimeOfAppDir = modTimeOfAppDir;
      this.filesWithModDate = Lists.newArrayList(filesWithModDate);
    }

    public ApplicationId createApplicationId(long now, int id) {
      return ApplicationId.newInstance(now, id);
    }
  }
}
