package org.apache.hadoop.yarn.logaggregation.testutils;

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
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.*;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class LogAggregationFilesBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(LogAggregationFilesBuilder.class);
  
  private final long now;
  private final Configuration conf;
  private FileSystem mockFs;
  private Path remoteRootLogPath;
  private String suffix;
  private String userDirName;
  private long userDirModTime;
  private List<ApplicationId> applicationIds;
  private List<PathWithFileStatus> appDirs;
  private final List<List<PathWithFileStatus>> appFiles = new ArrayList<>();
  private final Map<Integer, Exception> injectedAppDirDeletionExceptions = new HashMap<>();
  private AggregatedLogDeletionServiceForTest deletionService;
  private List<String> fileControllers;
  private long suffixDirModTime;
  private long bucketDirModTime;
  private String suffixDirName;
  private AppDescriptor[] apps;
  private int[] finishedAppIds;
  private int[] runningAppIds;

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
    this.userDirModTime = modTime;
    return this;
  }

  public LogAggregationFilesBuilder withSuffixDir(String suffix, long modTime) {
    this.suffix = suffix;
    this.suffixDirName = LogAggregationUtils.getBucketSuffix() + suffix;
    this.suffixDirModTime = modTime;
    return this;
  }

  /**
   * Bucket dir paths will be generated later
   * @param modTime
   * @return
   */
  public LogAggregationFilesBuilder withBucketDir(long modTime) {
    this.bucketDirModTime = modTime;
    return this;
  }

  public LogAggregationFilesBuilder withApps(AppDescriptor... apps) {
    this.apps = apps;
    validateAppControllers(apps);
    return this;
  }

  private void validateAppControllers(AppDescriptor[] apps) {
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
      this.injectedAppDirDeletionExceptions.put(i, e);
    }
    return this;
  }

  public LogAggregationFilesBuilder setupMocks() throws IOException {
    createApplicationsByDescriptors();

    List<Path> rootPaths = new ArrayList<>();
    if (fileControllers != null && !fileControllers.isEmpty()) {
      for (String fileController : fileControllers) {
        //Example path: <remote-app-log-dir>/<user>/bucket-<suffix>/<bucket id>/<application id>/<NodeManager id>
        //remoteRootLogPath: <remote-app-log-dir>/ ::: mockfs://foo/tmp/logs/
        //userDir: <remote-app-log-dir>/<user>/ ::: mockfs://foo/tmp/logs/me/
        //suffixDir: <remote-app-log-dir>/<user>/bucket-<suffix>/ ::: mockfs://foo/tmp/logs/me/bucket-logs/
        //bucketDir: <remote-app-log-dir>/<user>/bucket-<suffix>/<bucket id>/ ::: mockfs://foo/tmp/logs/me/bucket-logs/0001/
        //remoteRootLogPath with controller: <remote-app-log-dir>/<controllerName> ::: mockfs://foo/tmp/logs/IFile
        rootPaths.add(new Path(remoteRootLogPath, fileController));
      }
    } else {
      rootPaths.add(remoteRootLogPath);
    }
    
    for (Path rootPath : rootPaths) {
      String controllerName = rootPath.getName();
      ApplicationId arbitraryAppIdForBucketDir = this.applicationIds.get(0);
      PathWithFileStatus userDir = createDirLogPathWithFileStatus(rootPath, userDirName, userDirModTime);
      PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, suffixDirName, suffixDirModTime);
      PathWithFileStatus bucketDir = createDirBucketDirLogPathWithFileStatus(rootPath, userDirName, suffix, arbitraryAppIdForBucketDir, bucketDirModTime);
      setupListStatusForPath(rootPath, userDir);
      setupListStatusForPath(userDir, suffixDir);
      setupListStatusForPath(suffixDir, bucketDir);
      setupListStatusForPath(bucketDir, appDirs.stream()
              .filter(app -> app.path.toString().contains(controllerName))
              .map(app -> app.fileStatus)
              .toArray(FileStatus[]::new));
    }
    return this;
  }
  
  private void createApplicationsByDescriptors() throws IOException {
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
      LOG.debug("Created application with id '{}' to path '{}'", applicationId, appDir.path);
      appDirs.add(appDir);
      addAppChildrenFiles(appDesc, appDir);
    }

    for (int i = 0; i < appDirs.size(); i++) {
      List<PathWithFileStatus> appChildren = appFiles.get(i);
      Path appPath = appDirs.get(i).path;
      setupListStatusForPath(appPath,
              appChildren.stream()
                      .map(child -> child.fileStatus)
                      .toArray(FileStatus[]::new));
    }

    for (Map.Entry<Integer, Exception> e : injectedAppDirDeletionExceptions.entrySet()) {
      when(mockFs.delete(this.appDirs.get(e.getKey()).path, true)).thenThrow(e.getValue());
    }
  }

  private void setupListStatusForPath(Path dir, PathWithFileStatus pathWithFileStatus) throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(PathWithFileStatus dir, PathWithFileStatus pathWithFileStatus) throws IOException {
    setupListStatusForPath(dir, new FileStatus[]{pathWithFileStatus.fileStatus});
  }

  private void setupListStatusForPath(Path dir, FileStatus[] fileStatuses) throws IOException {
    LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir, fileStatuses);
    when(mockFs.listStatus(dir)).thenReturn(fileStatuses);
  }

  private void setupListStatusForPath(PathWithFileStatus dir, FileStatus[] fileStatuses) throws IOException {
    LOG.debug("Setting up listStatus. Parent: {}, files: {}", dir.path, fileStatuses);
    when(mockFs.listStatus(dir.path)).thenReturn(fileStatuses);
  }

  public LogAggregationFilesBuilder withFinishedApps(int... apps) {
    this.finishedAppIds = apps;
    return this;
  }

  public LogAggregationFilesBuilder withRunningApps(int... apps) {
    this.runningAppIds = apps;
    return this;
  }

  public LogAggregationFilesBuilder withBothFileControllers() {
    this.fileControllers = ALL_FILE_CONTROLLER_NAMES;
    return this;
  }

  public LogAggregationFilesBuilder setupAndRunDeletionService() {
    List<ApplicationId> finishedApps = new ArrayList<>();
    for (int i : finishedAppIds) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      finishedApps.add(appId);
    }

    List<ApplicationId> runningApps = new ArrayList<>();
    for (int i : runningAppIds) {
      ApplicationId appId = this.applicationIds.get(i - 1);
      runningApps.add(appId);
    }
    deletionService = new AggregatedLogDeletionServiceForTest(runningApps, finishedApps, conf);
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

  public LogAggregationFilesBuilder refreshLogRetentionSettings() throws IOException {
    deletionService.refreshLogRetentionSettings();
    return this;
  }

  public AggregatedLogDeletionServiceForTest getDeletionService() {
    return deletionService;
  }

  public LogAggregationFilesBuilder verifyCheckIntervalMilliSecondsEqualTo(int checkIntervalMilliSeconds) {
    assertEquals(checkIntervalMilliSeconds, deletionService.getCheckIntervalMsecs());
    return this;
  }
  
  public LogAggregationFilesBuilder verifyCheckIntervalMilliSecondsNotEqualTo(int checkIntervalMilliSeconds) {
    assertTrue(checkIntervalMilliSeconds != deletionService.getCheckIntervalMsecs());
    return this;
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
