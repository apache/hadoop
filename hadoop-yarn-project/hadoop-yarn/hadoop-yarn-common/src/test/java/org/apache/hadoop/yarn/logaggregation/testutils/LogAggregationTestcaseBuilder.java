package org.apache.hadoop.yarn.logaggregation.testutils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
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

import static org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.ALL_FILE_CONTROLLER_NAMES;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogAggregationTestcaseBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(LogAggregationTestcaseBuilder.class);

  public static final long NO_TIMEOUT = -1;
  final long now;
  final Configuration conf;
  Path remoteRootLogPath;
  String suffix;
  String userDirName;
  long userDirModTime;
  final Map<Integer, Exception> injectedAppDirDeletionExceptions = new HashMap<>();
  List<String> fileControllers;
  long suffixDirModTime;
  long bucketDirModTime;
  String suffixDirName;
  AppDescriptor[] apps;
  int[] finishedAppIds;
  int[] runningAppIds;
  PathWithFileStatus userDir;
  PathWithFileStatus suffixDir;
  PathWithFileStatus bucketDir;
  String bucketId;
  Pair<String, Long>[] additionalAppDirs = new Pair[] {};
  FileSystem rootFs;

  public LogAggregationTestcaseBuilder(Configuration conf) {
    this.conf = conf;
    this.now = System.currentTimeMillis();
  }

  public static LogAggregationTestcaseBuilder create(Configuration conf) {
    return new LogAggregationTestcaseBuilder(conf);
  }

  public LogAggregationTestcaseBuilder withRootPath(String root) throws IOException {
    Path rootPath = new Path(root);
    rootFs = rootPath.getFileSystem(conf);
    return this;
  }

  public LogAggregationTestcaseBuilder withRemoteRootLogPath(String remoteRootLogDir) {
    remoteRootLogPath = new Path(remoteRootLogDir);
    return this;
  }

  public LogAggregationTestcaseBuilder withUserDir(String userDirName, long modTime) throws IOException {
    this.userDirName = userDirName;
    this.userDirModTime = modTime;
    return this;
  }

  public LogAggregationTestcaseBuilder withSuffixDir(String suffix, long modTime) {
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
  public LogAggregationTestcaseBuilder withBucketDir(long modTime) {
    this.bucketDirModTime = modTime;
    return this;
  }

  public LogAggregationTestcaseBuilder withBucketDir(long modTime, String bucketId) {
    this.bucketDirModTime = modTime;
    this.bucketId = bucketId;
    return this;
  }

  public LogAggregationTestcaseBuilder withApps(AppDescriptor... apps) {
    this.apps = apps;
    return this;
  }

  public LogAggregationTestcaseBuilder withFinishedApps(int... apps) {
    this.finishedAppIds = apps;
    return this;
  }

  public LogAggregationTestcaseBuilder withRunningApps(int... apps) {
    this.runningAppIds = apps;
    return this;
  }

  public LogAggregationTestcaseBuilder withBothFileControllers() {
    this.fileControllers = ALL_FILE_CONTROLLER_NAMES;
    return this;
  }

  public LogAggregationTestcaseBuilder withAdditionalAppDirs(Pair<String, Long>... appDirs) {
    this.additionalAppDirs = appDirs;
    return this;
  }

  public LogAggregationTestcaseBuilder injectExceptionForAppDirDeletion(int... indices) {
    for (int i : indices) {
      AccessControlException e = new AccessControlException("Injected Error\nStack Trace :(");
      this.injectedAppDirDeletionExceptions.put(i, e);
    }
    return this;
  }

  public LogAggregationTestcase build() throws IOException {
    return new LogAggregationTestcase(this);
  }

  public static class AppDescriptor {
    final long modTimeOfAppDir;
    final ArrayList<Pair<String, Long>> filesWithModDate;
    String fileController;

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
