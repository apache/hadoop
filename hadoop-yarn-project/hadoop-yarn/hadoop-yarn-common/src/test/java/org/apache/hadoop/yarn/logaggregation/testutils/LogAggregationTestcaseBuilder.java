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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

import static org.apache.hadoop.yarn.logaggregation.TestAggregatedLogDeletionService.ALL_FILE_CONTROLLER_NAMES;

public class LogAggregationTestcaseBuilder {
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
  List<AppDescriptor> apps = Lists.newArrayList();
  int[] finishedAppIds;
  int[] runningAppIds;
  PathWithFileStatus userDir;
  PathWithFileStatus suffixDir;
  PathWithFileStatus bucketDir;
  String bucketId;
  List<Pair<String, Long>> additionalAppDirs = new ArrayList<>();
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

  public LogAggregationTestcaseBuilder withUserDir(String userDirName, long modTime) {
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
   * Bucket dir paths will be generated later.
   * @param modTime The modification time
   * @return The builder
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

  public final LogAggregationTestcaseBuilder withApps(List<AppDescriptor> apps) {
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

  public LogAggregationTestcaseBuilder withAdditionalAppDirs(List<Pair<String, Long>> appDirs) {
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

  public static final class AppDescriptor {
    final long modTimeOfAppDir;
    List<Pair<String, Long>> filesWithModDate = new ArrayList<>();
    String fileController;

    public AppDescriptor(long modTimeOfAppDir) {
      this.modTimeOfAppDir = modTimeOfAppDir;
    }

    public AppDescriptor(long modTimeOfAppDir, List<Pair<String, Long>> filesWithModDate) {
      this.modTimeOfAppDir = modTimeOfAppDir;
      this.filesWithModDate = filesWithModDate;
    }

    public AppDescriptor(String fileController, long modTimeOfAppDir,
                         List<Pair<String, Long>> filesWithModDate) {
      this(modTimeOfAppDir, filesWithModDate);
      this.fileController = fileController;
    }


    public ApplicationId createApplicationId(long now, int id) {
      return ApplicationId.newInstance(now, id);
    }
  }
}
