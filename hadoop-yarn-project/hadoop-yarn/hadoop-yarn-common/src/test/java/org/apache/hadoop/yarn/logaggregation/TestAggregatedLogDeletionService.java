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
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcase;
import org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcaseBuilder;
import org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcaseBuilder.AppDescriptor;
import org.apache.log4j.Level;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT;
import static org.apache.hadoop.yarn.logaggregation.LogAggregationTestUtils.enableFileControllers;
import static org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationTestcaseBuilder.NO_TIMEOUT;
import static org.mockito.Mockito.mock;

public class TestAggregatedLogDeletionService {
  private static final String T_FILE = "TFile";
  private static final String I_FILE = "IFile";
  private static final String USER_ME = "me";
  private static final String DIR_HOST1 = "host1";
  private static final String DIR_HOST2 = "host2";

  private static final String ROOT = "mockfs://foo/";
  private static final String REMOTE_ROOT_LOG_DIR = ROOT + "tmp/logs/";
  private static final String SUFFIX = "logs";
  private static final int TEN_DAYS_IN_SECONDS = 10 * 24 * 3600;

  private static final List<Class<? extends LogAggregationFileController>>
          ALL_FILE_CONTROLLERS = Arrays.asList(
          LogAggregationIndexedFileController.class,
          LogAggregationTFileController.class);
  public static final List<String> ALL_FILE_CONTROLLER_NAMES = Arrays.asList(I_FILE, T_FILE);

  @BeforeAll
  public static void beforeClass() {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @BeforeEach
  public void closeFilesystems() throws IOException {
    // prevent the same mockfs instance from being reused due to FS cache
    FileSystem.closeAll();
  }

  private Configuration setupConfiguration(int retainSeconds, int retainCheckIntervalSeconds) {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, retainSeconds);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
            retainCheckIntervalSeconds);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_ROOT_LOG_DIR);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, SUFFIX);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, T_FILE);
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, T_FILE),
            LogAggregationTFileController.class.getName());
    return conf;
  }

  @Test
  void testDeletion() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000 * 1000);
    long toKeepTime = now - (1500 * 1000);

    Configuration conf = setupConfiguration(1800, -1);
    long timeout = 2000L;
    LogAggregationTestcaseBuilder.create(conf)
        .withRootPath(ROOT)
        .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
        .withUserDir(USER_ME, toKeepTime)
        .withSuffixDir(SUFFIX, toDeleteTime)
        .withBucketDir(toDeleteTime)
        .withApps(Lists.newArrayList(
            new AppDescriptor(toDeleteTime, Lists.newArrayList()),
            new AppDescriptor(toDeleteTime, Lists.newArrayList(
                Pair.of(DIR_HOST1, toDeleteTime),
                Pair.of(DIR_HOST2, toKeepTime))),
            new AppDescriptor(toDeleteTime, Lists.newArrayList(
                Pair.of(DIR_HOST1, toDeleteTime),
                Pair.of(DIR_HOST2, toDeleteTime))),
            new AppDescriptor(toDeleteTime, Lists.newArrayList(
                Pair.of(DIR_HOST1, toDeleteTime),
                Pair.of(DIR_HOST2, toKeepTime)))))
        .withFinishedApps(1, 2, 3)
        .withRunningApps(4)
        .injectExceptionForAppDirDeletion(3)
        .build()
        .startDeletionService()
        .verifyAppDirsDeleted(timeout, 1, 3)
        .verifyAppDirsNotDeleted(timeout, 2, 4)
        .verifyAppFileDeleted(4, 1, timeout)
        .verifyAppFileNotDeleted(4, 2, timeout)
        .teardown(1);
  }

  @Test
  void testRefreshLogRetentionSettings() throws Exception {
    long now = System.currentTimeMillis();
    long before2000Secs = now - (2000 * 1000);
    long before50Secs = now - (50 * 1000);
    int checkIntervalSeconds = 2;
    int checkIntervalMilliSeconds = checkIntervalSeconds * 1000;

    Configuration conf = setupConfiguration(1800, 1);

    LogAggregationTestcase testcase = LogAggregationTestcaseBuilder.create(conf)
        .withRootPath(ROOT)
        .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
        .withUserDir(USER_ME, before50Secs)
        .withSuffixDir(SUFFIX, before50Secs)
        .withBucketDir(before50Secs)
        .withApps(Lists.newArrayList(
            //Set time last modified of app1Dir directory and its files to before2000Secs
            new AppDescriptor(before2000Secs, Lists.newArrayList(
                Pair.of(DIR_HOST1, before2000Secs))),
            //Set time last modified of app1Dir directory and its files to before50Secs
            new AppDescriptor(before50Secs, Lists.newArrayList(
                Pair.of(DIR_HOST1, before50Secs))))
        )
        .withFinishedApps(1, 2)
        .withRunningApps()
        .build();

    testcase
        .startDeletionService()
        //app1Dir would be deleted since it is done above log retention period
        .verifyAppDirDeleted(1, 10000L)
        //app2Dir is not expected to be deleted since it is below the threshold
        .verifyAppDirNotDeleted(2, 3000L);

    //Now, let's change the log aggregation retention configs
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 50);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
        checkIntervalSeconds);

    testcase
        //We have not called refreshLogSettings, hence don't expect to see
        // the changed conf values
        .verifyCheckIntervalMilliSecondsNotEqualTo(checkIntervalMilliSeconds)
        //refresh the log settings
        .refreshLogRetentionSettings()
        //Check interval time should reflect the new value
        .verifyCheckIntervalMilliSecondsEqualTo(checkIntervalMilliSeconds)
        //app2Dir should be deleted since it falls above the threshold
        .verifyAppDirDeleted(2, 10000L)
        //Close expected 2 times: once for refresh and once for stopping
        .teardown(2);
  }

  @Test
  void testCheckInterval() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - TEN_DAYS_IN_SECONDS * 1000;

    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();

    LogAggregationTestcaseBuilder.create(conf)
        .withRootPath(ROOT)
        .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
        .withUserDir(USER_ME, now)
        .withSuffixDir(SUFFIX, now)
        .withBucketDir(now)
        .withApps(Lists.newArrayList(
            new AppDescriptor(now,
                Lists.newArrayList(Pair.of(DIR_HOST1, now))),
            new AppDescriptor(now)))
        .withFinishedApps(1)
        .withRunningApps()
        .build()
        .startDeletionService()
        .verifyAnyPathListedAtLeast(4, 10000L)
        .verifyAppDirNotDeleted(1, NO_TIMEOUT)
        // modify the timestamp of the logs and verify if it is picked up quickly
        .changeModTimeOfApp(1, toDeleteTime)
        .changeModTimeOfAppLogDir(1, 1, toDeleteTime)
        .changeModTimeOfBucketDir(toDeleteTime)
        .reinitAllPaths()
        .verifyAppDirDeleted(1, 10000L)
        .teardown(1);
  }

  @Test
  void testRobustLogDeletion() throws Exception {
    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    long modTime = 0L;

    LogAggregationTestcaseBuilder.create(conf)
        .withRootPath(ROOT)
        .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
        .withUserDir(USER_ME, modTime)
        .withSuffixDir(SUFFIX, modTime)
        .withBucketDir(modTime, "0")
        .withApps(Lists.newArrayList(
            new AppDescriptor(modTime),
            new AppDescriptor(modTime),
            new AppDescriptor(modTime, Lists.newArrayList(Pair.of(DIR_HOST1, modTime)))))
        .withAdditionalAppDirs(Lists.newArrayList(Pair.of("application_a", modTime)))
        .withFinishedApps(1, 3)
        .withRunningApps()
        .injectExceptionForAppDirDeletion(1)
        .build()
        .runDeletionTask(TEN_DAYS_IN_SECONDS)
        .verifyAppDirDeleted(3, NO_TIMEOUT);
  }

  @Test
  void testDeletionTwoControllers() throws IOException {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000 * 1000);
    long toKeepTime = now - (1500 * 1000);


    Configuration conf = setupConfiguration(1800, -1);
    enableFileControllers(conf, REMOTE_ROOT_LOG_DIR, ALL_FILE_CONTROLLERS,
        ALL_FILE_CONTROLLER_NAMES);
    long timeout = 2000L;
    LogAggregationTestcaseBuilder.create(conf)
        .withRootPath(ROOT)
        .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
        .withBothFileControllers()
        .withUserDir(USER_ME, toKeepTime)
        .withSuffixDir(SUFFIX, toDeleteTime)
        .withBucketDir(toDeleteTime)
        .withApps(//Apps for TFile
            Lists.newArrayList(
                new AppDescriptor(T_FILE, toDeleteTime, Lists.newArrayList()),
                new AppDescriptor(T_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toKeepTime))),
                new AppDescriptor(T_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toDeleteTime))),
                new AppDescriptor(T_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toKeepTime))),
                //Apps for IFile
                new AppDescriptor(I_FILE, toDeleteTime, Lists.newArrayList()),
                new AppDescriptor(I_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toKeepTime))),
                new AppDescriptor(I_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toDeleteTime))),
                new AppDescriptor(I_FILE, toDeleteTime, Lists.newArrayList(
                    Pair.of(DIR_HOST1, toDeleteTime),
                    Pair.of(DIR_HOST2, toKeepTime)))))
        .withFinishedApps(1, 2, 3, 5, 6, 7)
        .withRunningApps(4, 8)
        .injectExceptionForAppDirDeletion(3, 6)
        .build()
        .startDeletionService()
        .verifyAppDirsDeleted(timeout, 1, 3, 5, 7)
        .verifyAppDirsNotDeleted(timeout, 2, 4, 6, 8)
        .verifyAppFilesDeleted(timeout, Lists.newArrayList(Pair.of(4, 1), Pair.of(8, 1)))
        .verifyAppFilesNotDeleted(timeout, Lists.newArrayList(Pair.of(4, 2), Pair.of(8, 2)))
        .teardown(1);
  }

  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }

    public void initialize(URI name, Configuration conf) throws IOException {}

    @Override
    public boolean hasPathCapability(Path path, String capability) {
      return true;
    }
  }
}
