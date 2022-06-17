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

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestAggregatedLogDeletionService {

  private static final String T_FILE = "TFile";
  private static final String USER_ME = "me";
  private static final String DIR_HOST1 = "host1";
  private static final String DIR_HOST2 = "host2";

  private static final String ROOT = "mockfs://foo/";
  private static final String REMOTE_ROOT_LOG_DIR = ROOT + "tmp/logs";
  private static final String SUFFIX = "logs";
  private static final String NEW_SUFFIX = LogAggregationUtils.getBucketSuffix() + SUFFIX;
  private static final int TEN_DAYS_IN_SECONDS = 10 * 24 * 3600;

  private static PathWithFileStatus createPathWithFileStatusForAppId(Path remoteRootLogDir,
                                                                     ApplicationId appId,
                                                                     String user, String suffix,
                                                                     long modificationTime) {
    Path path = LogAggregationUtils.getRemoteAppLogDir(
            remoteRootLogDir, appId, user, suffix);
    FileStatus fileStatus = createEmptyFileStatus(modificationTime, path);
    return new PathWithFileStatus(path, fileStatus);
  }

  private static FileStatus createEmptyFileStatus(long modificationTime, Path path) {
    return new FileStatus(0, true, 0, 0, modificationTime, path);
  }

  private static PathWithFileStatus createFileLogPathWithFileStatus(Path baseDir, String childDir,
                                                                    long modificationTime) {
    Path logPath = new Path(baseDir, childDir);
    FileStatus fStatus = createFileStatusWithLengthForFile(10, modificationTime, logPath);
    return new PathWithFileStatus(logPath, fStatus);
  }

  private static PathWithFileStatus createDirLogPathWithFileStatus(Path baseDir, String childDir,
                                                                   long modificationTime) {
    Path logPath = new Path(baseDir, childDir);
    FileStatus fStatus = createFileStatusWithLengthForDir(10, modificationTime, logPath);
    return new PathWithFileStatus(logPath, fStatus);
  }

  private static PathWithFileStatus createDirBucketDirLogPathWithFileStatus(Path remoteRootLogPath,
                                                                            String user,
                                                                            String suffix,
                                                                            ApplicationId appId,
                                                                            long modificationTime) {
    Path bucketDir = LogAggregationUtils.getRemoteBucketDir(remoteRootLogPath, user, suffix, appId);
    FileStatus fStatus = new FileStatus(0, true, 0, 0, modificationTime, bucketDir);
    return new PathWithFileStatus(bucketDir, fStatus);
  }

  private static FileStatus createFileStatusWithLengthForFile(long length,
                                                              long modificationTime,
                                                              Path logPath) {
    return new FileStatus(length, false, 1, 1, modificationTime, logPath);
  }

  private static FileStatus createFileStatusWithLengthForDir(long length,
                                                             long modificationTime,
                                                             Path logPath) {
    return new FileStatus(length, true, 1, 1, modificationTime, logPath);
  }

  @Before
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
  public void testDeletion() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000 * 1000);
    long toKeepTime = now - (1500 * 1000);

    Configuration conf = setupConfiguration(1800, -1);

    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();
    
    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);
    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME,
            toKeepTime);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[]{userDir.fileStatus});

    ApplicationId appId1 = ApplicationId.newInstance(now, 1);
    ApplicationId appId2 = ApplicationId.newInstance(now, 2);
    ApplicationId appId3 = ApplicationId.newInstance(now, 3);
    ApplicationId appId4 = ApplicationId.newInstance(now, 4);

    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX,
            toDeleteTime);
    PathWithFileStatus bucketDir = createDirLogPathWithFileStatus(remoteRootLogPath, SUFFIX,
            toDeleteTime);

    PathWithFileStatus app1 = createPathWithFileStatusForAppId(remoteRootLogPath, appId1,
            USER_ME, SUFFIX, toDeleteTime);
    PathWithFileStatus app2 = createPathWithFileStatusForAppId(remoteRootLogPath, appId2,
            USER_ME, SUFFIX, toDeleteTime);
    PathWithFileStatus app3 = createPathWithFileStatusForAppId(remoteRootLogPath, appId3,
            USER_ME, SUFFIX, toDeleteTime);
    PathWithFileStatus app4 = createPathWithFileStatusForAppId(remoteRootLogPath, appId4,
            USER_ME, SUFFIX, toDeleteTime);

    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[] {suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[] {bucketDir.fileStatus});
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[] {
        app1.fileStatus, app2.fileStatus, app3.fileStatus, app4.fileStatus});
    
    PathWithFileStatus app2Log1 = createFileLogPathWithFileStatus(app2.path, DIR_HOST1,
        toDeleteTime);
    PathWithFileStatus app2Log2 = createFileLogPathWithFileStatus(app2.path, DIR_HOST2, toKeepTime);
    PathWithFileStatus app3Log1 = createFileLogPathWithFileStatus(app3.path, DIR_HOST1,
        toDeleteTime);
    PathWithFileStatus app3Log2 = createFileLogPathWithFileStatus(app3.path, DIR_HOST2,
        toDeleteTime);
    PathWithFileStatus app4Log1 = createFileLogPathWithFileStatus(app4.path, DIR_HOST1,
        toDeleteTime);
    PathWithFileStatus app4Log2 = createFileLogPathWithFileStatus(app4.path, DIR_HOST2, toKeepTime);

    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[]{});
    when(mockFs.listStatus(app2.path)).thenReturn(new FileStatus[]{app2Log1.fileStatus,
        app2Log2.fileStatus});
    when(mockFs.listStatus(app3.path)).thenReturn(new FileStatus[]{app3Log1.fileStatus,
        app3Log2.fileStatus});
    when(mockFs.listStatus(app4.path)).thenReturn(new FileStatus[]{app4Log1.fileStatus,
        app4Log2.fileStatus});
    when(mockFs.delete(app3.path, true)).thenThrow(
            new AccessControlException("Injected Error\nStack Trace :("));

    final List<ApplicationId> finishedApplications = Collections.unmodifiableList(
            Arrays.asList(appId1, appId2, appId3));
    final List<ApplicationId> runningApplications = Collections.singletonList(appId4);

    AggregatedLogDeletionService deletionService =
            new AggregatedLogDeletionServiceForTest(runningApplications, finishedApplications);
    deletionService.init(conf);
    deletionService.start();

    int timeout = 2000;
    verify(mockFs, timeout(timeout)).delete(app1.path, true);
    verify(mockFs, timeout(timeout).times(0)).delete(app2.path, true);
    verify(mockFs, timeout(timeout)).delete(app3.path, true);
    verify(mockFs, timeout(timeout).times(0)).delete(app4.path, true);
    verify(mockFs, timeout(timeout)).delete(app4Log1.path, true);
    verify(mockFs, timeout(timeout).times(0)).delete(app4Log2.path, true);

    deletionService.stop();
  }

  @Test
  public void testRefreshLogRetentionSettings() throws Exception {
    long now = System.currentTimeMillis();
    long before2000Secs = now - (2000 * 1000);
    long before50Secs = now - (50 * 1000);
    int checkIntervalSeconds = 2;
    int checkIntervalMilliSeconds = checkIntervalSeconds * 1000;

    Configuration conf = setupConfiguration(1800, 1);

    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem) rootFs).getRawFileSystem();

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationId appId2 = ApplicationId.newInstance(System.currentTimeMillis(), 2);

    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME,
            before50Secs);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX,
            before50Secs);
    PathWithFileStatus bucketDir = createDirBucketDirLogPathWithFileStatus(remoteRootLogPath,
            USER_ME, SUFFIX, appId1, before50Secs);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[] {userDir.fileStatus});

    //Set time last modified of app1Dir directory and its files to before2000Secs 
    PathWithFileStatus app1 = createPathWithFileStatusForAppId(remoteRootLogPath, appId1,
            USER_ME, SUFFIX, before2000Secs);

    //Set time last modified of app1Dir directory and its files to before50Secs
    PathWithFileStatus app2 = createPathWithFileStatusForAppId(remoteRootLogPath, appId2,
            USER_ME, SUFFIX, before50Secs);

    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[]{suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[]{bucketDir.fileStatus});
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[]{app1.fileStatus,
            app2.fileStatus});

    PathWithFileStatus app1Log1 = createFileLogPathWithFileStatus(app1.path, DIR_HOST1,
            before2000Secs);
    PathWithFileStatus app2Log1 = createFileLogPathWithFileStatus(app2.path, DIR_HOST1,
            before50Secs);

    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[] {app1Log1.fileStatus});
    when(mockFs.listStatus(app2.path)).thenReturn(new FileStatus[] {app2Log1.fileStatus});

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1, appId2));

    AggregatedLogDeletionService deletionSvc = new AggregatedLogDeletionServiceForTest(null,
            finishedApplications, conf);
    
    deletionSvc.init(conf);
    deletionSvc.start();
    
    //app1Dir would be deleted since its done above log retention period
    verify(mockFs, timeout(10000)).delete(app1.path, true);
    //app2Dir is not expected to be deleted since it is below the threshold
    verify(mockFs, timeout(3000).times(0)).delete(app2.path, true);

    //Now, let's change the confs
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 50);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
            checkIntervalSeconds);
    //We have not called refreshLogSettings,hence don't expect to see the changed conf values
    assertTrue(checkIntervalMilliSeconds != deletionSvc.getCheckIntervalMsecs());
    
    //refresh the log settings
    deletionSvc.refreshLogRetentionSettings();

    //Check interval time should reflect the new value
    Assert.assertEquals(checkIntervalMilliSeconds, deletionSvc.getCheckIntervalMsecs());
    //app2Dir should be deleted since it falls above the threshold
    verify(mockFs, timeout(10000)).delete(app2.path, true);
    deletionSvc.stop();
  }
  
  @Test
  public void testCheckInterval() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - TEN_DAYS_IN_SECONDS * 1000;

    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME, now);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX, now);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[]{userDir.fileStatus});

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    PathWithFileStatus bucketDir = createDirBucketDirLogPathWithFileStatus(remoteRootLogPath,
            USER_ME, SUFFIX, appId1, now);

    PathWithFileStatus app1 = createPathWithFileStatusForAppId(remoteRootLogPath, appId1,
            USER_ME, SUFFIX, now);
    PathWithFileStatus app1Log1 = createFileLogPathWithFileStatus(app1.path, DIR_HOST1, now);

    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[] {suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[] {bucketDir.fileStatus});
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[] {app1.fileStatus});
    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[]{app1Log1.fileStatus});

    final List<ApplicationId> finishedApplications = Collections.singletonList(appId1);

    AggregatedLogDeletionService deletionSvc = new AggregatedLogDeletionServiceForTest(null,
            finishedApplications);
    deletionSvc.init(conf);
    deletionSvc.start();
 
    verify(mockFs, timeout(10000).atLeast(4)).listStatus(any(Path.class));
    verify(mockFs, never()).delete(app1.path, true);

    // modify the timestamp of the logs and verify if it's picked up quickly
    app1.changeModificationTime(toDeleteTime);
    app1Log1.changeModificationTime(toDeleteTime);
    bucketDir.changeModificationTime(toDeleteTime);
    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[] {suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[] {bucketDir.fileStatus });
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[] {app1.fileStatus });
    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[]{app1Log1.fileStatus});

    verify(mockFs, timeout(10000)).delete(app1.path, true);

    deletionSvc.stop();
  }

  @Test
  public void testRobustLogDeletion() throws Exception {
    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME, 0);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX, 0);
    PathWithFileStatus bucketDir = createDirLogPathWithFileStatus(suffixDir.path, "0", 0);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[]{userDir.fileStatus});
    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[]{suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[]{bucketDir.fileStatus});

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationId appId2 = ApplicationId.newInstance(System.currentTimeMillis(), 2);
    ApplicationId appId3 = ApplicationId.newInstance(System.currentTimeMillis(), 3);

    PathWithFileStatus app1 = createDirLogPathWithFileStatus(bucketDir.path, appId1.toString(), 0);
    PathWithFileStatus app2 = createDirLogPathWithFileStatus(bucketDir.path, "application_a", 0);
    PathWithFileStatus app3 = createDirLogPathWithFileStatus(bucketDir.path, appId3.toString(), 0);
    PathWithFileStatus app3Log3 = createDirLogPathWithFileStatus(app3.path, DIR_HOST1, 0);

    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[]{
        app1.fileStatus,app2.fileStatus, app3.fileStatus});
    when(mockFs.listStatus(app1.path)).thenThrow(
        new RuntimeException("Should be caught and logged"));
    when(mockFs.listStatus(app2.path)).thenReturn(new FileStatus[]{});
    when(mockFs.listStatus(app3.path)).thenReturn(new FileStatus[]{app3Log3.fileStatus});

    final List<ApplicationId> finishedApplications = Collections.unmodifiableList(
            Arrays.asList(appId1, appId3));

    ApplicationClientProtocol rmClient = createMockRMClient(finishedApplications, null);
    AggregatedLogDeletionService.LogDeletionTask deletionTask =
        new AggregatedLogDeletionService.LogDeletionTask(conf, TEN_DAYS_IN_SECONDS, rmClient);
    deletionTask.run();
    verify(mockFs).delete(app3.path, true);
  }

  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }

    public void initialize(URI name, Configuration conf) throws IOException {}
  }

  private static ApplicationClientProtocol createMockRMClient(
      List<ApplicationId> finishedApplications,
      List<ApplicationId> runningApplications) throws Exception {
    final ApplicationClientProtocol mockProtocol = mock(ApplicationClientProtocol.class);
    if (finishedApplications != null && !finishedApplications.isEmpty()) {
      for (ApplicationId appId : finishedApplications) {
        GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response = createApplicationReportWithFinishedApplication();
        when(mockProtocol.getApplicationReport(request)).thenReturn(response);
      }
    }
    if (runningApplications != null && !runningApplications.isEmpty()) {
      for (ApplicationId appId : runningApplications) {
        GetApplicationReportRequest request = GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response = createApplicationReportWithRunningApplication();
        when(mockProtocol.getApplicationReport(request)).thenReturn(response);
      }
    }
    return mockProtocol;
  }

  private static GetApplicationReportResponse createApplicationReportWithRunningApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(
      YarnApplicationState.RUNNING);
    GetApplicationReportResponse response =
        mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }

  private static GetApplicationReportResponse createApplicationReportWithFinishedApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
    GetApplicationReportResponse response = mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }

  private static class PathWithFileStatus {
    private final Path path;
    private FileStatus fileStatus;

    PathWithFileStatus(Path path, FileStatus fileStatus) {
      this.path = path;
      this.fileStatus = fileStatus;
    }

    public void changeModificationTime(long modTime) {
      fileStatus = new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(),
              fileStatus.getReplication(),
              fileStatus.getBlockSize(), modTime, fileStatus.getPath());
    }
  }

  private static class AggregatedLogDeletionServiceForTest extends AggregatedLogDeletionService {
    private final List<ApplicationId> finishedApplications;
    private final List<ApplicationId> runningApplications;
    private final Configuration conf;

    AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                               List<ApplicationId> finishedApplications) {
      this(runningApplications, finishedApplications, null);
    }

    AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                               List<ApplicationId> finishedApplications,
                                               Configuration conf) {
      this.runningApplications = runningApplications;
      this.finishedApplications = finishedApplications;
      this.conf = conf;
    }

    @Override
    protected ApplicationClientProtocol createRMClient() throws IOException {
      try {
        return createMockRMClient(finishedApplications, runningApplications);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    protected Configuration createConf() {
      return conf;
    }

    @Override
    protected void stopRMClient() {
      // DO NOTHING
    }
  }
}
