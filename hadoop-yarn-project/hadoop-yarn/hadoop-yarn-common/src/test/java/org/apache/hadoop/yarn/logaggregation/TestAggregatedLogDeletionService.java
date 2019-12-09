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

import static org.mockito.Mockito.*;

public class TestAggregatedLogDeletionService {
  
  @Before
  public void closeFilesystems() throws IOException {
    // prevent the same mockfs instance from being reused due to FS cache
    FileSystem.closeAll();
  }

  @Test
  public void testDeletion() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000*1000);
    long toKeepTime = now - (1500*1000);
    
    String root = "mockfs://foo/";
    String remoteRootLogDir = root+"tmp/logs";
    String configuredSuffix = "logs";
    String actualSuffix = "logs-tfile";
    final Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "1800");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, configuredSuffix);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, "TFile"),
        LogAggregationTFileController.class.getName());
    
    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();
    
    Path remoteRootLogPath = new Path(remoteRootLogDir);
    
    Path userDir = new Path(remoteRootLogPath, "me");
    FileStatus userDirStatus = new FileStatus(0, true, 0, 0, toKeepTime, userDir); 
    
    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(
        new FileStatus[]{userDirStatus});

    ApplicationId appId1 =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    Path userLogDir = new Path(userDir, actualSuffix);
    Path app1Dir = new Path(userLogDir, appId1.toString());
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app1Dir);
    
    ApplicationId appId2 =
        ApplicationId.newInstance(System.currentTimeMillis(), 2);
    Path app2Dir = new Path(userLogDir, appId2.toString());
    FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app2Dir);
    
    ApplicationId appId3 =
        ApplicationId.newInstance(System.currentTimeMillis(), 3);
    Path app3Dir = new Path(userLogDir, appId3.toString());
    FileStatus app3DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app3Dir);
    
    ApplicationId appId4 =
        ApplicationId.newInstance(System.currentTimeMillis(), 4);
    Path app4Dir = new Path(userLogDir, appId4.toString());
    FileStatus app4DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app4Dir);
    
    ApplicationId appId5 =
        ApplicationId.newInstance(System.currentTimeMillis(), 5);
    Path app5Dir = new Path(userLogDir, appId5.toString());
    FileStatus app5DirStatus =
        new FileStatus(0, true, 0, 0, toDeleteTime, app5Dir);

    when(mockFs.listStatus(userLogDir)).thenReturn(
      new FileStatus[] { app1DirStatus, app2DirStatus, app3DirStatus,
          app4DirStatus, app5DirStatus });
    
    when(mockFs.listStatus(app1Dir)).thenReturn(
        new FileStatus[]{});
    
    Path app2Log1 = new Path(app2Dir, "host1");
    FileStatus app2Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app2Log1);
    
    Path app2Log2 = new Path(app2Dir, "host2");
    FileStatus app2Log2Status = new FileStatus(10, false, 1, 1, toKeepTime, app2Log2);
    
    when(mockFs.listStatus(app2Dir)).thenReturn(
        new FileStatus[]{app2Log1Status, app2Log2Status});
    
    Path app3Log1 = new Path(app3Dir, "host1");
    FileStatus app3Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app3Log1);
    
    Path app3Log2 = new Path(app3Dir, "host2");
    FileStatus app3Log2Status = new FileStatus(10, false, 1, 1, toDeleteTime, app3Log2);
    
    when(mockFs.delete(app3Dir, true)).thenThrow(new AccessControlException("Injected Error\nStack Trace :("));
    
    when(mockFs.listStatus(app3Dir)).thenReturn(
        new FileStatus[]{app3Log1Status, app3Log2Status});
    
    Path app4Log1 = new Path(app4Dir, "host1");
    FileStatus app4Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app4Log1);
    
    Path app4Log2 = new Path(app4Dir, "host2");
    FileStatus app4Log2Status = new FileStatus(10, false, 1, 1, toDeleteTime, app4Log2);
    
    when(mockFs.listStatus(app4Dir)).thenReturn(
        new FileStatus[]{app4Log1Status, app4Log2Status});

    Path app5Log1 = new Path(app5Dir, "host1");
    FileStatus app5Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app5Log1);
    
    Path app5Log2 = new Path(app5Dir, "host2");
    FileStatus app5Log2Status = new FileStatus(10, false, 1, 1, toKeepTime, app5Log2);

    when(mockFs.listStatus(app5Dir)).thenReturn(
        new FileStatus[]{app5Log1Status, app5Log2Status});

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1, appId2, appId3,
          appId4));
    final List<ApplicationId> runningApplications =
        Collections.unmodifiableList(Arrays.asList(appId5));

    AggregatedLogDeletionService deletionService =
        new AggregatedLogDeletionService() {
          @Override
          protected ApplicationClientProtocol createRMClient()
              throws IOException {
            try {
              return createMockRMClient(finishedApplications,
                runningApplications);
            } catch (Exception e) {
              throw new IOException(e);
            }
          }
          @Override
          protected void stopRMClient() {
            // DO NOTHING
          }
        };
    deletionService.init(conf);
    deletionService.start();

    verify(mockFs, timeout(2000)).delete(app1Dir, true);
    verify(mockFs, timeout(2000).times(0)).delete(app2Dir, true);
    verify(mockFs, timeout(2000)).delete(app3Dir, true);
    verify(mockFs, timeout(2000)).delete(app4Dir, true);
    verify(mockFs, timeout(2000).times(0)).delete(app5Dir, true);
    verify(mockFs, timeout(2000)).delete(app5Log1, true);
    verify(mockFs, timeout(2000).times(0)).delete(app5Log2, true);

    deletionService.stop();
  }

  @Test
  public void testRefreshLogRetentionSettings() throws Exception {
    long now = System.currentTimeMillis();
    //time before 2000 sec
    long before2000Secs = now - (2000 * 1000);
    //time before 50 sec
    long before50Secs = now - (50 * 1000);
    String root = "mockfs://foo/";
    String remoteRootLogDir = root + "tmp/logs";
    String configuredSuffix = "logs";
    String actualSuffix = "logs-tfile";
    final Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "1800");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
        "1");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, configuredSuffix);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, "TFile"),
        LogAggregationTFileController.class.getName());

    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem) rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(remoteRootLogDir);

    Path userDir = new Path(remoteRootLogPath, "me");
    FileStatus userDirStatus = new FileStatus(0, true, 0, 0, before50Secs,
        userDir);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(
        new FileStatus[] { userDirStatus });

    Path userLogDir = new Path(userDir, actualSuffix);

    ApplicationId appId1 =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    //Set time last modified of app1Dir directory and its files to before2000Secs 
    Path app1Dir = new Path(userLogDir, appId1.toString());
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, before2000Secs,
        app1Dir);
    
    ApplicationId appId2 =
        ApplicationId.newInstance(System.currentTimeMillis(), 2);
    //Set time last modified of app1Dir directory and its files to before50Secs 
    Path app2Dir = new Path(userLogDir, appId2.toString());
    FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, before50Secs,
        app2Dir);

    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[] { app1DirStatus, app2DirStatus });

    Path app1Log1 = new Path(app1Dir, "host1");
    FileStatus app1Log1Status = new FileStatus(10, false, 1, 1, before2000Secs,
        app1Log1);

    when(mockFs.listStatus(app1Dir)).thenReturn(
        new FileStatus[] { app1Log1Status });

    Path app2Log1 = new Path(app2Dir, "host1");
    FileStatus app2Log1Status = new FileStatus(10, false, 1, 1, before50Secs,
        app2Log1);

    when(mockFs.listStatus(app2Dir)).thenReturn(
        new FileStatus[] { app2Log1Status });

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1, appId2));

    AggregatedLogDeletionService deletionSvc = new AggregatedLogDeletionService() {
      @Override
      protected Configuration createConf() {
        return conf;
      }
      @Override
      protected ApplicationClientProtocol createRMClient()
          throws IOException {
        try {
          return createMockRMClient(finishedApplications, null);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      @Override
      protected void stopRMClient() {
        // DO NOTHING
      }
    };
    
    deletionSvc.init(conf);
    deletionSvc.start();
    
    //app1Dir would be deleted since its done above log retention period
    verify(mockFs, timeout(10000)).delete(app1Dir, true);
    //app2Dir is not expected to be deleted since its below the threshold
    verify(mockFs, timeout(3000).times(0)).delete(app2Dir, true);

    //Now,lets change the confs
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "50");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
        "2");
    //We have not called refreshLogSettings,hence don't expect to see the changed conf values
    Assert.assertTrue(2000l != deletionSvc.getCheckIntervalMsecs());
    
    //refresh the log settings
    deletionSvc.refreshLogRetentionSettings();

    //Check interval time should reflect the new value
    Assert.assertTrue(2000l == deletionSvc.getCheckIntervalMsecs());
    //app2Dir should be deleted since it falls above the threshold
    verify(mockFs, timeout(10000)).delete(app2Dir, true);
    deletionSvc.stop();
  }
  
  @Test
  public void testCheckInterval() throws Exception {
    long RETENTION_SECS = 10 * 24 * 3600;
    long now = System.currentTimeMillis();
    long toDeleteTime = now - RETENTION_SECS*1000;

    String root = "mockfs://foo/";
    String remoteRootLogDir = root+"tmp/logs";
    String configuredSuffix = "logs";
    String actualSuffix = "logs-tfile";
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "864000");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS, "1");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, configuredSuffix);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, "TFile"),
        LogAggregationTFileController.class.getName());

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(remoteRootLogDir);

    Path userDir = new Path(remoteRootLogPath, "me");
    FileStatus userDirStatus = new FileStatus(0, true, 0, 0, now, userDir);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(
        new FileStatus[]{userDirStatus});

    ApplicationId appId1 =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    Path userLogDir = new Path(userDir, actualSuffix);
    Path app1Dir = new Path(userLogDir, appId1.toString());
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, now, app1Dir);

    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[]{app1DirStatus});

    Path app1Log1 = new Path(app1Dir, "host1");
    FileStatus app1Log1Status = new FileStatus(10, false, 1, 1, now, app1Log1);

    when(mockFs.listStatus(app1Dir)).thenReturn(
        new FileStatus[]{app1Log1Status});

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1));

    AggregatedLogDeletionService deletionSvc =
        new AggregatedLogDeletionService() {
      @Override
      protected ApplicationClientProtocol createRMClient()
          throws IOException {
        try {
          return createMockRMClient(finishedApplications, null);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      @Override
      protected void stopRMClient() {
        // DO NOTHING
      }
    };
    deletionSvc.init(conf);
    deletionSvc.start();
 
    verify(mockFs, timeout(10000).atLeast(4)).listStatus(any(Path.class));
    verify(mockFs, never()).delete(app1Dir, true);

    // modify the timestamp of the logs and verify it's picked up quickly
    app1DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app1Dir);
    app1Log1Status = new FileStatus(10, false, 1, 1, toDeleteTime, app1Log1);
    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[]{app1DirStatus});
    when(mockFs.listStatus(app1Dir)).thenReturn(
        new FileStatus[]{app1Log1Status});

    verify(mockFs, timeout(10000)).delete(app1Dir, true);

    deletionSvc.stop();
  }

  @Test
  public void testRobustLogDeletion() throws Exception {
    final long RETENTION_SECS = 10 * 24 * 3600;

    String root = "mockfs://foo/";
    String remoteRootLogDir = root+"tmp/logs";
    String configuredSuffix = "logs";
    String actualSuffix = "logs-tfile";
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class,
        FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "864000");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
        "1");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, configuredSuffix);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, "TFile"),
        LogAggregationTFileController.class.getName());

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(remoteRootLogDir);

    Path userDir = new Path(remoteRootLogPath, "me");
    FileStatus userDirStatus = new FileStatus(0, true, 0, 0, 0, userDir);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(
        new FileStatus[]{userDirStatus});

    Path userLogDir = new Path(userDir, actualSuffix);
    ApplicationId appId1 =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    Path app1Dir = new Path(userLogDir, appId1.toString());
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, 0, app1Dir);
    ApplicationId appId2 =
        ApplicationId.newInstance(System.currentTimeMillis(), 2);
    Path app2Dir = new Path(userLogDir, "application_a");
    FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, 0, app2Dir);
    ApplicationId appId3 =
        ApplicationId.newInstance(System.currentTimeMillis(), 3);
    Path app3Dir = new Path(userLogDir, appId3.toString());
    FileStatus app3DirStatus = new FileStatus(0, true, 0, 0, 0, app3Dir);

    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[]{app1DirStatus, app2DirStatus, app3DirStatus});

    when(mockFs.listStatus(app1Dir)).thenThrow(
        new RuntimeException("Should Be Caught and Logged"));
    Path app3Log3 = new Path(app3Dir, "host1");
    FileStatus app3Log3Status = new FileStatus(10, false, 1, 1, 0, app3Log3);
    when(mockFs.listStatus(app3Dir)).thenReturn(
        new FileStatus[]{app3Log3Status});

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1, appId3));

    ApplicationClientProtocol rmClient =
        createMockRMClient(finishedApplications, null);
    AggregatedLogDeletionService.LogDeletionTask deletionTask =
        new AggregatedLogDeletionService.LogDeletionTask(conf,
            RETENTION_SECS,
            rmClient);
    deletionTask.run();
    verify(mockFs).delete(app3Dir, true);
  }

  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }
    public void initialize(URI name, Configuration conf) throws IOException {}
  }

  private static ApplicationClientProtocol createMockRMClient(
      List<ApplicationId> finishedApplicaitons,
      List<ApplicationId> runningApplications) throws Exception {
    final ApplicationClientProtocol mockProtocol =
        mock(ApplicationClientProtocol.class);
    if (finishedApplicaitons != null && !finishedApplicaitons.isEmpty()) {
      for (ApplicationId appId : finishedApplicaitons) {
        GetApplicationReportRequest request =
            GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response =
            createApplicationReportWithFinishedApplication();
        when(mockProtocol.getApplicationReport(request))
          .thenReturn(response);
      }
    }
    if (runningApplications != null && !runningApplications.isEmpty()) {
      for (ApplicationId appId : runningApplications) {
        GetApplicationReportRequest request =
            GetApplicationReportRequest.newInstance(appId);
        GetApplicationReportResponse response =
            createApplicationReportWithRunningApplication();
        when(mockProtocol.getApplicationReport(request))
          .thenReturn(response);
      }
    }
    return mockProtocol;
  }

  private static GetApplicationReportResponse
      createApplicationReportWithRunningApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(
      YarnApplicationState.RUNNING);
    GetApplicationReportResponse response =
        mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }

  private static GetApplicationReportResponse
      createApplicationReportWithFinishedApplication() {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getYarnApplicationState()).thenReturn(
      YarnApplicationState.FINISHED);
    GetApplicationReportResponse response =
        mock(GetApplicationReportResponse.class);
    when(response.getApplicationReport()).thenReturn(report);
    return response;
  }
}
