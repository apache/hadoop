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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

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
    String suffix = "logs";
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "1800");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, suffix);
    
    Path rootPath = new Path(root);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();
    
    Path remoteRootLogPath = new Path(remoteRootLogDir);
    
    Path userDir = new Path(remoteRootLogPath, "me");
    FileStatus userDirStatus = new FileStatus(0, true, 0, 0, toKeepTime, userDir); 
    
    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(
        new FileStatus[]{userDirStatus});
    
    Path userLogDir = new Path(userDir, suffix);
    Path app1Dir = new Path(userLogDir, "application_1_1");
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app1Dir);
    
    Path app2Dir = new Path(userLogDir, "application_1_2");
    FileStatus app2DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app2Dir);
    
    Path app3Dir = new Path(userLogDir, "application_1_3");
    FileStatus app3DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app3Dir);
    
    Path app4Dir = new Path(userLogDir, "application_1_4");
    FileStatus app4DirStatus = new FileStatus(0, true, 0, 0, toDeleteTime, app4Dir);
    
    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[]{app1DirStatus, app2DirStatus, app3DirStatus, app4DirStatus});
    
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
    
    AggregatedLogDeletionService.LogDeletionTask task = 
      new AggregatedLogDeletionService.LogDeletionTask(conf, 1800);
    
    task.run();
    
    verify(mockFs).delete(app1Dir, true);
    verify(mockFs, times(0)).delete(app2Dir, true);
    verify(mockFs).delete(app3Dir, true);
    verify(mockFs).delete(app4Dir, true);
  }

  @Test
  public void testCheckInterval() throws Exception {
    long RETENTION_SECS = 10 * 24 * 3600;
    long now = System.currentTimeMillis();
    long toDeleteTime = now - RETENTION_SECS*1000;

    String root = "mockfs://foo/";
    String remoteRootLogDir = root+"tmp/logs";
    String suffix = "logs";
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.set(YarnConfiguration.LOG_AGGREGATION_ENABLED, "true");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, "864000");
    conf.set(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS, "1");
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteRootLogDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, suffix);

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

    Path userLogDir = new Path(userDir, suffix);
    Path app1Dir = new Path(userLogDir, "application_1_1");
    FileStatus app1DirStatus = new FileStatus(0, true, 0, 0, now, app1Dir);

    when(mockFs.listStatus(userLogDir)).thenReturn(
        new FileStatus[]{app1DirStatus});

    Path app1Log1 = new Path(app1Dir, "host1");
    FileStatus app1Log1Status = new FileStatus(10, false, 1, 1, now, app1Log1);

    when(mockFs.listStatus(app1Dir)).thenReturn(
        new FileStatus[]{app1Log1Status});

    AggregatedLogDeletionService deletionSvc =
        new AggregatedLogDeletionService();
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
  
  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }
    public void initialize(URI name, Configuration conf) throws IOException {}
  }
}
