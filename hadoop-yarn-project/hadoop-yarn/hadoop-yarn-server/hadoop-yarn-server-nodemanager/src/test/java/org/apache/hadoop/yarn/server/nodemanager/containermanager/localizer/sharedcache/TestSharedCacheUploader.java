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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.junit.Test;

public class TestSharedCacheUploader {

  /**
   * If verifyAccess fails, the upload should fail
   */
  @Test
  public void testFailVerifyAccess() throws Exception {
    SharedCacheUploader spied = createSpiedUploader();
    doReturn(false).when(spied).verifyAccess();

    assertFalse(spied.call());
  }

  /**
   * If rename fails, the upload should fail
   */
  @Test
  public void testRenameFail() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    Path localPath = mock(Path.class);
    when(localPath.getName()).thenReturn("foo.jar");
    String user = "joe";
    SCMUploaderProtocol scmClient = mock(SCMUploaderProtocol.class);
    SCMUploaderNotifyResponse response = mock(SCMUploaderNotifyResponse.class);
    when(response.getAccepted()).thenReturn(true);
    when(scmClient.notify(isA(SCMUploaderNotifyRequest.class))).
        thenReturn(response);
    FileSystem fs = mock(FileSystem.class);
    // return false when rename is called
    when(fs.rename(isA(Path.class), isA(Path.class))).thenReturn(false);
    FileSystem localFs = FileSystem.getLocal(conf);
    SharedCacheUploader spied =
        createSpiedUploader(resource, localPath, user, conf, scmClient, fs,
            localFs);
    // stub verifyAccess() to return true
    doReturn(true).when(spied).verifyAccess();
    // stub getActualPath()
    doReturn(localPath).when(spied).getActualPath();
    // stub computeChecksum()
    doReturn("abcdef0123456789").when(spied).computeChecksum(isA(Path.class));
    // stub uploadFile() to return true
    doReturn(true).when(spied).uploadFile(isA(Path.class), isA(Path.class));

    assertFalse(spied.call());
  }

  /**
   * If verifyAccess, uploadFile, rename, and notification succeed, the upload
   * should succeed
   */
  @Test
  public void testSuccess() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    Path localPath = mock(Path.class);
    when(localPath.getName()).thenReturn("foo.jar");
    String user = "joe";
    SCMUploaderProtocol scmClient = mock(SCMUploaderProtocol.class);
    SCMUploaderNotifyResponse response = mock(SCMUploaderNotifyResponse.class);
    when(response.getAccepted()).thenReturn(true);
    when(scmClient.notify(isA(SCMUploaderNotifyRequest.class))).
        thenReturn(response);
    FileSystem fs = mock(FileSystem.class);
    // return false when rename is called
    when(fs.rename(isA(Path.class), isA(Path.class))).thenReturn(true);
    FileSystem localFs = FileSystem.getLocal(conf);
    SharedCacheUploader spied =
        createSpiedUploader(resource, localPath, user, conf, scmClient, fs,
            localFs);
    // stub verifyAccess() to return true
    doReturn(true).when(spied).verifyAccess();
    // stub getActualPath()
    doReturn(localPath).when(spied).getActualPath();
    // stub computeChecksum()
    doReturn("abcdef0123456789").when(spied).computeChecksum(isA(Path.class));
    // stub uploadFile() to return true
    doReturn(true).when(spied).uploadFile(isA(Path.class), isA(Path.class));
    // stub notifySharedCacheManager to return true
    doReturn(true).when(spied).notifySharedCacheManager(isA(String.class),
        isA(String.class));

    assertTrue(spied.call());
  }

  /**
   * If verifyAccess, uploadFile, and rename succed, but it receives a nay from
   * SCM, the file should be deleted
   */
  @Test
  public void testNotifySCMFail() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    Path localPath = mock(Path.class);
    when(localPath.getName()).thenReturn("foo.jar");
    String user = "joe";
    FileSystem fs = mock(FileSystem.class);
    // return false when rename is called
    when(fs.rename(isA(Path.class), isA(Path.class))).thenReturn(true);
    FileSystem localFs = FileSystem.getLocal(conf);
    SharedCacheUploader spied =
        createSpiedUploader(resource, localPath, user, conf, null, fs,
            localFs);
    // stub verifyAccess() to return true
    doReturn(true).when(spied).verifyAccess();
    // stub getActualPath()
    doReturn(localPath).when(spied).getActualPath();
    // stub computeChecksum()
    doReturn("abcdef0123456789").when(spied).computeChecksum(isA(Path.class));
    // stub uploadFile() to return true
    doReturn(true).when(spied).uploadFile(isA(Path.class), isA(Path.class));
    // stub notifySharedCacheManager to return true
    doReturn(false).when(spied).notifySharedCacheManager(isA(String.class),
        isA(String.class));

    assertFalse(spied.call());
    verify(fs).delete(isA(Path.class), anyBoolean());
  }

  /**
   * If resource is public, verifyAccess should succeed
   */
  @Test
  public void testVerifyAccessPublicResource() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    // give public visibility
    when(resource.getVisibility()).thenReturn(LocalResourceVisibility.PUBLIC);
    Path localPath = mock(Path.class);
    when(localPath.getName()).thenReturn("foo.jar");
    String user = "joe";
    SCMUploaderProtocol scmClient = mock(SCMUploaderProtocol.class);
    FileSystem fs = mock(FileSystem.class);
    FileSystem localFs = FileSystem.getLocal(conf);
    SharedCacheUploader spied =
        createSpiedUploader(resource, localPath, user, conf, scmClient, fs,
            localFs);

    assertTrue(spied.verifyAccess());
  }

  /**
   * If the localPath does not exists, getActualPath should get to one level
   * down
   */
  @Test
  public void testGetActualPath() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    // give public visibility
    when(resource.getVisibility()).thenReturn(LocalResourceVisibility.PUBLIC);
    Path localPath = new Path("foo.jar");
    String user = "joe";
    SCMUploaderProtocol scmClient = mock(SCMUploaderProtocol.class);
    FileSystem fs = mock(FileSystem.class);
    FileSystem localFs = mock(FileSystem.class);
    // stub it to return a status that indicates a directory
    FileStatus status = mock(FileStatus.class);
    when(status.isDirectory()).thenReturn(true);
    when(localFs.getFileStatus(localPath)).thenReturn(status);
    SharedCacheUploader spied =
        createSpiedUploader(resource, localPath, user, conf, scmClient, fs,
            localFs);

    Path actualPath = spied.getActualPath();
    assertEquals(actualPath.getName(), localPath.getName());
    assertEquals(actualPath.getParent().getName(), localPath.getName());
  }

  private SharedCacheUploader createSpiedUploader() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);
    LocalResource resource = mock(LocalResource.class);
    Path localPath = mock(Path.class);
    String user = "foo";
    SCMUploaderProtocol scmClient = mock(SCMUploaderProtocol.class);
    FileSystem fs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    return createSpiedUploader(resource, localPath, user, conf, scmClient, fs,
        localFs);
  }

  private SharedCacheUploader createSpiedUploader(LocalResource resource, Path localPath,
      String user, Configuration conf, SCMUploaderProtocol scmClient,
      FileSystem fs, FileSystem localFs)
          throws IOException {
    SharedCacheUploader uploader = new SharedCacheUploader(resource, localPath, user, conf, scmClient,
        fs, localFs);
    return spy(uploader);
  }
}
