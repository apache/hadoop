/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Ozone File system tests that are light weight and use mocks.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ OzoneClientFactory.class, UserGroupInformation.class })
public class TestOzoneFileSystemWithMocks {

  @Test
  public void testFSUriWithHostPortOverrides() throws Exception {
    Configuration conf = new OzoneConfiguration();
    OzoneClient ozoneClient = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);

    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getVolume(eq("volume1"))).thenReturn(volume);
    when(volume.getBucket("bucket1")).thenReturn(bucket);

    PowerMockito.mockStatic(OzoneClientFactory.class);
    PowerMockito.when(OzoneClientFactory.getRpcClient(eq("local.host"),
        eq(5899), eq(conf))).thenReturn(ozoneClient);

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    PowerMockito.mockStatic(UserGroupInformation.class);
    PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    when(ugi.getShortUserName()).thenReturn("user1");

    URI uri = new URI("o3fs://bucket1.volume1.local.host:5899");

    FileSystem fileSystem = FileSystem.get(uri, conf);
    OzoneFileSystem ozfs = (OzoneFileSystem) fileSystem;

    assertEquals(ozfs.getUri().getAuthority(),
        "bucket1.volume1.local.host:5899");
    PowerMockito.verifyStatic();
    OzoneClientFactory.getRpcClient("local.host", 5899, conf);
  }

  @Test
  public void testFSUriHostVersionDefault() throws Exception {
    Configuration conf = new OzoneConfiguration();
    OzoneClient ozoneClient = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    OzoneVolume volume = mock(OzoneVolume.class);
    OzoneBucket bucket = mock(OzoneBucket.class);

    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getVolume(eq("volume1"))).thenReturn(volume);
    when(volume.getBucket("bucket1")).thenReturn(bucket);

    PowerMockito.mockStatic(OzoneClientFactory.class);
    PowerMockito.when(OzoneClientFactory.getRpcClient(eq(conf)))
        .thenReturn(ozoneClient);

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    PowerMockito.mockStatic(UserGroupInformation.class);
    PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
    when(ugi.getShortUserName()).thenReturn("user1");

    URI uri = new URI("o3fs://bucket1.volume1/key");

    FileSystem fileSystem = FileSystem.get(uri, conf);
    OzoneFileSystem ozfs = (OzoneFileSystem) fileSystem;

    assertEquals(ozfs.getUri().getAuthority(), "bucket1.volume1");
    PowerMockito.verifyStatic();
    OzoneClientFactory.getRpcClient(conf);
  }
}
