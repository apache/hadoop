/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.HdfsManifestToResourcesPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageManifest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests the hdfs manifest to resources plugin used by the
 * RuncContainerRuntime to map an image manifest into a list of local resources.
 */
public class TestHdfsManifestToResourcesPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHdfsManifestToResourcesPlugin.class);
  private Configuration conf;
  private String tmpPath = new StringBuffer(
      System.getProperty("test.build.data"))
      .append('/').append("hadoop.tmp.dir").toString();
  private static final String LAYER_MEDIA_TYPE =
      "application/vnd.docker.image.rootfs.diff.tar.gzip";
  private static final String CONFIG_MEDIA_TYPE =
      "application/vnd.docker.container.image.v1+json";

  @Before
  public void setup() {
    conf = new Configuration();
    File tmpDir = new File(tmpPath);
    tmpDir.mkdirs();
  }

  @After
  public void cleanUp() throws IOException {
    File tmpDir = new File(tmpPath);
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testGetLayerResources() throws IOException {
    ImageManifest mockManifest = mock(ImageManifest.class);
    ImageManifest.Blob mockLayer1 = mock(ImageManifest.Blob.class);
    ImageManifest.Blob mockLayer2 = mock(ImageManifest.Blob.class);
    String digest1Hash =
        "e060f9dd9e8cd9ec0e2814b661a96d78f7298120d7654ba9f83ebfb11ff1fb1e";
    String digest2Hash =
        "5af5ff88469c8473487bfbc2fe81b4e7d84644bd91f1ab9305de47ef5673637e";
    String digest1 =
        "sha256:" + digest1Hash;
    String digest2 =
        "sha256:" + digest2Hash;
    long size1 = 1234;
    long size2 = 5678;

    when(mockLayer1.getMediaType()).thenReturn(LAYER_MEDIA_TYPE);
    when(mockLayer1.getDigest()).thenReturn(digest1);
    when(mockLayer1.getSize()).thenReturn(size1);

    when(mockLayer2.getMediaType()).thenReturn(LAYER_MEDIA_TYPE);
    when(mockLayer2.getDigest()).thenReturn(digest2);
    when(mockLayer2.getSize()).thenReturn(size2);

    ArrayList<ImageManifest.Blob> mockLayers = new ArrayList<>();
    mockLayers.add(mockLayer1);
    mockLayers.add(mockLayer2);

    when(mockManifest.getLayers()).thenReturn(mockLayers);

    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);
    long modTime = 123456789;

    HdfsManifestToResourcesPlugin hdfsManifestToResourcesPlugin =
        new HdfsManifestToResourcesPlugin() {
          @Override
          protected FileStatus statBlob(Path path) throws IOException {
            FileStatus mockFileStatus = mock(FileStatus.class);
            when(mockFileStatus.getModificationTime()).thenReturn(modTime);
            return mockFileStatus;
          }
        };
    hdfsManifestToResourcesPlugin.init(conf);

    List<LocalResource> returnedLayers =
        hdfsManifestToResourcesPlugin.getLayerResources(mockManifest);

    URL url1 = URL.fromPath(new Path(tmpPath + "/layers",
        digest1Hash + ".sqsh"));
    URL url2 = URL.fromPath(new Path(tmpPath + "/layers",
        digest2Hash + ".sqsh"));

    LocalResource rsrc1 = LocalResource.newInstance(url1,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
        size1, modTime);
    LocalResource rsrc2 = LocalResource.newInstance(url2,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
        size2, modTime);

    Assert.assertEquals(rsrc1, returnedLayers.get(0));
    Assert.assertEquals(rsrc2, returnedLayers.get(1));

  }

  @Test
  public void testGetConfigResources() throws IOException {
    ImageManifest mockManifest = mock(ImageManifest.class);
    ImageManifest.Blob mockConfig = mock(ImageManifest.Blob.class);
    String digestHash =
        "e23cac476d0238f0f859c1e07e5faad85262bca490ef5c3a9da32a5b39c6b204";
    String digest =
        "sha256:" + digestHash;
    long size = 1234;

    when(mockConfig.getMediaType()).thenReturn(CONFIG_MEDIA_TYPE);
    when(mockConfig.getDigest()).thenReturn(digest);
    when(mockConfig.getSize()).thenReturn(size);
    when(mockManifest.getConfig()).thenReturn(mockConfig);

    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);
    long modTime = 123456789;

    HdfsManifestToResourcesPlugin hdfsManifestToResourcesPlugin =
        new HdfsManifestToResourcesPlugin() {
          @Override
          protected FileStatus statBlob(Path path) throws IOException {
            FileStatus mockFileStatus = mock(FileStatus.class);
            when(mockFileStatus.getModificationTime()).thenReturn(modTime);
            return mockFileStatus;
          }
        };
    hdfsManifestToResourcesPlugin.init(conf);

    LocalResource returnedLayer =
        hdfsManifestToResourcesPlugin.getConfigResource(mockManifest);

    URL url1 = URL.fromPath(new Path(tmpPath + "/config", digestHash));

    LocalResource rsrc = LocalResource.newInstance(url1,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
        size, modTime);

    Assert.assertEquals(rsrc, returnedLayer);
  }
}
