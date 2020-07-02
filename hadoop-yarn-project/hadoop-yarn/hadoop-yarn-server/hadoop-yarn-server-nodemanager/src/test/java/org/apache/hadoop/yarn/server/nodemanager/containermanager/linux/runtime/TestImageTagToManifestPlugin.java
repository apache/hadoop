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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageManifest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestPlugin;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests the hdfs manifest to resources plugin used by the
 * RuncContainerRuntime to map an image manifest into a list of local resources.
 */
public class TestImageTagToManifestPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestImageTagToManifestPlugin.class);
  private MockImageTagToManifestPlugin mockImageTagToManifestPlugin;
  private Configuration conf;
  private String tmpPath =
      new StringBuffer(System.getProperty("test.build.data"))
      .append('/').append("hadoop.tmp.dir").toString();
  private ObjectMapper mapper;

  private String manifestJson =
      "{\n" +
      "   \"schemaVersion\": 2,\n" +
      "   \"mediaType\": \"application/vnd.docker.distribution.manifest.v2+json\",\n" +
      "   \"config\": {\n" +
      "      \"mediaType\": \"application/vnd.docker.container.image.v1+json\",\n" +
      "      \"size\": 2857,\n" +
      "      \"digest\": \"sha256:e23cac476d0238f0f859c1e07e5faad85262bca490ef5c3a9da32a5b39c6b204\"\n" +
      "   },\n" +
      "   \"layers\": [\n" +
      "      {\n" +
      "         \"mediaType\": \"application/vnd.docker.image.rootfs.diff.tar.gzip\",\n" +
      "         \"size\": 185784329,\n" +
      "         \"digest\": \"sha256:e060f9dd9e8cd9ec0e2814b661a96d78f7298120d7654ba9f83ebfb11ff1fb1e\"\n" +
      "      },\n" +
      "      {\n" +
      "         \"mediaType\": \"application/vnd.docker.image.rootfs.diff.tar.gzip\",\n" +
      "         \"size\": 10852,\n" +
      "         \"digest\": \"sha256:5af5ff88469c8473487bfbc2fe81b4e7d84644bd91f1ab9305de47ef5673637e\"\n" +
      "      }\n" +
      "   ]\n" +
      "}";

  @Before
  public void setup() {
    conf = new Configuration();
    mapper = new ObjectMapper();
    File tmpDir = new File(tmpPath);
    tmpDir.mkdirs();
  }

  @After
  public void cleanUp() throws IOException {
    File tmpDir = new File(tmpPath);
    FileUtils.deleteDirectory(tmpDir);
  }

  /**
   * This class mocks the hdfs manifest to resources plugin used by the
   * RuncContainerRuntime to enable testing.
   */
  public class MockImageTagToManifestPlugin extends ImageTagToManifestPlugin {
    private BufferedReader mockLocalBufferedReader;
    private BufferedReader mockHdfsBufferedReader;

    MockImageTagToManifestPlugin(BufferedReader mockLocalBufferedReader,
        BufferedReader mockHdfsBufferedReader) {
      super();
      this.mockLocalBufferedReader = mockLocalBufferedReader;
      this.mockHdfsBufferedReader = mockHdfsBufferedReader;
    }

    @Override
    protected BufferedReader getLocalImageToHashReader() throws IOException {
      return mockLocalBufferedReader;
    }

    @Override
    protected BufferedReader getHdfsImageToHashReader() throws IOException {
      return mockHdfsBufferedReader;
    }
  }


  @Test
  public void testLocalGetHashFromImageTag() throws IOException {
    BufferedReader mockLocalBufferedReader = mock(BufferedReader.class);
    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    String commentImage = "commentimage:latest";
    String commentImageHash =
        "142fff813433c1faa8796388db3a1fa1e899ba08c9e42ad2e33c42696d0f15d2";

    String fakeImageLatest = "fakeimage:latest";
    String fakeImageCurrent= "fakeimage:current";
    String fakeImageHash =
        "f75903872eb2963e158502ef07f2e56d3a2e90a012b4afe3440461b54142a567";

    String busyboxImage = "repo/busybox:123";
    String busyboxHash =
        "c6912b9911deceec6c43ebb4c31c96374a8ebb3de4cd75f377dba6c07707de6e";

    String commentLine = "#" + commentImage + commentImageHash + "#2nd comment";
    String busyboxLine = busyboxImage + ":" + busyboxHash + "#comment";
    String fakeImageLine = fakeImageLatest + "," + fakeImageCurrent + ":"
        + fakeImageHash + "#fakeimage comment";

    when(mockLocalBufferedReader.readLine()).thenReturn(commentLine,
        fakeImageLine, busyboxLine, null);

    mockImageTagToManifestPlugin = new MockImageTagToManifestPlugin(
        mockLocalBufferedReader, mockHdfsBufferedReader);
    mockImageTagToManifestPlugin.loadImageToHashFiles();

    String returnedFakeImageHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(fakeImageLatest);
    String returnedBusyboxHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(busyboxImage);
    String returnedCommentHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(commentImage);

    Assert.assertEquals(fakeImageHash, returnedFakeImageHash);
    Assert.assertEquals(busyboxHash, returnedBusyboxHash);

    //Image hash should not be found, so returned hash should be the tag
    Assert.assertEquals(commentImage, returnedCommentHash);
  }

  @Test
  public void testHdfsGetHashFromImageTag() throws IOException {
    BufferedReader mockLocalBufferedReader = mock(BufferedReader.class);
    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    String commentImage = "commentimage:latest";
    String commentImageHash =
        "142fff813433c1faa8796388db3a1fa1e899ba08c9e42ad2e33c42696d0f15d2";

    String fakeImageLatest = "fakeimage:latest";
    String fakeImageCurrent= "fakeimage:current";
    String fakeImageHash =
        "f75903872eb2963e158502ef07f2e56d3a2e90a012b4afe3440461b54142a567";

    String busyboxImage = "repo/busybox:123";
    String busyboxHash =
        "c6912b9911deceec6c43ebb4c31c96374a8ebb3de4cd75f377dba6c07707de6e";

    String commentLine = "#" + commentImage + commentImageHash + "#2nd comment";
    String busyboxLine = busyboxImage + ":" + busyboxHash + "#comment";
    String fakeImageLine = fakeImageLatest + "," + fakeImageCurrent + ":"
        + fakeImageHash + "#fakeimage comment";

    when(mockHdfsBufferedReader.readLine()).thenReturn(commentLine,
        fakeImageLine, busyboxLine, null);

    mockImageTagToManifestPlugin = new MockImageTagToManifestPlugin(
        mockLocalBufferedReader, mockHdfsBufferedReader);
    mockImageTagToManifestPlugin.loadImageToHashFiles();

    String returnedFakeImageHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(fakeImageLatest);
    String returnedBusyboxHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(busyboxImage);
    String returnedCommentHash = mockImageTagToManifestPlugin
        .getHashFromImageTag(commentImage);

    Assert.assertEquals(fakeImageHash, returnedFakeImageHash);
    Assert.assertEquals(busyboxHash, returnedBusyboxHash);

    //Image hash should not be found, so returned hash should be the tag
    Assert.assertEquals(commentImage, returnedCommentHash);
  }

  @Test
  public void testGetManifestFromImageTag() throws IOException {
    String manifestPath = tmpPath + "/manifests";
    File manifestDir = new File(manifestPath);
    manifestDir.mkdirs();

    conf.set(NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE, "local-image-tag-to-hash");
    conf.set(NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE, "hdfs-image-tag-to-hash");
    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);
    String manifestHash =
        "d0e8c542d28e8e868848aeb58beecb31079eb7ada1293c4bc2eded08daed605a";

    PrintWriter printWriter = new PrintWriter(
        manifestPath + "/" + manifestHash);
    printWriter.println(manifestJson);
    printWriter.close();

    BufferedReader mockLocalBufferedReader = mock(BufferedReader.class);
    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    mockImageTagToManifestPlugin = new MockImageTagToManifestPlugin(
        mockLocalBufferedReader, mockHdfsBufferedReader) {
      @Override
      public String getHashFromImageTag(String imageTag) {
        return manifestHash;
      }
    };
    mockImageTagToManifestPlugin.init(conf);

    ImageManifest manifest = mockImageTagToManifestPlugin
        .getManifestFromImageTag("image");
    ImageManifest expectedManifest =
        mapper.readValue(manifestJson, ImageManifest.class);
    Assert.assertEquals(expectedManifest.toString(), manifest.toString());
  }
}
