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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestV2Plugin;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.mockito.Mockito.mock;

/**
 * This class tests the hdfs manifest to resources V2 plugin used by the
 * RuncContainerRuntime to map an image manifest into a list of local resources.
 */
public class TestImageTagToManifestV2Plugin {
  private MockImageTagToManifestV2Plugin mockImageTagToManifestV2Plugin;
  private Configuration conf;
  private final String tmpPath = System.getProperty("test.build.data") +
      '/' + "hadoop.tmp.dir";
  private ObjectMapper mapper;

  private final String manifestJson =
      "{\n" +
        "   \"schemaVersion\": 2,\n" +
        "   \"mediaType\": \"application/vnd.docker.distribution.manifest." +
        "v2+json\",\n" +
        "   \"config\": {\n" +
        "      \"mediaType\": \"application/vnd.docker.container.image." +
        "v1+json\",\n" +
        "      \"size\": 2857,\n" +
        "      \"digest\": \"sha256:e23cac476d0238f0f859c1e07e5faad85262bc" +
        "a490ef5c3a9da32a5b39c6b204\"\n" +
        "   },\n" +
        "   \"layers\": [\n" +
        "      {\n" +
        "         \"mediaType\": \"application/vnd.docker.image.rootfs.diff." +
        "tar.gzip\",\n" +
        "         \"size\": 185784329,\n" +
        "         \"digest\": \"sha256:e060f9dd9e8cd9ec0e2814b661a96d78f72" +
        "98120d7654ba9f83ebfb11ff1fb1e\"\n" +
        "      },\n" +
        "      {\n" +
        "         \"mediaType\": \"application/vnd.docker.image.rootfs.diff." +
        "tar.gzip\",\n" +
        "         \"size\": 10852,\n" +
        "         \"digest\": \"sha256:5af5ff88469c8473487bfbc2fe81b4e7d8464" +
        "4bd91f1ab9305de47ef5673637e\"\n" +
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
  public static class MockImageTagToManifestV2Plugin
      extends ImageTagToManifestV2Plugin {
    private final BufferedReader mockHdfsBufferedReader;

    MockImageTagToManifestV2Plugin(BufferedReader mockHdfsBufferedReader) {
      super();
      this.mockHdfsBufferedReader = mockHdfsBufferedReader;
    }

    @Override
    protected BufferedReader getHdfsImageToHashReader(String imageTag,
        String imageNamespace) {
      return mockHdfsBufferedReader;
    }
  }

  @Test
  public void testHdfsGetHashFromImageTag() throws IOException {
    String imageFile = "busybox@123.properties";
    String imageTag = "busybox:123";
    String imageHash =
        "f650652f6914b7549cdf8b98866af75d26635ff7947f72c6fd239109dd1a50eb";

    // Test image property file
    FileWriter fw = new FileWriter(tmpPath + imageFile);
    fw.append("#Mon Mar 08 15:15:15 CST 2021\n");
    fw.append("runc.import.source=hub.docker.com/repo/busybox:123\n");
    fw.append("runc.import.type=docker\n");
    fw.append("runc.import.time=2021-03-08T21:15:14.652Z\n");
    fw.append("runc.manifest=sha256:f650652f6914b7549cdf8b98866af75d26635" +
        "ff7947f72c6fd239109dd1a50eb\n");
    fw.close();

    BufferedReader br =
        new BufferedReader(new FileReader(tmpPath + imageFile));
    mockImageTagToManifestV2Plugin = new MockImageTagToManifestV2Plugin(br);
    mockImageTagToManifestV2Plugin.init(conf);
    String returnedHash =
        mockImageTagToManifestV2Plugin.getHashFromImageTag(imageTag);
    br.close();

    Assert.assertEquals(imageHash, returnedHash);
  }

  @Test
  public void testHdfsGetHashFromImageTagWrongHashFails() throws IOException {
    String imageFile = "busybox@123.properties";
    String imageTag = "busybox:123";
    String imageHash =
        "f650652f6914b7549cdf8b98866af75d26635ff7947f72c6fd239109dd1a50eb";

    // Test image property file, non-matching hash
    FileWriter fw = new FileWriter(tmpPath + imageFile);
    fw.append("#Mon Mar 08 15:15:15 CST 2021\n");
    fw.append("runc.import.source=hub.docker.com/repo/busybox:123\n");
    fw.append("runc.import.type=docker\n");
    fw.append("runc.import.time=2021-03-08T21:15:14.652Z\n");
    fw.append("runc.manifest=sha256:aaaaaaa12344b7549cdf8b98866af75d266" +
        "35ff7947f72c6fd239109dd1a50eb\n");
    fw.close();

    BufferedReader br =
        new BufferedReader(new FileReader(tmpPath + imageFile));
    mockImageTagToManifestV2Plugin = new MockImageTagToManifestV2Plugin(br);
    mockImageTagToManifestV2Plugin.init(conf);
    String returnedHash =
        mockImageTagToManifestV2Plugin.getHashFromImageTag(imageTag);
    br.close();

    Assert.assertNotEquals(imageHash, returnedHash);
  }

  @Test
  public void testHdfsGetHashFromImageTagFailsMissingNull() throws IOException {
    String imageFile = "busybox@123.properties";
    String imageTag = "busybox:123";

    // Test image property file, missing manifest line
    FileWriter fw = new FileWriter(tmpPath + imageFile);
    fw.append("#Mon Mar 08 15:15:15 CST 2021\n");
    fw.append("runc.import.source=hub.docker.com/repo/busybox:123\n");
    fw.append("runc.import.type=docker\n");
    fw.append("runc.import.time=2021-03-08T21:15:14.652Z\n");
    fw.close();

    BufferedReader br =
        new BufferedReader(new FileReader(tmpPath + imageFile));
    mockImageTagToManifestV2Plugin = new MockImageTagToManifestV2Plugin(br);
    mockImageTagToManifestV2Plugin.init(conf);
    String returnedHash =
        mockImageTagToManifestV2Plugin.getHashFromImageTag(imageTag);
    br.close();

    Assert.assertNull(returnedHash);
  }

  @Test
  public void testHdfsGetHashFromImageTagFailsParseNull() throws IOException {
    String imageFile = "busybox@123.properties";
    String imageTag = "busybox:123";

    // Test image property file, missing : to parse
    FileWriter fw = new FileWriter(tmpPath + imageFile);
    fw.append("#Mon Mar 08 15:15:15 CST 2021\n");
    fw.append("runc.import.source=hub.docker.com/repo/busybox:123\n");
    fw.append("runc.import.type=docker\n");
    fw.append("runc.import.time=2021-03-08T21:15:14.652Z\n");
    fw.append("runc.manifest=sha256aaaaaaa12344b7549cdf8b98866af75d26635" +
        "ff7947f72c6fd239109dd1a50eb\n");
    fw.close();

    BufferedReader br =
        new BufferedReader(new FileReader(tmpPath + imageFile));
    mockImageTagToManifestV2Plugin = new MockImageTagToManifestV2Plugin(br);
    mockImageTagToManifestV2Plugin.init(conf);
    String returnedHash =
        mockImageTagToManifestV2Plugin.getHashFromImageTag(imageTag);
    br.close();

    Assert.assertNull(returnedHash);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromNullImageTagFails() throws IOException {
    String imageTag = null;
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromEmptyImageTagFails() throws IOException {
    String imageTag = "";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromInvalidImageTagFails() throws IOException {
    String imageTag = "hadoop/busybox:1.0.0:0";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromInvalidTagFails() throws IOException {
    String imageTag = "busybox:1,0,0";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    plugin.init(conf);
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromInvalidTag2Fails() throws IOException {
    String imageTag = "busybox:,,111";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    plugin.init(conf);
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromInvalidTag3Fails() throws IOException {
    String imageTag = "//busybox:,,111";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    plugin.init(conf);
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test (expected = IOException.class)
  public void testGetManifestFromInvalidTag4Fails() throws IOException {
    String imageTag = "default@1/busybox:,,111";
    ImageTagToManifestV2Plugin plugin = new ImageTagToManifestV2Plugin();
    plugin.init(conf);
    ImageManifest hash = plugin.getManifestFromImageTag(imageTag);
  }

  @Test
  public void testGetManifestFromImageTag() throws IOException {
    String manifestHash =
        "d0e8c542d28e8e868848aeb58beecb31079eb7ada1293c4bc2eded08daed605a";
    String manifestPath = tmpPath + "/manifest/" + manifestHash.substring(0, 2);
    File manifestDir = new File(manifestPath);
    manifestDir.mkdirs();

    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);

    PrintWriter printWriter =
        new PrintWriter(manifestPath + "/" + manifestHash);
    printWriter.println(manifestJson);
    printWriter.close();

    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    mockImageTagToManifestV2Plugin =
        new MockImageTagToManifestV2Plugin(mockHdfsBufferedReader) {
      @Override
      public String getHashFromImageTag(String imageTag) {
        return manifestHash;
      }
    };
    mockImageTagToManifestV2Plugin.init(conf);

    ImageManifest manifest =
        mockImageTagToManifestV2Plugin.getManifestFromImageTag("image");
    ImageManifest expectedManifest =
        mapper.readValue(manifestJson, ImageManifest.class);
    Assert.assertEquals(expectedManifest.toString(), manifest.toString());
  }

  @Test(expected = IOException.class)
  public void testGetManifestFromImageTagNullHashFailsWithException()
      throws IOException {
    String manifestPath = tmpPath + "/manifests";
    File manifestDir = new File(manifestPath);
    manifestDir.mkdirs();

    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);

    // Null manifest hash test
    PrintWriter printWriter = new PrintWriter(manifestPath + "/");
    printWriter.println(manifestJson);
    printWriter.close();

    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    mockImageTagToManifestV2Plugin =
      new TestImageTagToManifestV2Plugin.
          MockImageTagToManifestV2Plugin(mockHdfsBufferedReader) {
      @Override
      public String getHashFromImageTag(String imageTag) {
        return null;
      }
    };
    mockImageTagToManifestV2Plugin.init(conf);

    ImageManifest manifest =
        mockImageTagToManifestV2Plugin.getManifestFromImageTag("image");
    ImageManifest expectedManifest =
        mapper.readValue(manifestJson, ImageManifest.class);
    Assert.assertEquals(expectedManifest.toString(), manifest.toString());
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetManifestFromImageTagV1FailsWithException()
      throws IOException {
    String manifestPath = tmpPath + "/manifests";
    File manifestDir = new File(manifestPath);
    manifestDir.mkdirs();

    conf.set(NM_RUNC_IMAGE_TOPLEVEL_DIR, tmpPath);
    String manifestHash =
        "d0e8c542d28e8e868848aeb58beecb31079eb7ada1293c4bc2eded08daed605a";

    // V1 repository layout test
    PrintWriter printWriter =
        new PrintWriter(manifestPath + "/" + manifestHash);
    printWriter.println(manifestJson);
    printWriter.close();

    BufferedReader mockHdfsBufferedReader = mock(BufferedReader.class);

    mockImageTagToManifestV2Plugin =
      new TestImageTagToManifestV2Plugin.
          MockImageTagToManifestV2Plugin(mockHdfsBufferedReader) {
        @Override
        public String getHashFromImageTag(String imageTag) {
          return manifestHash;
        }
      };
    mockImageTagToManifestV2Plugin.init(conf);

    ImageManifest manifest =
        mockImageTagToManifestV2Plugin.getManifestFromImageTag("image");
    ImageManifest expectedManifest =
        mapper.readValue(manifestJson, ImageManifest.class);
    Assert.assertEquals(expectedManifest.toString(), manifest.toString());
  }
}
