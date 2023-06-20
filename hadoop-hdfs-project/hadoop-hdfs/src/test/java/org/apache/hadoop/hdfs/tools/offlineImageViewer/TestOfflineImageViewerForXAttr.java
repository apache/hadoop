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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests OfflineImageViewer if the input fsimage has XAttributes
 */
public class TestOfflineImageViewerForXAttr {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestOfflineImageViewerForXAttr.class);

  private static File originalFsimage = null;

  static String attr1JSon;

  /**
   * Create a populated namespace for later testing. Save its contents to a data
   * structure and store its fsimage location. We only want to generate the
   * fsimage file once and use it for multiple tests.
   */
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      // Create a name space with XAttributes
      Path dir = new Path("/dir1");
      hdfs.mkdirs(dir);
      hdfs.setXAttr(dir, "user.attr1", "value1".getBytes());
      hdfs.setXAttr(dir, "user.attr2", "value2".getBytes());
      // Write results to the fsimage file
      hdfs.setSafeMode(SafeModeAction.ENTER, false);
      hdfs.saveNamespace();

      List<XAttr> attributes = new ArrayList<XAttr>();
      attributes.add(XAttrHelper.buildXAttr("user.attr1", "value1".getBytes()));

      attr1JSon = JsonUtil.toJsonString(attributes, null);

      attributes.add(XAttrHelper.buildXAttr("user.attr2", "value2".getBytes()));

      // Determine the location of the fsimage file
      originalFsimage = FSImageTestUtil.findLatestImageFile(FSImageTestUtil
          .getFSImage(cluster.getNameNode()).getStorage().getStorageDir(0));
      if (originalFsimage == null) {
        throw new RuntimeException("Didn't generate or can't find fsimage");
      }
      LOG.debug("original FS image file is " + originalFsimage);
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testWebImageViewerForListXAttrs() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=LISTXATTRS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());

      String content = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);

      assertTrue("Missing user.attr1 in response ",
          content.contains("user.attr1"));
      assertTrue("Missing user.attr2 in response ",
          content.contains("user.attr2"));

    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithOutParameters()
      throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);

      assertTrue("Missing user.attr1 in response ",
          content.contains("user.attr1"));
      assertTrue("Missing user.attr2 in response ",
          content.contains("user.attr2"));
    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithParameters() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {

      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=attr8");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
          connection.getResponseCode());

      url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=user.attr1");
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);
      assertEquals(attr1JSon, content);
    }
  }

  @Test
  public void testWebImageViewerForGetXAttrsWithCodecParameters()
      throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL(
          "http://localhost:"
              + port
              + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=USER.attr1&encoding=TEXT");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      String content = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);
      assertEquals(attr1JSon, content);

    }
  }

  @Test
  public void testWithWebHdfsFileSystem() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);

      List<String> names = webhdfs.listXAttrs(new Path("/dir1"));
      assertTrue(names.contains("user.attr1"));
      assertTrue(names.contains("user.attr2"));

      String value = new String(webhdfs.getXAttr(new Path("/dir1"),
          "user.attr1"));
      assertEquals("value1", value);

      value = new String(webhdfs.getXAttr(new Path("/dir1"), "USER.attr1"));
      assertEquals("value1", value);

      Map<String, byte[]> contentMap = webhdfs.getXAttrs(new Path("/dir1"),
          names);

      assertEquals("value1", new String(contentMap.get("user.attr1")));
      assertEquals("value2", new String(contentMap.get("user.attr2")));
    }
  }

  @Test
  public void testResponseCode() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      URL url = new URL(
          "http://localhost:"
              + port
              + "/webhdfs/v1/dir1/?op=GETXATTRS&xattr.name=user.notpresent&encoding=TEXT");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();

      assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
          connection.getResponseCode());

    }
  }
}
