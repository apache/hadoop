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
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests GETCONTENTSUMMARY operation for WebImageViewer
 */
public class TestOfflineImageViewerForContentSummary {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestOfflineImageViewerForContentSummary.class);

  private static File originalFsimage = null;
  private static ContentSummary summaryFromDFS = null;
  private static ContentSummary emptyDirSummaryFromDFS = null;
  private static ContentSummary fileSummaryFromDFS = null;
  private static ContentSummary symLinkSummaryFromDFS = null;
  private static ContentSummary symLinkSummaryForDirContainsFromDFS=null;
  /**
   * Create a populated namespace for later testing. Save its contents to a
   * data structure and store its fsimage location. We only want to generate
   * the fsimage file once and use it for multiple tests.
   */
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      Path parentDir = new Path("/parentDir");
      Path childDir1 = new Path(parentDir, "childDir1");
      Path childDir2 = new Path(parentDir, "childDir2");
      Path dirForLinks = new Path("/dirForLinks");
      hdfs.mkdirs(parentDir);
      hdfs.mkdirs(childDir1);
      hdfs.mkdirs(childDir2);
      hdfs.mkdirs(dirForLinks);
      hdfs.setQuota(parentDir, 10, 1024*1024*1024);

      Path file1OnParentDir = new Path(parentDir, "file1");
      try (FSDataOutputStream o = hdfs.create(file1OnParentDir)) {
        o.write("123".getBytes());
      }
      try (FSDataOutputStream o = hdfs.create(new Path(parentDir, "file2"))) {
        o.write("1234".getBytes());
      }
      try (FSDataOutputStream o = hdfs.create(new Path(childDir1, "file3"))) {
        o.write("123".getBytes());
      }
      try (FSDataOutputStream o = hdfs.create(new Path(parentDir, "file4"))) {
        o.write("123".getBytes());
      }
      Path link1 = new Path("/link1");
      Path link2 = new Path("/dirForLinks/linkfordir1");
      hdfs.createSymlink(new Path("/parentDir/file4"), link1, true);
      summaryFromDFS = hdfs.getContentSummary(parentDir);
      emptyDirSummaryFromDFS = hdfs.getContentSummary(childDir2);
      fileSummaryFromDFS = hdfs.getContentSummary(file1OnParentDir);
      symLinkSummaryFromDFS = hdfs.getContentSummary(link1);
      hdfs.createSymlink(childDir1, link2, true);
      symLinkSummaryForDirContainsFromDFS = hdfs.getContentSummary(new Path(
          "/dirForLinks"));
      // Write results to the fsimage file
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false);
      hdfs.saveNamespace();
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
  public static void deleteOriginalFSImage() {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testGetContentSummaryForEmptyDirectory() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/parentDir/childDir2?op=GETCONTENTSUMMARY");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);
      ContentSummary summary = webfs.getContentSummary(new Path(
          "/parentDir/childDir2"));
      verifyContentSummary(emptyDirSummaryFromDFS, summary);
    }
  }

  @Test
  public void testGetContentSummaryForDirectory() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/parentDir/?op=GETCONTENTSUMMARY");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);
      ContentSummary summary = webfs.getContentSummary(new Path("/parentDir/"));
      verifyContentSummary(summaryFromDFS, summary);
    }
  }

  @Test
  public void testGetContentSummaryForFile() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/parentDir/file1?op=GETCONTENTSUMMARY");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);
      ContentSummary summary = webfs.
          getContentSummary(new Path("/parentDir/file1"));
      verifyContentSummary(fileSummaryFromDFS, summary);
    }
  }

  @Test
  public void testGetContentSummaryForSymlink() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);
      ContentSummary summary = webfs.getContentSummary(new Path("/link1"));
      verifyContentSummary(symLinkSummaryFromDFS, summary);
    }
  }

  @Test
  public void testGetContentSummaryForDirContainsSymlink() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webfs = (WebHdfsFileSystem) FileSystem.get(uri, conf);
      ContentSummary summary = webfs.getContentSummary(new Path(
          "/dirForLinks/"));
      verifyContentSummary(symLinkSummaryForDirContainsFromDFS, summary);
    }
  }

  private void verifyContentSummary(ContentSummary expected,
      ContentSummary actual) {
    assertEquals(expected.getDirectoryCount(), actual.getDirectoryCount());
    assertEquals(expected.getFileCount(), actual.getFileCount());
    assertEquals(expected.getLength(), actual.getLength());
    assertEquals(expected.getSpaceConsumed(), actual.getSpaceConsumed());
    assertEquals(expected.getQuota(), actual.getQuota());
    assertEquals(expected.getSpaceQuota(), actual.getSpaceQuota());
  }

  @Test
  public void testGetContentSummaryResponseCode() throws Exception {
    try (WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"))) {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();
      URL url = new URL("http://localhost:" + port
          + "/webhdfs/v1/dir123/?op=GETCONTENTSUMMARY");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          connection.getResponseCode());
    }
  }
}
