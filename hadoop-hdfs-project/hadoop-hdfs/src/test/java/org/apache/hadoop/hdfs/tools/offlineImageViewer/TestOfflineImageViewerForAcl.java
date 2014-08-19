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

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests OfflineImageViewer if the input fsimage has HDFS ACLs
 */
public class TestOfflineImageViewerForAcl {

  private static final Log LOG =
      LogFactory.getLog(TestOfflineImageViewerForAcl.class);

  private static File originalFsimage = null;

  // ACLs as set to dfs, to be compared with viewer's output
  final static HashMap<String, AclStatus> writtenAcls = Maps.newHashMap();

  /**
   * Create a populated namespace for later testing. Save its contents to a
   * data structure and store its fsimage location.
   * We only want to generate the fsimage file once and use it for
   * multiple tests.
   */
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      // Create a reasonable namespace with ACLs
      Path dir = new Path("/dirWithNoAcl");
      hdfs.mkdirs(dir);
      writtenAcls.put(dir.toString(), hdfs.getAclStatus(dir));

      dir = new Path("/dirWithDefaultAcl");
      hdfs.mkdirs(dir);
      hdfs.setAcl(dir, Lists.newArrayList(
          aclEntry(DEFAULT, USER, ALL),
          aclEntry(DEFAULT, USER, "foo", ALL),
          aclEntry(DEFAULT, GROUP, READ_EXECUTE),
          aclEntry(DEFAULT, OTHER, NONE)));
      writtenAcls.put(dir.toString(), hdfs.getAclStatus(dir));

      Path file = new Path("/noAcl");
      FSDataOutputStream o = hdfs.create(file);
      o.write(23);
      o.close();
      writtenAcls.put(file.toString(), hdfs.getAclStatus(file));

      file = new Path("/withAcl");
      o = hdfs.create(file);
      o.write(23);
      o.close();
      hdfs.setAcl(file, Lists.newArrayList(
          aclEntry(ACCESS, USER, READ_WRITE),
          aclEntry(ACCESS, USER, "foo", READ),
          aclEntry(ACCESS, GROUP, READ),
          aclEntry(ACCESS, OTHER, NONE)));
      writtenAcls.put(file.toString(), hdfs.getAclStatus(file));

      file = new Path("/withSeveralAcls");
      o = hdfs.create(file);
      o.write(23);
      o.close();
      hdfs.setAcl(file, Lists.newArrayList(
          aclEntry(ACCESS, USER, READ_WRITE),
          aclEntry(ACCESS, USER, "foo", READ_WRITE),
          aclEntry(ACCESS, USER, "bar", READ),
          aclEntry(ACCESS, GROUP, READ),
          aclEntry(ACCESS, GROUP, "group", READ),
          aclEntry(ACCESS, OTHER, NONE)));
      writtenAcls.put(file.toString(), hdfs.getAclStatus(file));

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
  public static void deleteOriginalFSImage() throws IOException {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testWebImageViewerForAcl() throws IOException,
      InterruptedException, URISyntaxException {
    WebImageViewer viewer = new WebImageViewer(
        NetUtils.createSocketAddr("localhost:0"));
    try {
      viewer.initServer(originalFsimage.getAbsolutePath());
      int port = viewer.getPort();

      // create a WebHdfsFileSystem instance
      URI uri = new URI("webhdfs://localhost:" + String.valueOf(port));
      Configuration conf = new Configuration();
      WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.get(uri, conf);

      // GETACLSTATUS operation to a directory without ACL
      AclStatus acl = webhdfs.getAclStatus(new Path("/dirWithNoAcl"));
      assertEquals(writtenAcls.get("/dirWithNoAcl"), acl);

      // GETACLSTATUS operation to a directory with a default ACL
      acl = webhdfs.getAclStatus(new Path("/dirWithDefaultAcl"));
      assertEquals(writtenAcls.get("/dirWithDefaultAcl"), acl);

      // GETACLSTATUS operation to a file without ACL
      acl = webhdfs.getAclStatus(new Path("/noAcl"));
      assertEquals(writtenAcls.get("/noAcl"), acl);

      // GETACLSTATUS operation to a file with a ACL
      acl = webhdfs.getAclStatus(new Path("/withAcl"));
      assertEquals(writtenAcls.get("/withAcl"), acl);

      // GETACLSTATUS operation to a file with several ACL entries
      acl = webhdfs.getAclStatus(new Path("/withSeveralAcls"));
      assertEquals(writtenAcls.get("/withSeveralAcls"), acl);

      // GETACLSTATUS operation to a invalid path
      URL url = new URL("http://localhost:" + port +
          "/webhdfs/v1/invalid/?op=GETACLSTATUS");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.connect();
      assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
          connection.getResponseCode());
    } finally {
      // shutdown the viewer
      viewer.shutdown();
    }
  }
}
