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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFetchImage {
  
  private static final File FETCHED_IMAGE_FILE =
      GenericTestUtils.getTestDir("target/fetched-image-dir");
  // Shamelessly stolen from NNStorage.
  private static final Pattern IMAGE_REGEX = Pattern.compile("fsimage_(\\d+)");

  private MiniDFSCluster cluster;
  private NameNode nn0 = null;
  private NameNode nn1 = null;
  private Configuration conf = null;

  @BeforeClass
  public static void setupImageDir() {
    FETCHED_IMAGE_FILE.mkdirs();
  }

  @AfterClass
  public static void cleanup() {
    FileUtil.fullyDelete(FETCHED_IMAGE_FILE);
  }

  @Before
  public void setupCluster() throws IOException, URISyntaxException {
    conf = new Configuration();
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 1024);

    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(1)
        .build();
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    HATestUtil.configureFailoverFs(cluster, conf);
    cluster.waitActive();
  }

  /**
   * Download a few fsimages using `hdfs dfsadmin -fetchImage ...' and verify
   * the results.
   */
  @Test(timeout=30000)
  public void testFetchImageHA() throws Exception {
    final Path parent = new Path(
        PathUtils.getTestPath(getClass()),
        GenericTestUtils.getMethodName());

    int nnIndex = 0;
    /* run on nn0 as active */
    cluster.transitionToActive(nnIndex);
    testFetchImageInternal(
        nnIndex,
        new Path(parent, "dir1"),
        new Path(parent, "dir2"));

    /* run on nn1 as active */
    nnIndex = 1;
    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    cluster.transitionToActive(nnIndex);
    testFetchImageInternal(
        nnIndex,
        new Path(parent, "dir3"),
        new Path(parent, "dir4"));
  }

  private void testFetchImageInternal(
      final int nnIndex,
      final Path dir1,
      final Path dir2) throws Exception {
    final Configuration dfsConf = cluster.getConfiguration(nnIndex);
    final DFSAdmin dfsAdmin = new DFSAdmin();
    dfsAdmin.setConf(dfsConf);

    try (FileSystem fs = cluster.getFileSystem(nnIndex)) {
      runFetchImage(dfsAdmin, cluster);
      
      fs.mkdirs(dir1);
      fs.mkdirs(dir2);
      
      cluster.getNameNodeRpc(nnIndex).setSafeMode(
          SafeModeAction.SAFEMODE_ENTER,
          false);
      cluster.getNameNodeRpc(nnIndex).saveNamespace(0, 0);
      cluster.getNameNodeRpc(nnIndex).setSafeMode(
          SafeModeAction.SAFEMODE_LEAVE,
          false);
      
      runFetchImage(dfsAdmin, cluster);
    }
  }

  /**
   * Run `hdfs dfsadmin -fetchImage ...' and verify that the downloaded image is
   * correct.
   */
  private static void runFetchImage(DFSAdmin dfsAdmin, MiniDFSCluster cluster)
      throws Exception {
    int retVal = dfsAdmin.run(new String[]{"-fetchImage",
        FETCHED_IMAGE_FILE.getPath() });
    
    assertEquals(0, retVal);
    
    File highestImageOnNn = getHighestFsImageOnCluster(cluster);
    MD5Hash expected = MD5FileUtils.computeMd5ForFile(highestImageOnNn);
    MD5Hash actual = MD5FileUtils.computeMd5ForFile(
        new File(FETCHED_IMAGE_FILE, highestImageOnNn.getName()));
    
    assertEquals(expected, actual);
  }
  
  /**
   * @return the fsimage with highest transaction ID in the cluster.
   */
  private static File getHighestFsImageOnCluster(MiniDFSCluster cluster) {
    long highestImageTxId = -1;
    File highestImageOnNn = null;
    for (URI nameDir : cluster.getNameDirs(0)) {
      for (File imageFile : new File(new File(nameDir), "current").listFiles()) {
        Matcher imageMatch = IMAGE_REGEX.matcher(imageFile.getName());
        if (imageMatch.matches()) {
          long imageTxId = Long.parseLong(imageMatch.group(1));
          if (imageTxId > highestImageTxId) {
            highestImageTxId = imageTxId;
            highestImageOnNn = imageFile;
          }
        }
      }
    }
    return highestImageOnNn;
  }
}
