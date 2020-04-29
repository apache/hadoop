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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ALLSSD_STORAGE_POLICY_NAME;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Tests OfflineImageViewer if the input fsimage has HDFS StoragePolicy entries.
 */
public class TestOfflineImageViewerForStoragePolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOfflineImageViewerForStoragePolicy.class);

  private static File originalFsimage = null;
  private static File tempDir;

  /**
   * Create a populated namespace for later testing. Save its contents to a
   * data structure and store its fsimage location.
   */
  @BeforeClass
  public static void createOriginalFSImage() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, true);

      File[] nnDirs = MiniDFSCluster.getNameNodeDirectory(
          MiniDFSCluster.getBaseDirectory(), 0, 0);
      tempDir = nnDirs[0];

      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      Path dir = new Path("/dir_wo_sp");
      hdfs.mkdirs(dir);

      dir = new Path("/dir_wo_sp/sub_dir_wo_sp");
      hdfs.mkdirs(dir);

      dir = new Path("/dir_wo_sp/sub_dir_w_sp_allssd");
      hdfs.mkdirs(dir);
      hdfs.setStoragePolicy(dir, ALLSSD_STORAGE_POLICY_NAME);

      Path file = new Path("/dir_wo_sp/file_wo_sp");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
        o.close();
      }

      file = new Path("/dir_wo_sp/file_w_sp_allssd");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
        o.close();
        hdfs.setStoragePolicy(file, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);
      }

      dir = new Path("/dir_w_sp_allssd");
      hdfs.mkdirs(dir);
      hdfs.setStoragePolicy(dir, HdfsConstants.ALLSSD_STORAGE_POLICY_NAME);

      dir = new Path("/dir_w_sp_allssd/sub_dir_wo_sp");
      hdfs.mkdirs(dir);

      file = new Path("/dir_w_sp_allssd/file_wo_sp");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
        o.close();
      }

      dir = new Path("/dir_w_sp_allssd/sub_dir_w_sp_hot");
      hdfs.mkdirs(dir);
      hdfs.setStoragePolicy(dir, HdfsConstants.HOT_STORAGE_POLICY_NAME);

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
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @AfterClass
  public static void deleteOriginalFSImage() throws IOException {
    if (originalFsimage != null && originalFsimage.exists()) {
      originalFsimage.delete();
    }
  }

  @Test
  public void testPBDelimitedWriterForStoragePolicy() throws Exception {
    String expected = DFSTestUtil.readResoucePlainFile(
        "testStoragePolicy.csv");
    String result = readStoragePolicyFromFsimageFile();
    assertEquals(expected, result);
  }

  private String readStoragePolicyFromFsimageFile() throws Exception {
    StringBuilder builder = new StringBuilder();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    String delemiter = "\t";

    File delimitedOutput = new File(tempDir, "delimitedOutput");

    if (OfflineImageViewerPB.run(new String[] {"-p", "Delimited",
        "-i", originalFsimage.getAbsolutePath(),
        "-o", delimitedOutput.getAbsolutePath(),
        "-sp"}) != 0) {
      throw new IOException("oiv returned failure creating " +
          "delimited output with sp.");
    }

    try (InputStream input = new FileInputStream(delimitedOutput);
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(input))) {
      String line;
      boolean header = true;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split(delemiter);
        if (!header) {
          String path = fields[0];
          int storagePolicy = Integer.parseInt(fields[12]);
          builder.append(path).append(",").append(storagePolicy).append("\n");
        }
        header = false;
      }
    }
    return builder.toString();
  }
}