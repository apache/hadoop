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
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;

/**
 * Tests OfflineImageViewer if the input fsimage has HDFS ErasureCodingPolicy
 * entries.
 */
public class TestOfflineImageViewerForErasureCodingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOfflineImageViewerForErasureCodingPolicy.class);

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

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(10).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();

      hdfs.enableErasureCodingPolicy("RS-6-3-1024k");
      hdfs.enableErasureCodingPolicy("RS-3-2-1024k");

      Path dir = new Path("/dir_wo_ec_rs63");
      hdfs.mkdirs(dir);
      hdfs.setErasureCodingPolicy(dir, "RS-6-3-1024k");

      dir = new Path("/dir_wo_ec_rs63/sub_dir_1");
      hdfs.mkdirs(dir);

      dir = new Path("/dir_wo_ec_rs63/sub_dir_2");
      hdfs.mkdirs(dir);

      Path file = new Path("/dir_wo_ec_rs63/file_wo_ec_1");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
      }

      file = new Path("/dir_wo_ec_rs63/file_wo_ec_2");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
      }

      dir = new Path("/dir_wo_ec_rs32");
      hdfs.mkdirs(dir);
      hdfs.setErasureCodingPolicy(dir, "RS-3-2-1024k");

      dir = new Path("/dir_wo_ec_rs32/sub_dir_1");
      hdfs.mkdirs(dir);

      file = new Path("/dir_wo_ec_rs32/file_wo_ec");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
      }

      dir = new Path("/dir_wo_rep");
      hdfs.mkdirs(dir);

      dir = new Path("/dir_wo_rep/sub_dir_1");
      hdfs.mkdirs(dir);

      file = new Path("/dir_wo_rep/file_rep");
      try (FSDataOutputStream  o = hdfs.create(file)) {
        o.write(123);
      }

      // Write results to the fsimage file
      hdfs.setSafeMode(SafeModeAction.ENTER, false);
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
  public void testPBDelimitedWriterForErasureCodingPolicy() throws Exception {
    String expected = DFSTestUtil.readResoucePlainFile(
        "testErasureCodingPolicy.csv");
    String result = readECPolicyFromFsimageFile();
    assertEquals(expected, result);
  }

  private String readECPolicyFromFsimageFile() throws Exception {
    StringBuilder builder = new StringBuilder();
    String delemiter = "\t";

    File delimitedOutput = new File(tempDir, "delimitedOutput");

    if (OfflineImageViewerPB.run(new String[] {"-p", "Delimited",
        "-i", originalFsimage.getAbsolutePath(),
        "-o", delimitedOutput.getAbsolutePath(),
        "-ec"}) != 0) {
      throw new IOException("oiv returned failure creating " +
          "delimited output with ec.");
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
          String ecPolicy = fields[12];
          builder.append(path).append(",").append(ecPolicy).append("\n");
        }
        header = false;
      }
    }
    return builder.toString();
  }
}