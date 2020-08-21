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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BatchOperations;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BatchOpsException;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBatchRename {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem dfs;
  private static WebHdfsFileSystem webHdfs;
  private Path root = new Path("/test/batchrename/");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    dfs = cluster.getFileSystem();

    webHdfs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
        WebHdfsConstants.WEBHDFS_SCHEME);
  }

  @AfterClass
  public static void afterClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testhasPathCapability() throws Exception {
    assertTrue("DistributedFileSystem should has batch rename capbility",
        dfs.hasPathCapability(root, "fs.capability.batch.rename"));
  }

  private List<String> generateBatchFiles(
      int totalNum, int createNum, final Path dir, String tag)
      throws IOException {
    List<String> files = new ArrayList<>();
    for (int i = 0; i < totalNum; i++) {
      Path p = new Path(dir, tag + "_" + i);
      if (createNum-- > 0) {
        DFSTestUtil.createFile(dfs, p, 10, (short) 1, 0);
        assertTrue(dfs.exists(p));
      } else {
        assertFalse(dfs.exists(p));
      }
      files.add(p.toString());
    }
    return files;
  }

  private void testBatchRename(BatchOperations batchFS) throws Exception {
    Path testDir = new Path(root, "testBatchRename");
    assertTrue(dfs.mkdirs(testDir));

    List<String> srcs = generateBatchFiles(
        2, 2, testDir, "src");
    List<String> dsts =generateBatchFiles(
        2, 0, testDir, "dst");

    batchFS.batchRename(
        srcs.toArray(new String[srcs.size()]),
        dsts.toArray(new String[dsts.size()]));

    for (String f : srcs) {
      assertFalse(dfs.exists(new Path(f)));
    }
    for (String f : dsts) {
      assertTrue(dfs.exists(new Path(f)));
      dfs.delete(new Path(f), true);
    }
  }

  @Test
  public void testBatchRaname() throws Exception {
    testBatchRename(dfs);
    testBatchRename(webHdfs);
  }

  private void testInvalidInput(BatchOperations batchFS) throws Exception {
    List<String> srcs = new ArrayList<>();
    srcs.add("/testInvalidInput_Mismatch");
    List<String> dsts = new ArrayList<>();
    LambdaTestUtils.intercept(InvalidPathException.class,
        "mismatch batch path",
        () -> batchFS.batchRename(
            srcs.toArray(new String[srcs.size()]),
            dsts.toArray(new String[dsts.size()])));
  }

  @Test
  public void testInvalidInput() throws Exception {
    testInvalidInput(dfs);
    testInvalidInput(webHdfs);
  }

   // rename /src_1:/src_2(not existing) to /dst_1:/dst_2
  private void testPartialSuccess1(BatchOperations batchFS) throws Exception {
    Path testDir = new Path(root, "partial_success");
    assertTrue(dfs.mkdirs(testDir));

    List<String> srcs =  generateBatchFiles(
        2, 1, testDir, "src");
    List<String> dsts = generateBatchFiles(
        2, 0, testDir, "dst");
    try {
      batchFS.batchRename(
          srcs.toArray(new String[srcs.size()]),
          dsts.toArray(new String[dsts.size()]));
    } catch (BatchOpsException e) {
      long index = e.getIndex();
      assertEquals(1, index);
      long total = e.getTotal();
      assertEquals(2, total);

      String reason = e.getReason();
      assertTrue(reason.contains("FileNotFoundException"));

      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        assertFalse(dfs.exists(p));
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        assertTrue(dfs.exists(p));
        dfs.delete(p, true);
      }
    }
  }

   // rename src_1:src_1/subdir to /dst_1:/dst_2
  private void testPartialSuccess2(BatchOperations batchFS) throws Exception {
    Path testDir = new Path(root, "partial_success");
    List<String> srcs = new ArrayList<>();
    Path src1 = new Path(testDir, "src_1");
    assertTrue(dfs.mkdirs(src1));
    srcs.add(src1.toString());
    Path src1Subdir = new Path(src1, "subdir");
    assertTrue(dfs.mkdirs(src1Subdir));
    srcs.add(src1Subdir.toString());

    List<String> dsts = generateBatchFiles(
        2, 0, testDir, "dst");
    try {
      batchFS.batchRename(
          srcs.toArray(new String[srcs.size()]),
          dsts.toArray(new String[dsts.size()]));
    } catch (BatchOpsException e) {
      long index = e.getIndex();
      assertEquals(1, index);
      long total = e.getTotal();
      assertEquals(2, total);
      String reason = e.getReason();
      assertTrue(reason.contains("FileNotFoundException"));
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        assertFalse(dfs.exists(p));
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        assertTrue(dfs.exists(p));
        dfs.delete(p, true);
      }
    }
  }

  // rename src_1:src_2 /dst_1:/dst_1
  private void testPartialSuccess3(BatchOperations batchFS) throws Exception {
    Path testDir = new Path(root, "partial_success_3");
    List<String> srcs =  generateBatchFiles(
        2, 2, testDir, "src");
    List<String> dsts = generateBatchFiles(
        1, 0, testDir, "dst");
    dsts.add(dsts.get(0));

    try {
      batchFS.batchRename(
          srcs.toArray(new String[srcs.size()]),
          dsts.toArray(new String[dsts.size()]));
    } catch (BatchOpsException e) {
      long index = e.getIndex();
      assertEquals(1, index);
      long total = e.getTotal();
      assertEquals(2, total);
      String reason = e.getReason();
      assertTrue(reason.contains("FileAlreadyExistsException"));
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        assertFalse(dfs.exists(p));
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        assertTrue(dfs.exists(p));
        dfs.delete(p, true);
      }
    }
  }

  @Test
  public void testPartialSuccess() throws Exception {
    testPartialSuccess1(dfs);
    testPartialSuccess1(webHdfs);
    testPartialSuccess2(dfs);
    testPartialSuccess2(webHdfs);
    testPartialSuccess3(dfs);
    testPartialSuccess3(webHdfs);
  }

  @Test
  public void testPathMustBeAbsoluteForWebFS() throws Exception {
    List<String> srcs = new ArrayList<>();
    srcs.add("hdfs://namenodelong startTime = System.currentTimeMillis();/f1");
    List<String> dsts = new ArrayList<>();
    dsts.add("hdfs://namenode/f2");
    LambdaTestUtils.intercept(InvalidPathException.class,
        "Path is not absolute",
        () -> webHdfs.batchRename(
            srcs.toArray(new String[srcs.size()]),
            dsts.toArray(new String[dsts.size()])));
  }
}
