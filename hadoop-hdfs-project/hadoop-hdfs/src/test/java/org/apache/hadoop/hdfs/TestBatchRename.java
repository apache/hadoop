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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.FutureRenameBuilder;
import org.apache.hadoop.fs.IORenameStatistic;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.apache.hadoop.hdfs.protocol.BatchRenameException;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBatchRename {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem dfs;
  private static FileSystem fs;
  private static WebHdfsFileSystem webHdfs;
  private Path root = new Path("/test/batchrename/");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    dfs = cluster.getFileSystem();
    fs = cluster.getFileSystem();;

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
        ContractTestUtils.assertPathExists(dfs,"Source file not created", p);
      } else {
        ContractTestUtils.assertPathsDoNotExist(dfs,
            "Destination file is existing", p);
      }
      files.add(p.toString());
    }
    return files;
  }

  private void testBatchRename(FileSystem doFS) throws Exception {
    Path testDir = new Path(root, "testBatchRename");
    dfs.mkdirs(testDir);

    List<String> srcs = generateBatchFiles(
        2, 2, testDir, "src");
    List<String> dsts =generateBatchFiles(
        2, 0, testDir, "dst");

    doFS.batchRename(srcs, dsts);

    for (String f : srcs) {
      ContractTestUtils.assertPathsDoNotExist(dfs,
          "Source file not renamed", new Path(f));
    }
    for (String f : dsts) {
      assertTrue(dfs.exists(new Path(f)));
      ContractTestUtils.assertPathExists(dfs,
          "Destination file not created", new Path(f));
      dfs.delete(new Path(f), true);
    }
  }

  @Test
  public void testBatchRaname() throws Exception {
    testBatchRename(dfs);
    testBatchRename(webHdfs);
  }

  private void testInvalidInput(FileSystem doFS) throws Exception {
    List<String> srcs = new ArrayList<>();
    srcs.add("/testInvalidInput_Mismatch");
    List<String> dsts = new ArrayList<>();
    LambdaTestUtils.intercept(InvalidPathException.class,
        "mismatch batch path",
        () -> doFS.batchRename(srcs, dsts));
  }

  @Test
  public void testInvalidInput() throws Exception {
    testInvalidInput(dfs);
    testInvalidInput(webHdfs);
  }

   // rename /src_1:/src_2(not existing) to /dst_1:/dst_2
  private void testPartialSuccess1(FileSystem doFS) throws Exception {
    Path testDir = new Path(root, "partial_success");
    dfs.mkdirs(testDir);

    List<String> srcs =  generateBatchFiles(
        2, 1, testDir, "src");
    List<String> dsts = generateBatchFiles(
        2, 0, testDir, "dst");
    try {
      doFS.batchRename(srcs, dsts);
    } catch (BatchRenameException e) {
      long index = e.getIndex();
      assertEquals("Partial success number mismatch!", 1, index);
      long total = e.getTotal();
      assertEquals("Total number mismatch!", 2, total);

      String reason = e.getReason();
      assertTrue("Error message mismatch",
          reason.contains("FileNotFoundException"));

      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        ContractTestUtils.assertPathsDoNotExist(dfs,
            "Source file not renamed", p);
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        ContractTestUtils.assertPathExists(dfs,
            "Destination file not created", p);
        dfs.delete(p, true);
      }
    }
  }

   // rename src_1:src_1/subdir to /dst_1:/dst_2
  private void testPartialSuccess2(FileSystem dofs) throws Exception {
    Path testDir = new Path(root, "partial_success");
    List<String> srcs = new ArrayList<>();
    Path src1 = new Path(testDir, "src_1");
    dfs.mkdirs(src1);
    srcs.add(src1.toString());
    Path src1Subdir = new Path(src1, "subdir");
    dfs.mkdirs(src1Subdir);
    srcs.add(src1Subdir.toString());

    List<String> dsts = generateBatchFiles(
        2, 0, testDir, "dst");
    try {
      dofs.batchRename(srcs, dsts);
    } catch (BatchRenameException e) {
      long index = e.getIndex();
      assertEquals("Partial success number mismatch!", 1, index);
      long total = e.getTotal();
      assertEquals("Total number mismatch!", 2, total);
      String reason = e.getReason();
      assertTrue("Error message mismatch",
          reason.contains("FileNotFoundException"));
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        ContractTestUtils.assertPathsDoNotExist(dfs,
            "Source file not renamed", p);
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        ContractTestUtils.assertPathExists(dfs,
            "Destination file not created", p);
        dfs.delete(p, true);
      }
    }
  }

  // rename src_1:src_2 /dst_1:/dst_1
  private void testPartialSuccess3(FileSystem doFS) throws Exception {
    Path testDir = new Path(root, "partial_success_3");
    List<String> srcs =  generateBatchFiles(
        2, 2, testDir, "src");
    List<String> dsts = generateBatchFiles(
        1, 0, testDir, "dst");
    dsts.add(dsts.get(0));

    try {
      doFS.batchRename(srcs, dsts);
    } catch (BatchRenameException e) {
      long index = e.getIndex();
      assertEquals("Partial success number mismatch!", 1, index);
      long total = e.getTotal();
      assertEquals("Total number mismatch!", 2, total);
      String reason = e.getReason();
      assertTrue("Error message mismatch!",
          reason.contains("FileAlreadyExistsException"));
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "src_" + i);
        ContractTestUtils.assertPathsDoNotExist(dfs,
            "Source file not renamed", p);
      }
      for (int i = 0; i < index; i++) {
        Path p = new Path(testDir, "dst_" + i);
        ContractTestUtils.assertPathExists(dfs,
            "Destination file not created", p);
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
        "Error message mismatch when rename invalid path o WebFS!",
        () -> webHdfs.batchRename(
            srcs, dsts));
  }

  @Test
  public void testOpenBuild() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    Path p = new Path("/f1");
    FutureDataInputStreamBuilder builder =
        fs.openFile(p);
    interceptFuture(FileNotFoundException.class,
        "", builder.build());
  }

  @Test
  public void testRenameByBuilder() throws Exception {
    Path testDir = new Path(root, "testBatchRename");
    dfs.mkdirs(testDir);

    List<String> srcs = generateBatchFiles(
        2, 2, testDir, "src");
    List<String> dsts =generateBatchFiles(
        2, 0, testDir, "dst");

    FutureRenameBuilder builder = fs.renameFile(srcs, dsts);
    IORenameStatistic ret = FutureIOSupport.awaitFuture(builder.build());

    for (String f : srcs) {
      ContractTestUtils.assertPathsDoNotExist(dfs,
          "Source file not renamed", new Path(f));
    }
    for (String f : dsts) {
      assertTrue(dfs.exists(new Path(f)));
      ContractTestUtils.assertPathExists(dfs,
          "Destination file not created", new Path(f));
      dfs.delete(new Path(f), true);
    }
  }
}
