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

package org.apache.hadoop.tools;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.ECAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.util.DistCpTestUtils;

import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests distcp in combination with HDFS raw.* XAttrs.
 */
public class TestDistCpWithRawXAttrs {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;

  private static final String rawName1 = "raw.a1";
  private static final byte[] rawValue1 = {0x37, 0x38, 0x39};
  private static final String userName1 = "user.a1";
  private static final byte[] userValue1 = {0x38, 0x38, 0x38};

  private static final Path dir1 = new Path("/src/dir1");
  private static final Path subDir1 = new Path(dir1, "subdir1");
  private static final Path file1 = new Path("/src/file1");
  private static final String rawRootName = "/.reserved/raw";
  private static final String rootedDestName = "/dest";
  private static final String rootedSrcName = "/src";
  private static final String rawDestName = "/.reserved/raw/dest";
  private static final String rawSrcName = "/.reserved/raw/src";

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true)
            .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /* Test that XAttrs and raw.* XAttrs are preserved when appropriate. */
  @Test
  public void testPreserveRawXAttrs1() throws Exception {
    final String relSrc = "/./.reserved/../.reserved/raw/../raw/src/../src";
    final String relDst = "/./.reserved/../.reserved/raw/../raw/dest/../dest";
    doTestPreserveRawXAttrs(relSrc, relDst, "-px", true, true,
        DistCpConstants.SUCCESS);
    doTestStandardPreserveRawXAttrs("-px", true);
    final Path savedWd = fs.getWorkingDirectory();
    try {
      fs.setWorkingDirectory(new Path("/.reserved/raw"));
      doTestPreserveRawXAttrs("../.." + rawSrcName, "../.." + rawDestName,
              "-px", true, true, DistCpConstants.SUCCESS);
    } finally {
      fs.setWorkingDirectory(savedWd);
    }
  }

  /* Test that XAttrs are not preserved and raw.* are when appropriate. */
  @Test
  public void testPreserveRawXAttrs2() throws Exception {
    doTestStandardPreserveRawXAttrs("-p", false);
  }

  /* Test that XAttrs are not preserved and raw.* are when appropriate. */
  @Test
  public void testPreserveRawXAttrs3() throws Exception {
    doTestStandardPreserveRawXAttrs(null, false);
  }

  @Test
  public void testPreserveRawXAttrs4() throws Exception {
    doTestStandardPreserveRawXAttrs("-update -delete", false);
  }

  private static Path[] pathnames = { new Path("dir1"),
                                      new Path("dir1/subdir1"),
                                      new Path("file1") };

  private static void makeFilesAndDirs(FileSystem fs) throws Exception {
    fs.delete(new Path("/src"), true);
    fs.delete(new Path("/dest"), true);
    fs.mkdirs(subDir1);
    fs.create(file1).close();
  }

  private void initXAttrs() throws Exception {
    makeFilesAndDirs(fs);
    for (Path p : pathnames) {
      fs.setXAttr(new Path(rawRootName + "/src", p), rawName1, rawValue1);
      fs.setXAttr(new Path(rawRootName + "/src", p), userName1, userValue1);
    }
  }

  private void doTestStandardPreserveRawXAttrs(String options,
      boolean expectUser)
      throws Exception {
    doTestPreserveRawXAttrs(rootedSrcName, rootedDestName, options,
        false, expectUser, DistCpConstants.SUCCESS);
    doTestPreserveRawXAttrs(rootedSrcName, rawDestName, options,
        false, expectUser, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rootedDestName, options,
        false, expectUser, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rawDestName, options,
        true, expectUser, DistCpConstants.SUCCESS);
  }

  private void doTestPreserveRawXAttrs(String src, String dest,
      String preserveOpts, boolean expectRaw, boolean expectUser,
      int expectedExitCode) throws Exception {
    initXAttrs();

    DistCpTestUtils.assertRunDistCp(expectedExitCode, src, dest,
        preserveOpts, conf);

    if (expectedExitCode == DistCpConstants.SUCCESS) {
      Map<String, byte[]> xAttrs = Maps.newHashMap();
      for (Path p : pathnames) {
        xAttrs.clear();
        if (expectRaw) {
          xAttrs.put(rawName1, rawValue1);
        }
        if (expectUser) {
          xAttrs.put(userName1, userValue1);
        }
        DistCpTestUtils.assertXAttrs(new Path(dest, p), fs, xAttrs);
      }
    }
  }

  @Test
  public void testPreserveEC() throws Exception {
    final String src = "/src";
    final String dest = "/dest";

    final Path destDir1 = new Path("/dest/dir1");
    final Path destSubDir1 = new Path(destDir1, "subdir1");

    String[] args = {"-setPolicy", "-path", dir1.toString(),
        "-policy", "XOR-2-1-1024k"};

    fs.delete(new Path("/dest"), true);
    fs.mkdirs(subDir1);
    fs.create(file1).close();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    dfs.enableErasureCodingPolicy("XOR-2-1-1024k");
    int res = ToolRunner.run(conf, new ECAdmin(conf), args);
    assertEquals("Unable to set EC policy on " + subDir1.toString(), res, 0);

    // preserve all attributes
    DistCpTestUtils.assertRunDistCp(DistCpConstants.SUCCESS, src, dest,
        "-pe", conf);

    FileStatus srcStatus = fs.getFileStatus(new Path(src));
    FileStatus srcDir1Status = fs.getFileStatus(dir1);
    FileStatus srcSubDir1Status = fs.getFileStatus(subDir1);

    FileStatus destStatus = fs.getFileStatus(new Path(dest));
    FileStatus destDir1Status = fs.getFileStatus(destDir1);
    FileStatus destSubDir1Status = fs.getFileStatus(destSubDir1);

    assertFalse("/src is erasure coded!",
        srcStatus.isErasureCoded());
    assertFalse("/dest is erasure coded!",
        destStatus.isErasureCoded());
    assertTrue("/src/dir1 is not erasure coded!",
        srcDir1Status.isErasureCoded());
    assertTrue("/dest/dir1 is not erasure coded!",
        destDir1Status.isErasureCoded());
    assertTrue("/src/dir1/subdir1 is not erasure coded!",
        srcSubDir1Status.isErasureCoded());
    assertTrue("/dest/dir1/subdir1 is not erasure coded!",
        destSubDir1Status.isErasureCoded());
  }
}
