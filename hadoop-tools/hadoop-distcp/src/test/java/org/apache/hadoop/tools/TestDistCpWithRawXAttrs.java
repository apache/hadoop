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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.util.DistCpTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

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
    doTestPreserveRawXAttrs(rootedSrcName, rootedDestName, "-px",
        false, true, DistCpConstants.SUCCESS);
    doTestPreserveRawXAttrs(rootedSrcName, rawDestName, "-px",
        false, true, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rootedDestName, "-px",
        false, true, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rawDestName, "-px",
        true, true, DistCpConstants.SUCCESS);
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
    doTestPreserveRawXAttrs(rootedSrcName, rootedDestName, "-p",
        false, false, DistCpConstants.SUCCESS);
    doTestPreserveRawXAttrs(rootedSrcName, rawDestName, "-p",
        false, false, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rootedDestName, "-p",
        false, false, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rawDestName, "-p",
        true, false, DistCpConstants.SUCCESS);
  }

  /* Test that XAttrs are not preserved and raw.* are when appropriate. */
  @Test
  public void testPreserveRawXAttrs3() throws Exception {
    doTestPreserveRawXAttrs(rootedSrcName, rootedDestName, null,
        false, false, DistCpConstants.SUCCESS);
    doTestPreserveRawXAttrs(rootedSrcName, rawDestName, null,
        false, false, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rootedDestName, null,
        false, false, DistCpConstants.INVALID_ARGUMENT);
    doTestPreserveRawXAttrs(rawSrcName, rawDestName, null,
        true, false, DistCpConstants.SUCCESS);
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
}
