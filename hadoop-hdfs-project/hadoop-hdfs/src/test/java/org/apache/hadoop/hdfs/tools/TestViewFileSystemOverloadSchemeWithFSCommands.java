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
package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme;
import org.apache.hadoop.fs.viewfs.ViewFsTestSetup;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * Tests HDFS commands with ViewFileSystemOverloadScheme with configured mount
 * links.
 */
public class TestViewFileSystemOverloadSchemeWithFSCommands {
  private static final String FS_IMPL_PATTERN_KEY = "fs.%s.impl";
  private static final String HDFS_SCHEME = "hdfs";
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private URI defaultFSURI;
  private File localTargetDir;
  private static final String TEST_ROOT_DIR = PathUtils
      .getTestDirName(TestViewFileSystemOverloadSchemeWithFSCommands.class);
  private static final String HDFS_USER_FOLDER = "/HDFSUser";
  private static final String LOCAL_FOLDER = "/local";
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  /**
   * Sets up the configurations and starts the MiniDFSCluster.
   */
  @Before
  public void startCluster() throws IOException {
    conf = new Configuration();
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFileSystemOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
        HDFS_SCHEME), DistributedFileSystem.class.getName());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitClusterUp();
    defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    localTargetDir = new File(TEST_ROOT_DIR, "/root/");
    Assert.assertEquals(HDFS_SCHEME, defaultFSURI.getScheme()); // hdfs scheme.
  }

  @After
  public void tearDown() throws IOException {
    try {
      System.out.flush();
      System.err.flush();
    } finally {
      System.setOut(OLD_OUT);
      System.setErr(OLD_ERR);
    }
    if (cluster != null) {
      FileSystem.closeAll();
      cluster.shutdown();
    }
    resetStream();
  }

  private void redirectStream() {
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  private void resetStream() {
    out.reset();
    err.reset();
  }

  private static void scanIntoList(final ByteArrayOutputStream baos,
      final List<String> list) {
    final Scanner scanner = new Scanner(baos.toString());
    while (scanner.hasNextLine()) {
      list.add(scanner.nextLine());
    }
    scanner.close();
  }

  /**
   * Adds the given mount links to config. sources contains mount link src and
   * the respective index location in targets contains the target uri.
   */
  void addMountLinks(String mountTable, String[] sources, String[] targets,
      Configuration config) throws IOException, URISyntaxException {
    ViewFsTestSetup.addMountLinksToConf(mountTable, sources, targets, config);
  }

  /**
   * Tests DF with ViewFSOverloadScheme.
   */
  @Test
  public void testDFWithViewFsOverloadScheme() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    List<String> mounts = Lists.newArrayList();
    mounts.add(HDFS_USER_FOLDER);
    mounts.add(LOCAL_FOLDER);
    addMountLinks(defaultFSURI.getHost(),
        mounts.toArray(new String[mounts.size()]),
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    FsShell fsShell = new FsShell(conf);
    try {
      redirectStream();
      int ret =
          ToolRunner.run(fsShell, new String[] {"-fs", defaultFSURI.toString(),
              "-df", "-h", defaultFSURI.toString() + "/" });
      assertEquals(0, ret);
      final List<String> errList = Lists.newArrayList();
      scanIntoList(out, errList);
      assertEquals(3, errList.size());
      for (int i = 1; i < errList.size(); i++) {
        String[] lineSplits = errList.get(i).split("\\s+");
        String mount = lineSplits[lineSplits.length - 1];
        mounts.remove(mount);
      }
      String msg =
          "DF was not calculated on all mounts. The left out mounts are: "
              + mounts;
      assertEquals(msg, 0, mounts.size());
    } finally {
      fsShell.close();
    }
  }
}