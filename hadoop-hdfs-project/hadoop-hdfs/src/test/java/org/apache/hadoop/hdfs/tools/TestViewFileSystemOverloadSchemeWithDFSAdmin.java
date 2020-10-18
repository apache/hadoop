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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
 * Tests DFSAdmin with ViewFileSystemOverloadScheme with configured mount links.
 */
public class TestViewFileSystemOverloadSchemeWithDFSAdmin {
  private static final String FS_IMPL_PATTERN_KEY = "fs.%s.impl";
  private static final String HDFS_SCHEME = "hdfs";
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private URI defaultFSURI;
  private File localTargetDir;
  private static final String TEST_ROOT_DIR = PathUtils
      .getTestDirName(TestViewFileSystemOverloadSchemeWithDFSAdmin.class);
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

  private void assertErrMsg(String errorMsg, int line) {
    final List<String> errList = Lists.newArrayList();
    scanIntoList(err, errList);
    assertThat(errList.get(line), containsString(errorMsg));
  }

  private void assertOutMsg(String outMsg, int line) {
    final List<String> errList = Lists.newArrayList();
    scanIntoList(out, errList);
    assertThat(errList.get(line), containsString(outMsg));
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
   * Tests savenamespace with ViewFSOverloadScheme by specifying -fs option.
   */
  @Test
  public void testSaveNameSpace() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    int ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", defaultFSURI.toString(), "-safemode", "enter" });
    assertEquals(0, ret);
    redirectStream();
    ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", defaultFSURI.toString(), "-saveNamespace" });
    assertEquals(0, ret);
    assertOutMsg("Save namespace successful", 0);
    ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", defaultFSURI.toString(), "-safemode", "leave" });
    assertEquals(0, ret);

  }

  /**
   * Tests savenamespace with ViewFSOverloadScheme, but without -fs option.
   */
  @Test
  public void testSaveNamespaceWithoutSpecifyingFS() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    int ret = ToolRunner.run(dfsAdmin, new String[] {"-safemode", "enter" });
    assertEquals(0, ret);
    redirectStream();
    ret = ToolRunner.run(dfsAdmin, new String[] {"-saveNamespace" });
    assertOutMsg("Save namespace successful", 0);
    assertEquals(0, ret);
    ret = ToolRunner.run(dfsAdmin, new String[] {"-safemode", "leave" });
    assertEquals(0, ret);
  }

  /**
   * Tests safemode with ViewFSOverloadScheme, but with wrong target fs.
   */
  @Test
  public void testSafeModeWithWrongFS() throws Exception {
    final Path hdfsTargetPath =
        new Path("hdfs://nonExistent" + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getHost(), new String[] {HDFS_USER_FOLDER},
        new String[] {hdfsTargetPath.toUri().toString()}, conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    redirectStream();
    int ret = ToolRunner.run(dfsAdmin, new String[] {"-safemode", "enter" });
    assertEquals(-1, ret);
    assertErrMsg("safemode: java.net.UnknownHostException: nonExistent", 0);
  }

  /**
   * Tests safemode with ViewFSOverloadScheme, but -fs option with local fs.
   */
  @Test
  public void testSafeModeShouldFailOnLocalTargetFS() throws Exception {
    addMountLinks(defaultFSURI.getHost(), new String[] {LOCAL_FOLDER },
        new String[] {localTargetDir.toURI().toString() }, conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    // ViewFSOveloadScheme uri with localfs mount point
    String uri = defaultFSURI.toString() + LOCAL_FOLDER;
    redirectStream();
    int ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", uri, "-safemode", "enter" });
    assertEquals(-1, ret);
    assertErrMsg("safemode: FileSystem file:/// is not an HDFS file system."
        + " The fs class is: org.apache.hadoop.fs.LocalFileSystem", 0);
  }

  /**
   * Tests safemode get with ViewFSOverloadScheme, but without any mount links
   * configured. The ViewFSOverloadScheme should consider initialized fs as
   * fallback fs automatically.
   */
  @Test
  public void testGetSafemodeWithoutMountLinksConfigured() throws Exception {
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    try {
      redirectStream();
      int ret = ToolRunner.run(dfsAdmin,
          new String[] {"-fs", defaultFSURI.toString(), "-safemode", "get"});
      assertOutMsg("Safe mode is OFF", 0);
      assertEquals(0, ret);
    } finally {
      dfsAdmin.close();
    }
  }

  /**
   * Tests allowSnapshot and disallowSnapshot with ViewFSOverloadScheme.
   */
  @Test
  public void testAllowAndDisalllowSnapShot() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER},
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    redirectStream();
    int ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", defaultFSURI.toString(), "-allowSnapshot", "/" });
    assertOutMsg("Allowing snapshot on / succeeded", 0);
    assertEquals(0, ret);
    ret = ToolRunner.run(dfsAdmin, new String[] {"-fs",
        defaultFSURI.toString(), "-disallowSnapshot", "/" });
    assertOutMsg("Disallowing snapshot on / succeeded", 1);
    assertEquals(0, ret);
  }

  /**
   * Tests setBalancerBandwidth with ViewFSOverloadScheme.
   */
  @Test
  public void testSetBalancerBandwidth() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    final DFSAdmin dfsAdmin = new DFSAdmin(conf);
    redirectStream();
    int ret = ToolRunner.run(dfsAdmin,
        new String[] {"-fs", defaultFSURI.toString(), "-setBalancerBandwidth",
            "1000"});
    assertOutMsg("Balancer bandwidth is set to 1000", 0);
    assertEquals(0, ret);
  }
}