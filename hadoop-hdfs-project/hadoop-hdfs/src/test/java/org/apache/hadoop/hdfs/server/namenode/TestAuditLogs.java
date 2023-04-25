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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.LoggerFactory;

/**
 * A JUnit test that audit logs are generated
 */
@RunWith(Parameterized.class)
public class TestAuditLogs {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestAuditLogs.class);

  final boolean useAsyncEdits;

  private static LogCapturer auditLogCapture;

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<>();
    params.add(new Object[]{Boolean.FALSE});
    params.add(new Object[]{Boolean.TRUE});
    return params;
  }

  public TestAuditLogs(boolean useAsyncEdits) {
    this.useAsyncEdits = useAsyncEdits;
  }

  // Pattern for: 
  // allowed=(true|false) ugi=name ip=/address cmd={cmd} src={path} dst=null perm=null
  private static final Pattern AUDIT_PATTERN = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" +
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" +
      "cmd=.*?\\ssrc=.*?\\sdst=null\\s" +
      "perm=.*?");
  private static final Pattern SUCCESS_PATTERN = Pattern.compile(
      ".*allowed=true.*");
  private static final Pattern FAILURE_PATTERN = Pattern.compile(
      ".*allowed=false.*");
  private static final Pattern WEB_OPEN_PATTERN = Pattern.compile(
      ".*cmd=open.*proto=webhdfs.*");

  static final String username = "bob";
  static final String[] groups = { "group1" };
  static final String fileName = "/srcdat";

  DFSTestUtil util;
  MiniDFSCluster cluster;
  FileSystem fs;
  String fnames[];
  Configuration conf;
  UserGroupInformation userGroupInfo;

  @Before
  public void setupCluster() throws Exception {
    // must configure prior to instantiating the namesystem because it
    // will reconfigure the logger if async is enabled
    conf = new HdfsConfiguration();
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING, useAsyncEdits);
    util = new DFSTestUtil.Builder().setName("TestAuditAllowed").
        setNumFiles(20).build();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    fs = cluster.getFileSystem();
    util.createFiles(fs, fileName);

    // make sure the appender is what it's supposed to be
    Logger logger = org.apache.log4j.Logger.getLogger(
        "org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit");
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    assertTrue(appenders.get(0) instanceof AsyncAppender);
    
    fnames = util.getFileNames(fileName);
    util.waitReplication(fs, fileName, (short)3);
    userGroupInfo = UserGroupInformation.createUserForTesting(username, groups);
 }

  @After
  public void teardownCluster() throws Exception {
    util.cleanup(fs, "/srcdat");
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @BeforeClass
  public static void beforeClass() {
    auditLogCapture = LogCapturer.captureLogs(FSNamesystem.AUDIT_LOG);
  }

  @AfterClass
  public static void afterClass() {
    auditLogCapture.stopCapturing();
  }


  /** test that allowed operation puts proper entry in audit log */
  @Test
  public void testAuditAllowed() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    InputStream istream = userfs.open(file);
    int val = istream.read();
    istream.close();
    verifySuccessCommandsAuditLogs(2, fnames[0], "cmd=open");
    assertTrue("failed to read from file", val >= 0);
  }

  /** test that allowed stat puts proper entry in audit log */
  @Test
  public void testAuditAllowedStat() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    FileStatus st = userfs.getFileStatus(file);
    verifySuccessCommandsAuditLogs(2, fnames[0], "cmd=getfileinfo");
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /** test that denied operation puts proper entry in audit log */
  @Test
  public void testAuditDenied() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    fs.setPermission(file, new FsPermission((short)0600));
    fs.setOwner(file, "root", null);

    try {
      userfs.open(file);
      fail("open must not succeed");
    } catch(AccessControlException e) {
      System.out.println("got access denied, as expected.");
    }
    verifyFailedCommandsAuditLogs(1, fnames[0], "cmd=open");
  }

  /** test that access via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfs() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    InputStream istream = webfs.open(file);
    int val = istream.read();
    istream.close();

    verifySuccessCommandsAuditLogs(3, fnames[0], "cmd=open");
    assertTrue("failed to read from file", val >= 0);
  }

  /** test that stat via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsStat() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    FileStatus st = webfs.getFileStatus(file);

    verifySuccessCommandsAuditLogs(2, fnames[0], "cmd=getfileinfo");
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /** test that denied access via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsDenied() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0600));
    fs.setOwner(file, "root", null);

    try {
      WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
      InputStream istream = webfs.open(file);
      int val = istream.read();
      fail("open+read must not succeed, got " + val);
    } catch(AccessControlException E) {
      System.out.println("got access denied, as expected.");
    }
    verifyFailedCommandsAuditLogs(1, fnames[0], "cmd=open");
  }

  /** test that open via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsOpen() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsConstants.WEBHDFS_SCHEME);
    webfs.open(file).read();

    verifySuccessCommandsAuditLogs(3, fnames[0], "cmd=open");
  }

  /** make sure that "\r\n" isn't made into a newline in audit log */
  @Test
  public void testAuditCharacterEscape() throws Exception {
    final Path file = new Path("foo" + "\r\n" + "bar");
    fs.create(file);
    verifySuccessCommandsAuditLogs(1, "foo", "cmd=create");
  }

  private void verifySuccessCommandsAuditLogs(int leastExpected, String file, String cmd) {
    String[] auditLogOutputLines = auditLogCapture.getOutput().split("\\n");
    int success = 0;
    for (String auditLogLine : auditLogOutputLines) {
      if (!auditLogLine.contains("allowed=")) {
        continue;
      }
      String line = "allowed=" + auditLogLine.split("allowed=")[1];
      LOG.info("Line: {}", line);
      if (SUCCESS_PATTERN.matcher(line).matches() && line.contains(file) && line.contains(cmd)) {
        assertTrue("Expected audit event not found in audit log",
            AUDIT_PATTERN.matcher(line).matches());
        LOG.info("Successful verification. Log line: {}", line);
        success++;
      }
    }
    if (success < leastExpected) {
      throw new AssertionError(
          "Least expected: " + leastExpected + ". Actual success: " + success);
    }
  }

  private void verifyFailedCommandsAuditLogs(int expected, String file, String cmd) {
    String[] auditLogOutputLines = auditLogCapture.getOutput().split("\\n");
    int success = 0;
    for (String auditLogLine : auditLogOutputLines) {
      if (!auditLogLine.contains("allowed=")) {
        continue;
      }
      String line = "allowed=" + auditLogLine.split("allowed=")[1];
      LOG.info("Line: {}", line);
      if (FAILURE_PATTERN.matcher(line).matches() && line.contains(file) && line.contains(
          cmd)) {
        assertTrue("Expected audit event not found in audit log",
            AUDIT_PATTERN.matcher(line).matches());
        LOG.info("Failure verification. Log line: {}", line);
        success++;
      }
    }
    assertEquals("Expected: " + expected + ". Actual failure: " + success, expected,
        success);
  }

}
