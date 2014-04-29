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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.PathUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A JUnit test that audit logs are generated
 */
@RunWith(Parameterized.class)
public class TestAuditLogs {
  static final String auditLogFile = PathUtils.getTestDirName(TestAuditLogs.class) + "/TestAuditLogs-audit.log";
  final boolean useAsyncLog;
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{new Boolean(false)});
    params.add(new Object[]{new Boolean(true)});
    return params;
  }
  
  public TestAuditLogs(boolean useAsyncLog) {
    this.useAsyncLog = useAsyncLog;
  }

  // Pattern for: 
  // allowed=(true|false) ugi=name ip=/address cmd={cmd} src={path} dst=null perm=null
  static final Pattern auditPattern = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=.*?\\ssrc=.*?\\sdst=null\\s" + 
      "perm=.*?");
  static final Pattern successPattern = Pattern.compile(
      ".*allowed=true.*");
  static final Pattern webOpenPattern = Pattern.compile(
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
    configureAuditLogs();
    conf = new HdfsConfiguration();
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY, useAsyncLog);
    util = new DFSTestUtil.Builder().setName("TestAuditAllowed").
        setNumFiles(20).build();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    fs = cluster.getFileSystem();
    util.createFiles(fs, fileName);

    // make sure the appender is what it's supposed to be
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    assertEquals(1, appenders.size());
    assertEquals(useAsyncLog, appenders.get(0) instanceof AsyncAppender);
    
    fnames = util.getFileNames(fileName);
    util.waitReplication(fs, fileName, (short)3);
    userGroupInfo = UserGroupInformation.createUserForTesting(username, groups);
 }

  @After
  public void teardownCluster() throws Exception {
    util.cleanup(fs, "/srcdat");
    fs.close();
    cluster.shutdown();
  }

  /** test that allowed operation puts proper entry in audit log */
  @Test
  public void testAuditAllowed() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    setupAuditLogs();
    InputStream istream = userfs.open(file);
    int val = istream.read();
    istream.close();
    verifyAuditLogs(true);
    assertTrue("failed to read from file", val >= 0);
  }

  /** test that allowed stat puts proper entry in audit log */
  @Test
  public void testAuditAllowedStat() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    setupAuditLogs();
    FileStatus st = userfs.getFileStatus(file);
    verifyAuditLogs(true);
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /** test that denied operation puts proper entry in audit log */
  @Test
  public void testAuditDenied() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    fs.setPermission(file, new FsPermission((short)0600));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    try {
      userfs.open(file);
      fail("open must not succeed");
    } catch(AccessControlException e) {
      System.out.println("got access denied, as expected.");
    }
    verifyAuditLogs(false);
  }

  /** test that access via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfs() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsFileSystem.SCHEME);
    InputStream istream = webfs.open(file);
    int val = istream.read();
    istream.close();

    verifyAuditLogsRepeat(true, 3);
    assertTrue("failed to read from file", val >= 0);
  }

  /** test that stat via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsStat() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsFileSystem.SCHEME);
    FileStatus st = webfs.getFileStatus(file);

    verifyAuditLogs(true);
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /** test that access via Hftp puts proper entry in audit log */
  @Test
  public void testAuditHftp() throws Exception {
    final Path file = new Path(fnames[0]);

    final String hftpUri =
      "hftp://" + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    HftpFileSystem hftpFs = null;

    setupAuditLogs();
    try {
      hftpFs = (HftpFileSystem) new Path(hftpUri).getFileSystem(conf);
      InputStream istream = hftpFs.open(file);
      @SuppressWarnings("unused")
      int val = istream.read();
      istream.close();

      verifyAuditLogs(true);
    } finally {
      if (hftpFs != null) hftpFs.close();
    }
  }

  /** test that denied access via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsDenied() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0600));
    fs.setOwner(file, "root", null);

    setupAuditLogs();
    try {
      WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsFileSystem.SCHEME);
      InputStream istream = webfs.open(file);
      int val = istream.read();
      fail("open+read must not succeed, got " + val);
    } catch(AccessControlException E) {
      System.out.println("got access denied, as expected.");
    }
    verifyAuditLogsRepeat(false, 2);
  }

  /** test that open via webhdfs puts proper entry in audit log */
  @Test
  public void testAuditWebHdfsOpen() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short)0644));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    WebHdfsFileSystem webfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf, WebHdfsFileSystem.SCHEME);
    webfs.open(file);

    verifyAuditLogsCheckPattern(true, 3, webOpenPattern);
  }

  /** Sets up log4j logger for auditlogs */
  private void setupAuditLogs() throws IOException {
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    // enable logging now that the test is ready to run
    logger.setLevel(Level.INFO);
  }
  
  private void configureAuditLogs() throws IOException {
    // Shutdown the LogManager to release all logger open file handles.
    // Unfortunately, Apache commons logging library does not provide
    // means to release underlying loggers. For additional info look up
    // commons library FAQ.
    LogManager.shutdown();

    File file = new File(auditLogFile);
    if (file.exists()) {
      assertTrue(file.delete());
    }
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    // disable logging while the cluster startup preps files
    logger.setLevel(Level.OFF);
    PatternLayout layout = new PatternLayout("%m%n");
    RollingFileAppender appender = new RollingFileAppender(layout, auditLogFile);
    logger.addAppender(appender);
  }

  // Ensure audit log has only one entry
  private void verifyAuditLogs(boolean expectSuccess) throws IOException {
    verifyAuditLogsRepeat(expectSuccess, 1);
  }

  // Ensure audit log has exactly N entries
  private void verifyAuditLogsRepeat(boolean expectSuccess, int ndupe)
      throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);

    // Close the appenders and force all logs to be flushed
    Enumeration<?> appenders = logger.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender appender = (Appender)appenders.nextElement();
      appender.close();
    }

    BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
    String line = null;
    boolean ret = true;

    try {
      for (int i = 0; i < ndupe; i++) {
        line = reader.readLine();
        assertNotNull(line);
        assertTrue("Expected audit event not found in audit log",
            auditPattern.matcher(line).matches());
        ret &= successPattern.matcher(line).matches();
      }
      assertNull("Unexpected event in audit log", reader.readLine());
      assertTrue("Expected success=" + expectSuccess, ret == expectSuccess);
    } finally {
      reader.close();
    }
  }

  // Ensure audit log has exactly N entries
  private void verifyAuditLogsCheckPattern(boolean expectSuccess, int ndupe, Pattern pattern)
      throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);

    // Close the appenders and force all logs to be flushed
    Enumeration<?> appenders = logger.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender appender = (Appender)appenders.nextElement();
      appender.close();
    }

    BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
    String line = null;
    boolean ret = true;
    boolean patternMatches = false;

    try {
        for (int i = 0; i < ndupe; i++) {
          line = reader.readLine();
          assertNotNull(line);
          patternMatches |= pattern.matcher(line).matches();
          ret &= successPattern.matcher(line).matches();
        }
        assertNull("Unexpected event in audit log", reader.readLine());
        assertTrue("Expected audit event not found in audit log", patternMatches);
        assertTrue("Expected success=" + expectSuccess, ret == expectSuccess);
      } finally {
        reader.close();
      }
  }
}
