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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.ProxyServers;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Level;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.NNTOP_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/**
 * Tests for the {@link AuditLogger} custom audit logging interface.
 */
public class TestAuditLogger {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestAuditLogger.class);
  static {
    GenericTestUtils.setLogLevel(LOG, Level.ALL);
  }

  private static final short TEST_PERMISSION = (short) 0654;

  @Before
  public void setup() {
    DummyAuditLogger.initialized = false;
    DummyAuditLogger.logCount = 0;
    DummyAuditLogger.remoteAddr = null;

    Configuration conf = new HdfsConfiguration();
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);    
  }

  /**
   * Tests that AuditLogger works as expected.
   */
  @Test
  public void testAuditLogger() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();
      assertTrue(DummyAuditLogger.initialized);
      DummyAuditLogger.resetLogCount();

      FileSystem fs = cluster.getFileSystem();
      long time = System.currentTimeMillis();
      fs.setTimes(new Path("/"), time, time);
      assertEquals(1, DummyAuditLogger.logCount);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Tests that TopAuditLogger can be disabled
   */
  @Test
  public void testDisableTopAuditLogger() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(NNTOP_ENABLED_KEY, false);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();
      List<AuditLogger> auditLoggers =
          cluster.getNameNode().getNamesystem().getAuditLoggers();
      for (AuditLogger auditLogger : auditLoggers) {
        assertFalse(
            "top audit logger is still hooked in after it is disabled",
            auditLogger instanceof TopAuditLogger);
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWebHdfsAuditLogger() throws IOException, URISyntaxException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    GetOpParam.Op op = GetOpParam.Op.GETFILESTATUS;
    try {
      cluster.waitClusterUp();
      assertTrue(DummyAuditLogger.initialized);      
      URI uri = new URI(
          "http",
          NetUtils.getHostPortString(cluster.getNameNode().getHttpAddress()),
          "/webhdfs/v1/", op.toQueryString(), null);
      
      // non-proxy request
      HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.connect();
      assertEquals(200, conn.getResponseCode());
      conn.disconnect();
      assertEquals("getfileinfo", DummyAuditLogger.lastCommand);
      DummyAuditLogger.resetLogCount();
      assertEquals("127.0.0.1", DummyAuditLogger.remoteAddr);
      
      // non-trusted proxied request
      conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setRequestProperty("X-Forwarded-For", "1.1.1.1");
      conn.connect();
      assertEquals(200, conn.getResponseCode());
      conn.disconnect();
      assertEquals("getfileinfo", DummyAuditLogger.lastCommand);
      assertTrue(DummyAuditLogger.logCount == 1);
      DummyAuditLogger.resetLogCount();
      assertEquals("127.0.0.1", DummyAuditLogger.remoteAddr);
      
      // trusted proxied request
      conf.set(ProxyServers.CONF_HADOOP_PROXYSERVERS, "127.0.0.1");
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      conn = (HttpURLConnection) uri.toURL().openConnection();
      conn.setRequestMethod(op.getType().toString());
      conn.setRequestProperty("X-Forwarded-For", "1.1.1.1");
      conn.connect();
      assertEquals(200, conn.getResponseCode());
      conn.disconnect();
      assertEquals("getfileinfo", DummyAuditLogger.lastCommand);
      assertTrue(DummyAuditLogger.logCount == 1);
      assertEquals("1.1.1.1", DummyAuditLogger.remoteAddr);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Minor test related to HADOOP-9155. Verify that during a
   * FileSystem.setPermission() operation, the stat passed in during the
   * logAuditEvent() call returns the new permission rather than the old
   * permission.
   */
  @Test
  public void testAuditLoggerWithSetPermission() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();
      assertTrue(DummyAuditLogger.initialized);
      DummyAuditLogger.resetLogCount();

      FileSystem fs = cluster.getFileSystem();
      long time = System.currentTimeMillis();
      final Path p = new Path("/");
      fs.setTimes(p, time, time);
      fs.setPermission(p, new FsPermission(TEST_PERMISSION));
      assertEquals(TEST_PERMISSION, DummyAuditLogger.foundPermission);
      assertEquals(2, DummyAuditLogger.logCount);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Verify that the audit logger is aware of the call context
   */
  @Test
  public void testAuditLoggerWithCallContext() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HADOOP_CALLER_CONTEXT_ENABLED_KEY, true);
    conf.setInt(HADOOP_CALLER_CONTEXT_MAX_SIZE_KEY, 128);
    conf.setInt(HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_KEY, 40);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    LogCapturer auditlog = LogCapturer.captureLogs(FSNamesystem.auditLog);

    try {
      cluster.waitClusterUp();
      final FileSystem fs = cluster.getFileSystem();
      final long time = System.currentTimeMillis();
      final Path p = new Path("/");

      assertNull(CallerContext.getCurrent());

      // context-only
      CallerContext context = new CallerContext.Builder("setTimes").build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.setTimes(p, time, time);
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setTimes%n")));
      auditlog.clearOutput();

      // context with signature
      context = new CallerContext.Builder("setTimes")
          .setSignature("L".getBytes(CallerContext.SIGNATURE_ENCODING))
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.setTimes(p, time, time);
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setTimes:L%n")));
      auditlog.clearOutput();

      // long context is truncated
      final String longContext = StringUtils.repeat("foo", 100);
      context = new CallerContext.Builder(longContext)
          .setSignature("L".getBytes(CallerContext.SIGNATURE_ENCODING))
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.setTimes(p, time, time);
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=%s:L%n", longContext.substring(0, 128))));
      auditlog.clearOutput();

      // empty context is ignored
      context = new CallerContext.Builder("")
          .setSignature("L".getBytes(CallerContext.SIGNATURE_ENCODING))
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set empty caller context");
      fs.setTimes(p, time, time);
      assertFalse(auditlog.getOutput().contains("callerContext="));
      auditlog.clearOutput();

      // caller context is inherited in child thread
      context = new CallerContext.Builder("setTimes")
          .setSignature("L".getBytes(CallerContext.SIGNATURE_ENCODING))
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      Thread child = new Thread(new Runnable()
      {
        @Override
        public void run() {
          try {
            fs.setTimes(p, time, time);
          } catch (IOException e) {
            fail("Unexpected exception found." + e);
          }
        }
      });
      child.start();
      try {
        child.join();
      } catch (InterruptedException ignored) {
        // Ignore
      }
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setTimes:L%n")));
      auditlog.clearOutput();

      // caller context is overridden in child thread
      final CallerContext childContext =
          new CallerContext.Builder("setPermission")
              .setSignature("L".getBytes(CallerContext.SIGNATURE_ENCODING))
              .build();
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      child = new Thread(new Runnable()
      {
        @Override
        public void run() {
          try {
            CallerContext.setCurrent(childContext);
            fs.setPermission(p, new FsPermission((short)777));
          } catch (IOException e) {
            fail("Unexpected exception found." + e);
          }
        }
      });
      child.start();
      try {
        child.join();
      } catch (InterruptedException ignored) {
        // Ignore
      }
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setPermission:L%n")));
      auditlog.clearOutput();

      // reuse the current context's signature
       context = new CallerContext.Builder("mkdirs")
           .setSignature(CallerContext.getCurrent().getSignature()).build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.mkdirs(new Path("/reuse-context-signature"));
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=mkdirs:L%n")));
      auditlog.clearOutput();

      // too long signature is ignored
      context = new CallerContext.Builder("setTimes")
          .setSignature(new byte[41])
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.setTimes(p, time, time);
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setTimes%n")));
      auditlog.clearOutput();

      // null signature is ignored
      context = new CallerContext.Builder("setTimes").setSignature(null)
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.setTimes(p, time, time);
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=setTimes%n")));
      auditlog.clearOutput();

      // empty signature is ignored
      context = new CallerContext.Builder("mkdirs")
          .setSignature("".getBytes(CallerContext.SIGNATURE_ENCODING))
          .build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.mkdirs(new Path("/empty-signature"));
      assertTrue(auditlog.getOutput().endsWith(
          String.format("callerContext=mkdirs%n")));
      auditlog.clearOutput();

      // invalid context is not passed to the rpc
      context = new CallerContext.Builder(null).build();
      CallerContext.setCurrent(context);
      LOG.info("Set current caller context as {}", CallerContext.getCurrent());
      fs.mkdirs(new Path("/empty-signature"));
      assertFalse(auditlog.getOutput().contains("callerContext="));
      auditlog.clearOutput();

    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testAuditLogWithAclFailure() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitClusterUp();
      final FSDirectory dir = cluster.getNamesystem().getFSDirectory();

      final FSDirectory mockedDir = Mockito.spy(dir);
      AccessControlException ex = new AccessControlException();
      doThrow(ex).when(mockedDir).checkTraverse(any(), any(), any());
      cluster.getNamesystem().setFSDirectory(mockedDir);
      assertTrue(DummyAuditLogger.initialized);
      DummyAuditLogger.resetLogCount();

      final FileSystem fs = cluster.getFileSystem();
      final Path p = new Path("/");
      final List<AclEntry> acls = Lists.newArrayList();

      try {
        fs.getAclStatus(p);
      } catch (AccessControlException ignored) {}

      try {
        fs.setAcl(p, acls);
      } catch (AccessControlException ignored) {}

      try {
        fs.removeAcl(p);
      } catch (AccessControlException ignored) {}

      try {
        fs.removeDefaultAcl(p);
      } catch (AccessControlException ignored) {}

      try {
        fs.removeAclEntries(p, acls);
      } catch (AccessControlException ignored) {}

      try {
        fs.modifyAclEntries(p, acls);
      } catch (AccessControlException ignored) {}

      assertEquals(6, DummyAuditLogger.logCount);
      assertEquals(6, DummyAuditLogger.unsuccessfulCount);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Verify Audit log entries for the successful ACL API calls and ACL commands
   * over FS Shell.
   */
  @Test (timeout = 60000)
  public void testAuditLogForAcls() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        DummyAuditLogger.class.getName());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitClusterUp();
      assertTrue(DummyAuditLogger.initialized);

      final FileSystem fs = cluster.getFileSystem();
      final Path p = new Path("/debug.log");
      DFSTestUtil.createFile(fs, p, 1024, (short)1, 0L);

      DummyAuditLogger.resetLogCount();
      fs.getAclStatus(p);
      assertEquals(1, DummyAuditLogger.logCount);

      // FS shell command '-getfacl' additionally calls getFileInfo() and then
      // followed by getAclStatus() only if the ACL bit is set. Since the
      // initial permission didn't have the ACL bit set, getAclStatus() is
      // skipped.
      DFSTestUtil.FsShellRun("-getfacl " + p.toUri().getPath(), 0, null, conf);
      assertEquals(2, DummyAuditLogger.logCount);

      final List<AclEntry> acls = Lists.newArrayList();
      acls.add(AclTestHelpers.aclEntry(ACCESS, USER, ALL));
      acls.add(AclTestHelpers.aclEntry(ACCESS, USER, "user1", ALL));
      acls.add(AclTestHelpers.aclEntry(ACCESS, GROUP, READ_EXECUTE));
      acls.add(AclTestHelpers.aclEntry(ACCESS, OTHER, EXECUTE));

      fs.setAcl(p, acls);
      assertEquals(3, DummyAuditLogger.logCount);

      // Since the file has ACL bit set, FS shell command '-getfacl' should now
      // call getAclStatus() additionally after getFileInfo().
      DFSTestUtil.FsShellRun("-getfacl " + p.toUri().getPath(), 0, null, conf);
      assertEquals(5, DummyAuditLogger.logCount);

      fs.removeAcl(p);
      assertEquals(6, DummyAuditLogger.logCount);

      List<AclEntry> aclsToRemove = Lists.newArrayList();
      aclsToRemove.add(AclTestHelpers.aclEntry(DEFAULT, USER, "user1", ALL));
      fs.removeAclEntries(p, aclsToRemove);
      fs.removeDefaultAcl(p);
      assertEquals(8, DummyAuditLogger.logCount);

      // User ACL has been removed, FS shell command '-getfacl' should now
      // skip call to getAclStatus() after getFileInfo().
      DFSTestUtil.FsShellRun("-getfacl " + p.toUri().getPath(), 0, null, conf);
      assertEquals(9, DummyAuditLogger.logCount);
      assertEquals(0, DummyAuditLogger.unsuccessfulCount);
    } finally {
      cluster.shutdown();
    }
  }


  /**
   * Tests that a broken audit logger causes requests to fail.
   */
  @Test
  public void testBrokenLogger() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        BrokenAuditLogger.class.getName());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      cluster.waitClusterUp();

      FileSystem fs = cluster.getFileSystem();
      long time = System.currentTimeMillis();
      fs.setTimes(new Path("/"), time, time);
      fail("Expected exception due to broken audit logger.");
    } catch (RemoteException re) {
      // Expected.
    } finally {
      cluster.shutdown();
    }
  }

  public static class DummyAuditLogger implements AuditLogger {

    static boolean initialized;
    static int logCount;
    static int unsuccessfulCount;
    static short foundPermission;
    static String remoteAddr;
    private static String lastCommand;
    
    public void initialize(Configuration conf) {
      initialized = true;
    }

    public static void resetLogCount() {
      logCount = 0;
      unsuccessfulCount = 0;
    }

    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus stat) {
      remoteAddr = addr.getHostAddress();
      logCount++;
      if (!succeeded) {
        unsuccessfulCount++;
      }
      lastCommand = cmd;
      if (stat != null) {
        foundPermission = stat.getPermission().toShort();
      }
    }

    public static String getLastCommand() {
      return lastCommand;
    }

  }

  public static class BrokenAuditLogger implements AuditLogger {

    public void initialize(Configuration conf) {
      // No op.
    }

    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus stat) {
      if (!cmd.equals("datanodeReport")) {
        throw new RuntimeException("uh oh");
      }
    }

  }

}
