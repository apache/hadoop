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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.junit.Assert.assertTrue;

public class TestFSNamesystemLockReport {

  @FunctionalInterface
  private interface SupplierWithException<T> {
    T get() throws Exception;
  }

  @FunctionalInterface
  private interface Procedure {
    void invoke() throws Exception;
  }

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private UserGroupInformation userGroupInfo;
  private GenericTestUtils.LogCapturer logs;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.set(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, "hadoop");

    // Make the lock report always shown
    conf.setLong(DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY, 0);
    conf.setLong(DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY, 0);
    conf.setLong(DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY, 0);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    fs = cluster.getFileSystem();

    userGroupInfo = UserGroupInformation.createUserForTesting("bob",
        new String[] {"hadoop"});

    logs = GenericTestUtils.LogCapturer.captureLogs(FSNamesystem.LOG);
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(FSNamesystem.class.getName()),
        org.slf4j.event.Level.INFO);
  }

  @After
  public void cleanUp() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void test() throws Exception {
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    // The log output should contain "by create (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=null,perm=bob:hadoop:rw-r--r--)"
    FSDataOutputStream os = testLockReport(() ->
        userfs.create(new Path("/file")),
        ".* by create \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=null," +
        "perm=bob:hadoop:rw-r--r--\\) .*");
    os.close();

    // The log output should contain "by open (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=null,perm=null)"
    FSDataInputStream is = testLockReport(() -> userfs.open(new Path("/file")),
        ".* by open \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=null," +
        "perm=null\\) .*");
    is.close();

    // The log output should contain "by setPermission (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=null,perm=bob:hadoop:-w----r-T)"
    testLockReport(() ->
        userfs.setPermission(new Path("/file"), new FsPermission(644)),
        ".* by setPermission \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=null," +
        "perm=bob:hadoop:-w----r-T\\) .*");

    // The log output should contain "by setOwner (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=null,perm=alice:group1:-w----r-T)"
    testLockReport(() -> userfs.setOwner(new Path("/file"), "alice", "group1"),
        ".* by setOwner \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=null," +
        "perm=alice:group1:-w----r-T\\) .*");

    // The log output should contain "by listStatus (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/,dst=null,perm=null)"
    testLockReport(() -> userfs.listStatus(new Path("/")),
        ".* by listStatus \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/,dst=null," +
        "perm=null\\) .*");

    // The log output should contain "by getfileinfo (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=null,perm=null)"
    testLockReport(() -> userfs.getFileStatus(new Path("/file")),
        ".* by getfileinfo \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=null," +
        "perm=null\\) .*");

    // The log output should contain "by mkdirs (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/dir,dst=null,perm=bob:hadoop:rwxr-xr-x)"
    testLockReport(() -> userfs.mkdirs(new Path("/dir")),
        ".* by mkdirs \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/dir,dst=null," +
        "perm=bob:hadoop:rwxr-xr-x\\) .*");

    // The log output should contain "by delete (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file2,dst=null,perm=null)"
    testLockReport(() -> userfs.rename(new Path("/file"), new Path("/file2")),
        ".* by rename \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file,dst=/file2," +
        "perm=alice:group1:-w----r-T\\) .*");

    // The log output should contain "by rename (ugi=bob (auth:SIMPLE),
    // ip=/127.0.0.1,src=/file,dst=/file2,perm=alice:group1:-w----r-T)"
    testLockReport(() -> userfs.delete(new Path("/file2"), false),
        ".* by delete \\(ugi=bob \\(auth:SIMPLE\\)," +
        "ip=[a-zA-Z0-9.]+/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3},src=/file2,dst=null," +
        "perm=null\\) .*");
  }

  private void testLockReport(Procedure procedure,
      String expectedLockReportRegex) throws Exception {
    logs.clearOutput();
    userGroupInfo.doAs((PrivilegedExceptionAction<Void>) () -> {
      procedure.invoke();
      return null;
    });
    assertTrue(matches(expectedLockReportRegex));
  }

  private <T> T testLockReport(SupplierWithException<T> supplier,
      String expectedLockReportRegex) throws Exception {
    logs.clearOutput();
    T ret = userGroupInfo.doAs((PrivilegedExceptionAction<T>) supplier::get);
    assertTrue(matches(expectedLockReportRegex));
    return ret;
  }

  private boolean matches(String regex) {
    // Check for each line
    for (String line : logs.getOutput().split(System.lineSeparator())) {
      if (line.matches(regex)) {
        return true;
      }
    }
    return false;
  }
}
