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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;

import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public class TestHomeDirectoryAutoCreator {

  public void callApiGetFileStatus(UserGroupInformation ugi, MiniDFSCluster cluster)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = cluster.getFileSystem();
        fs.getFileStatus(new Path("/"));
        return null;
      }
    });
  }

  public void callApiListStatus(UserGroupInformation ugi, MiniDFSCluster cluster)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = cluster.getFileSystem();
        fs.listStatus(new Path("/"));
        return null;
      }
    });
  }

  public void callApiMkdirs(UserGroupInformation ugi, MiniDFSCluster cluster, Path p)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = cluster.getFileSystem();
        fs.mkdirs(p);
        return null;
      }
    });
  }

  public void callApiCreateFile(UserGroupInformation ugi, MiniDFSCluster cluster, Path p)
      throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = cluster.getFileSystem();
        try (FSDataOutputStream out = fs.create(p)){
          out.writeUTF("hello");
        }
        return null;
      }
    });
  }


  @Test
  public void testDisableMakeHomeDir() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user/"));

      assertFalse(dfs.exists(new Path("/user/foo")));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);
      assertFalse(dfs.exists(new Path("/user/foo")));

      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      assertNull(rpcServer.getHomeDirectoryAutoCreator().getCache());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnsureHomeDirForSuperUser() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      // make superuser home during mkdirs()
      dfs.mkdirs(new Path("/user"));
      dfs.setPermission(new Path("/user"), new FsPermission(0755));

      // superuser
      UserGroupInformation superuser = UserGroupInformation.getLoginUser();
      String superUserHome = "/user/" + superuser.getShortUserName();

      // check exists superuser home as non-superuser
      UserGroupInformation nonSuperUser =
          UserGroupInformation.createUserForTesting("foo", new String[]{"foo"});
      MiniDFSCluster finalCluster = cluster;
      nonSuperUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          FileSystem fs = finalCluster.getFileSystem();
          assertTrue(fs.exists(new Path(superUserHome)));
          return null;
        }
      });

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnsureHomeDir() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user"));
      dfs.setPermission(new Path("/user"), new FsPermission(0755));

      // check as superuser
      assertFalse(dfs.exists(new Path("/user/foo")));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);

      Path home = new Path("/user/foo");
      FileStatus status = dfs.getFileStatus(home);
      assertNotNull(status);
      if (status != null) {
        assertEquals("hello", status.getGroup());
        assertTrue(status.isDirectory());
      }

      QuotaUsage usage = dfs.getQuotaUsage(home);
      assertEquals(-1, usage.getQuota());
      assertEquals(-1, usage.getSpaceQuota());


      // check api to make home
      assertFalse(dfs.exists(new Path("/user/bar")));
      fakeUgi = UserGroupInformation.createUserForTesting("bar", new String[]{"users"});
      callApiListStatus(fakeUgi, cluster);
      assertTrue(dfs.exists(new Path("/user/bar")));

      assertFalse(dfs.exists(new Path("/user/baz")));
      fakeUgi = UserGroupInformation.createUserForTesting("baz", new String[]{"users"});
      callApiMkdirs(fakeUgi, cluster, new Path("/user/baz/dir"));
      assertTrue(dfs.exists(new Path("/user/baz")));
      assertTrue(dfs.exists(new Path("/user/baz/dir")));

      FileStatus xs = dfs.getFileStatus(new Path("/user/baz"));
      assertEquals(700, xs.getPermission().toOctal());
      assertFalse(dfs.exists(new Path("/user/foobar")));
      fakeUgi = UserGroupInformation.createUserForTesting("foobar", new String[]{"users"});
      callApiCreateFile(fakeUgi, cluster, new Path("/user/foobar/file.txt"));
      assertTrue(dfs.exists(new Path("/user/foobar")));
      assertTrue(dfs.exists(new Path("/user/foobar/file.txt")));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testEnsureHomeDirWithQuota() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    long spaceQuotaDefault = 0;
    String strSpaceQuotaDefault = "0";
    long nameQuotaDefault = 1;

    String groupA = "services";
    long spaceQuotaGroupA = 2L * 1024 * 1024 * 1024;
    String strSpaceQuotaGroupA = "2G";
    long nameQuotaGroupA = 500_000;

    String groupB = "users";
    long spaceQuotaGroupB = 1L * 1024 * 1024;
    String strSpaceQuotaGroupB = "1M";
    long nameQuotaGroupB = 100_000;

    String confQuota = String.format("%s:%s,%s:%s:%s,%s:%s:%s",
        nameQuotaDefault, strSpaceQuotaDefault,
        groupA, nameQuotaGroupA, strSpaceQuotaGroupA,
        groupB, nameQuotaGroupB, strSpaceQuotaGroupB);

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_QUOTA, confQuota);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user/"));

      assertFalse(dfs.exists(new Path("/user/foo")));
      assertFalse(dfs.exists(new Path("/user/bar")));
      assertFalse(dfs.exists(new Path("/user/baz")));
      UserGroupInformation foo =
          UserGroupInformation.createUserForTesting("foo", new String[]{"services"});
      UserGroupInformation bar =
          UserGroupInformation.createUserForTesting("bar", new String[]{"users"});
      UserGroupInformation baz =
          UserGroupInformation.createUserForTesting("baz", new String[]{"test-users"});

      callApiGetFileStatus(foo, cluster);
      callApiGetFileStatus(bar, cluster);
      callApiGetFileStatus(baz, cluster);


      // groupA: services
      Path home = new Path("/user/foo");
      FileStatus status = dfs.getFileStatus(home);
      assertNotNull(status);
      if (status != null) {
        assertEquals("services", status.getGroup());
        assertTrue(status.isDirectory());

        QuotaUsage usage = dfs.getQuotaUsage(home);
        assertEquals(nameQuotaGroupA, usage.getQuota());
        assertEquals(spaceQuotaGroupA, usage.getSpaceQuota());
      }

      // groupB: users
      home = new Path("/user/bar");
      status = dfs.getFileStatus(home);
      assertNotNull(status);
      if (status != null) {
        assertEquals("users", status.getGroup());
        assertTrue(status.isDirectory());

        QuotaUsage usage = dfs.getQuotaUsage(home);
        assertEquals(nameQuotaGroupB, usage.getQuota());
        assertEquals(spaceQuotaGroupB, usage.getSpaceQuota());
      }


      // default
      home = new Path("/user/baz");
      status = dfs.getFileStatus(home);
      assertNotNull(status);
      if (status != null) {
        assertEquals("test-users", status.getGroup());
        assertTrue(status.isDirectory());

        QuotaUsage usage = dfs.getQuotaUsage(home);
        assertEquals(nameQuotaDefault, usage.getQuota());
        assertEquals(spaceQuotaDefault, usage.getSpaceQuota());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnsureHomeDirWithDefaultGroupAndQuota()
      throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    long spaceQuota = 3L * 1024 * 1024 * 1024;
    String strSpaceQuota = "3G";
    long nameQuota = 1_000_000;
    String strNameQuota = String.valueOf(nameQuota);
    String defaultGroup = "world";

    String confQuota = String.format("%s:%s", strNameQuota, strSpaceQuota);
    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_QUOTA, confQuota);
    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_GROUP, defaultGroup);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user/"));

      assertFalse(dfs.exists(new Path("/user/foo")));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);

      Path home = new Path("/user/foo");
      FileStatus status = dfs.getFileStatus(home);
      assertNotNull(status);
      if (status != null) {
        assertEquals(defaultGroup, status.getGroup());
        assertTrue(status.isDirectory());
      }

      QuotaUsage usage = dfs.getQuotaUsage(home);
      assertEquals(nameQuota, usage.getQuota());
      assertEquals(spaceQuota, usage.getSpaceQuota());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnsureHomeDirWithPermission() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION, "0750");
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user/"));

      assertFalse(dfs.exists(new Path("/user/foo")));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);

      Path home = new Path("/user/foo");
      FileStatus status = dfs.getFileStatus(home);

      assertEquals("rwxr-x---", status.getPermission().toString());
      assertEquals(750, status.getPermission().toOctal());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFailedMakeHomeDirNoPrimaryGroup() throws IOException, InterruptedException {

    PrintStream originalOut = System.out;
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      // for capture log output
      System.setOut(new PrintStream(outContent));
      LogManager.resetConfiguration();
      BasicConfigurator.configure();

      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user"));
      dfs.setPermission(new Path("/user"), new FsPermission(0755));

      // check as superuser
      assertFalse(dfs.exists(new Path("/user/foo")));

      // no primary group
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{});
      callApiGetFileStatus(fakeUgi, cluster);
      Path home = new Path("/user/foo");
      assertFalse(dfs.exists(home));

      // retry with primary group
      fakeUgi = UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);
      // already caching. do not retry create home dir
      assertFalse(dfs.exists(home));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      // rollback log4j conf
      System.setOut(originalOut);
      LogManager.resetConfiguration();
      BasicConfigurator.configure();
    }

    String output = outContent.toString();
    System.out.println(output);
    assertTrue(output.contains("Failed creating home directory. " +
        "Will not attempt to create home directory in future. " +
        "/user/foo: java.io.IOException: There is no primary group for UGI foo"));
  }

  @Test
  public void testCacheWithTTL() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE, "100");
    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL, "2s");

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user"));
      dfs.setPermission(new Path("/user"), new FsPermission(0755));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);

      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      HomeDirectoryAutoCreator creator = rpcServer.getHomeDirectoryAutoCreator();
      Cache<String, Boolean> cache = creator.getCache();

      // superuser and foo
      assertEquals(2, cache.size());
      assertEquals(100, creator.getCacheMaxSize());
      assertEquals(2 * 1000, creator.getCacheTTLms());
      assertNotNull(cache.getIfPresent("foo"));
      Thread.sleep(1000);
      assertNotNull(cache.getIfPresent("foo"));
      Thread.sleep(1000);
      assertNull(cache.getIfPresent("foo"));

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCacheDisableTTL() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_ENABLED, true);
    conf.setInt(DFS_NAMENODE_HANDLER_COUNT_KEY, 1);

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_MAX_SIZE, "100");
    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_CACHE_TTL, "0");

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(new Path("/user"));
      dfs.setPermission(new Path("/user"), new FsPermission(0755));
      UserGroupInformation fakeUgi =
          UserGroupInformation.createUserForTesting("foo", new String[]{"hello"});
      callApiGetFileStatus(fakeUgi, cluster);

      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      HomeDirectoryAutoCreator creator = rpcServer.getHomeDirectoryAutoCreator();
      Cache<String, Boolean> cache = creator.getCache();

      // superuser and foo
      assertEquals(2, cache.size());
      assertEquals(100, creator.getCacheMaxSize());
      assertEquals(0, creator.getCacheTTLms());

      assertNotNull(cache.getIfPresent("foo"));
      Thread.sleep(2000);
      assertNotNull(cache.getIfPresent("foo"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testToSpaceQuotaString() throws IOException {

    long quota = 3L * 1024 * 1024 * 1024;
    String strQuota = HomeDirectoryAutoCreator.toSpaceQuotaString(quota);
    assertEquals("3 GB", strQuota);

    quota = 3L * 1024 * 1024 * 1024 * 1024;
    strQuota = HomeDirectoryAutoCreator.toSpaceQuotaString(quota);
    assertEquals("3 TB", strQuota);

    quota = 3L * 1024 * 1024 * 1024 * 1024 * 1024;
    strQuota = HomeDirectoryAutoCreator.toSpaceQuotaString(quota);
    assertEquals("3 PB", strQuota);

    quota = 0;
    strQuota = HomeDirectoryAutoCreator.toSpaceQuotaString(quota);
    assertEquals("0 B", strQuota);
  }

  @Test
  public void testToNameQuotaString() throws IOException {
    long quota = 1_000_000;
    String strQuota = HomeDirectoryAutoCreator.toNameQuotaString(quota);
    assertEquals("1,000,000", strQuota);

    quota = 0;
    strQuota = HomeDirectoryAutoCreator.toNameQuotaString(quota);
    assertEquals("0", strQuota);
  }

  @Test
  public void testParseQuota() {
    Map<String, HomeDirectoryAutoCreator.QuotaValue> map =
        HomeDirectoryAutoCreator.QuotaValue.parseQuota(
            "1:0,hadoop:none:none,users:1000:1M,hdfs:NONE:NONE");
    assertEquals(4, map.size());
    HomeDirectoryAutoCreator.QuotaValue quota = map.get("");
    assertEquals(1, quota.getNameQuota());
    assertEquals(0, quota.getSpaceQuota());

    quota = map.get("hadoop");
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quota.getNameQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quota.getSpaceQuota());

    quota = map.get("hdfs");
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quota.getNameQuota());
    assertEquals(HdfsConstants.QUOTA_DONT_SET, quota.getSpaceQuota());

    quota = map.get("users");
    assertEquals(1000, quota.getNameQuota());
    assertEquals(1 * 1024 * 1024, quota.getSpaceQuota());

    map = HomeDirectoryAutoCreator.QuotaValue.parseQuota(null);
    assertNotNull(map);
    assertEquals(0, map.size());
  }

  @Test
  public void testGetHomeDirPermission() throws IOException {

    Configuration conf = new Configuration(false);
    NameNode nn = Mockito.mock(NameNode.class);
    HomeDirectoryAutoCreator creator = new HomeDirectoryAutoCreator(conf, nn);
    FsPermission permission = creator.getHomeDirPermission(conf);
    assertEquals(700, permission.toOctal());

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION,
        "777");
    permission = creator.getHomeDirPermission(conf);
    assertEquals(777, permission.toOctal());

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION,
        "0755");
    permission = creator.getHomeDirPermission(conf);
    assertEquals(755, permission.toOctal());

    conf.set(HomeDirectoryAutoCreator.DFS_NAMENODE_AUTO_CREATE_USER_HOME_PERMISSION,
        "invalid_value");
    permission = creator.getHomeDirPermission(conf);
    assertEquals(700, permission.toOctal());
  }
}
