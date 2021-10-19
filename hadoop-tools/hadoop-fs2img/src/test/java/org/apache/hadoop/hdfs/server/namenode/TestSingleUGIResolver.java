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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

/**
 * Validate resolver assigning all paths to a single owner/group.
 */
public class TestSingleUGIResolver {

  @Rule public TestName name = new TestName();

  private static final int TESTUID = 10101;
  private static final int TESTGID = 10102;
  private static final String TESTUSER = "tenaqvyybdhragqvatbf";
  private static final String TESTGROUP = "tnyybcvatlnxf";

  private SingleUGIResolver ugi = new SingleUGIResolver();

  @Before
  public void setup() {
    Configuration conf = new Configuration(false);
    conf.setInt(SingleUGIResolver.UID, TESTUID);
    conf.setInt(SingleUGIResolver.GID, TESTGID);
    conf.set(SingleUGIResolver.USER, TESTUSER);
    conf.set(SingleUGIResolver.GROUP, TESTGROUP);
    ugi.setConf(conf);
    System.out.println(name.getMethodName());
  }

  @Test
  public void testRewrite() {
    FsPermission p1 = new FsPermission((short)0755);
    match(ugi.resolve(file("dingo", "dingo", p1)), p1);
    match(ugi.resolve(file(TESTUSER, "dingo", p1)), p1);
    match(ugi.resolve(file("dingo", TESTGROUP, p1)), p1);
    match(ugi.resolve(file(TESTUSER, TESTGROUP, p1)), p1);

    FsPermission p2 = new FsPermission((short)0x8000);
    match(ugi.resolve(file("dingo", "dingo", p2)), p2);
    match(ugi.resolve(file(TESTUSER, "dingo", p2)), p2);
    match(ugi.resolve(file("dingo", TESTGROUP, p2)), p2);
    match(ugi.resolve(file(TESTUSER, TESTGROUP, p2)), p2);

    Map<Integer, String> ids = ugi.ugiMap();
    assertEquals(2, ids.size());
    assertEquals(TESTUSER, ids.get(10101));
    assertEquals(TESTGROUP, ids.get(10102));
  }

  @Test
  public void testDefault() {
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      user = "hadoop";
    }
    Configuration conf = new Configuration(false);
    ugi.setConf(conf);
    Map<Integer, String> ids = ugi.ugiMap();
    assertEquals(2, ids.size());
    assertEquals(user, ids.get(0));
    assertEquals(user, ids.get(1));
  }

  @Test
  public void testAclResolution() {
    long perm;

    FsPermission p1 = new FsPermission((short)0755);
    FileStatus fileStatus = file("dingo", "dingo", p1);
    perm = ugi.getPermissionsProto(fileStatus, null);
    match(perm, p1);

    AclEntry aclEntry = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.ALL)
        .setName("dingo")
        .build();

    AclStatus aclStatus = new AclStatus.Builder()
        .owner("dingo")
        .group(("dingo"))
        .addEntry(aclEntry)
        .setPermission(p1)
        .build();

    perm = ugi.getPermissionsProto(null, aclStatus);
    match(perm, p1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidUid() {
    Configuration conf = ugi.getConf();
    conf.setInt(SingleUGIResolver.UID, (1 << 24) + 1);
    ugi.setConf(conf);
    ugi.resolve(file(TESTUSER, TESTGROUP, new FsPermission((short)0777)));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidGid() {
    Configuration conf = ugi.getConf();
    conf.setInt(SingleUGIResolver.GID, (1 << 24) + 1);
    ugi.setConf(conf);
    ugi.resolve(file(TESTUSER, TESTGROUP, new FsPermission((short)0777)));
  }

  @Test(expected=IllegalStateException.class)
  public void testDuplicateIds() {
    Configuration conf = new Configuration(false);
    conf.setInt(SingleUGIResolver.UID, 4344);
    conf.setInt(SingleUGIResolver.GID, 4344);
    conf.set(SingleUGIResolver.USER, TESTUSER);
    conf.set(SingleUGIResolver.GROUP, TESTGROUP);
    ugi.setConf(conf);
    ugi.ugiMap();
  }

  static void match(long encoded, FsPermission p) {
    assertEquals(p, new FsPermission((short)(encoded & 0xFFFF)));
    long uid = (encoded >>> UGIResolver.USER_STRID_OFFSET);
    uid &= UGIResolver.USER_GROUP_STRID_MASK;
    assertEquals(TESTUID, uid);
    long gid = (encoded >>> UGIResolver.GROUP_STRID_OFFSET);
    gid &= UGIResolver.USER_GROUP_STRID_MASK;
    assertEquals(TESTGID, gid);
  }

  static FileStatus file(String user, String group, FsPermission perm) {
    Path p = new Path("foo://bar:4344/baz/dingo");
    return new FileStatus(
          4344 * (1 << 20),        /* long length,             */
          false,                   /* boolean isdir,           */
          1,                       /* int block_replication,   */
          256 * (1 << 20),         /* long blocksize,          */
          0L,                      /* long modification_time,  */
          0L,                      /* long access_time,        */
          perm,                    /* FsPermission permission, */
          user,                    /* String owner,            */
          group,                   /* String group,            */
          p);                      /* Path path                */
  }

}
