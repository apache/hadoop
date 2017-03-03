/*
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
 *
 */

package org.apache.hadoop.fs.adl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

import com.squareup.okhttp.mockwebserver.MockResponse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Stub adl server and test acl data conversion within SDK and Hadoop adl
 * client.
 */
public class TestACLFeatures extends AdlMockWebServer {

  @Test(expected=AccessControlException.class)
  public void testModifyAclEntries() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    List<AclEntry> entries = new ArrayList<AclEntry>();
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder();
    aclEntryBuilder.setName("hadoop");
    aclEntryBuilder.setType(AclEntryType.USER);
    aclEntryBuilder.setPermission(FsAction.ALL);
    aclEntryBuilder.setScope(AclEntryScope.ACCESS);
    entries.add(aclEntryBuilder.build());

    aclEntryBuilder.setName("hdfs");
    aclEntryBuilder.setType(AclEntryType.GROUP);
    aclEntryBuilder.setPermission(FsAction.READ_WRITE);
    aclEntryBuilder.setScope(AclEntryScope.DEFAULT);
    entries.add(aclEntryBuilder.build());

    getMockAdlFileSystem().modifyAclEntries(new Path("/test1/test2"), entries);

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .modifyAclEntries(new Path("/test1/test2"), entries);
  }

  @Test(expected=AccessControlException.class)
  public void testRemoveAclEntriesWithOnlyUsers()
      throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    List<AclEntry> entries = new ArrayList<AclEntry>();
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder();
    aclEntryBuilder.setName("hadoop");
    aclEntryBuilder.setType(AclEntryType.USER);
    entries.add(aclEntryBuilder.build());

    getMockAdlFileSystem().removeAclEntries(new Path("/test1/test2"), entries);

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .removeAclEntries(new Path("/test1/test2"), entries);
  }

  @Test(expected=AccessControlException.class)
  public void testRemoveAclEntries() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    List<AclEntry> entries = new ArrayList<AclEntry>();
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder();
    aclEntryBuilder.setName("hadoop");
    aclEntryBuilder.setType(AclEntryType.USER);
    aclEntryBuilder.setPermission(FsAction.ALL);
    aclEntryBuilder.setScope(AclEntryScope.ACCESS);
    entries.add(aclEntryBuilder.build());

    aclEntryBuilder.setName("hdfs");
    aclEntryBuilder.setType(AclEntryType.GROUP);
    aclEntryBuilder.setPermission(FsAction.READ_WRITE);
    aclEntryBuilder.setScope(AclEntryScope.DEFAULT);
    entries.add(aclEntryBuilder.build());

    getMockAdlFileSystem().removeAclEntries(new Path("/test1/test2"), entries);

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .removeAclEntries(new Path("/test1/test2"), entries);
  }

  @Test(expected=AccessControlException.class)
  public void testRemoveDefaultAclEntries()
      throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().removeDefaultAcl(new Path("/test1/test2"));

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem().removeDefaultAcl(new Path("/test1/test2"));
  }

  @Test(expected=AccessControlException.class)
  public void testRemoveAcl() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().removeAcl(new Path("/test1/test2"));

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem().removeAcl(new Path("/test1/test2"));
  }

  @Test(expected=AccessControlException.class)
  public void testSetAcl() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    List<AclEntry> entries = new ArrayList<AclEntry>();
    AclEntry.Builder aclEntryBuilder = new AclEntry.Builder();
    aclEntryBuilder.setName("hadoop");
    aclEntryBuilder.setType(AclEntryType.USER);
    aclEntryBuilder.setPermission(FsAction.ALL);
    aclEntryBuilder.setScope(AclEntryScope.ACCESS);
    entries.add(aclEntryBuilder.build());

    aclEntryBuilder.setName("hdfs");
    aclEntryBuilder.setType(AclEntryType.GROUP);
    aclEntryBuilder.setPermission(FsAction.READ_WRITE);
    aclEntryBuilder.setScope(AclEntryScope.DEFAULT);
    entries.add(aclEntryBuilder.build());

    getMockAdlFileSystem().setAcl(new Path("/test1/test2"), entries);

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem().setAcl(new Path("/test1/test2"), entries);
  }

  @Test(expected=AccessControlException.class)
  public void testCheckAccess() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().access(new Path("/test1/test2"), FsAction.ALL);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().access(new Path("/test1/test2"), FsAction.EXECUTE);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().access(new Path("/test1/test2"), FsAction.READ);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem()
        .access(new Path("/test1/test2"), FsAction.READ_EXECUTE);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem()
        .access(new Path("/test1/test2"), FsAction.READ_WRITE);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().access(new Path("/test1/test2"), FsAction.NONE);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().access(new Path("/test1/test2"), FsAction.WRITE);

    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem()
        .access(new Path("/test1/test2"), FsAction.WRITE_EXECUTE);

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .access(new Path("/test1/test2"), FsAction.WRITE_EXECUTE);
  }

  @Test(expected=AccessControlException.class)
  public void testSetPermission() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem()
        .setPermission(new Path("/test1/test2"), FsPermission.getDefault());

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .setPermission(new Path("/test1/test2"), FsPermission.getDefault());
  }

  @Test(expected=AccessControlException.class)
  public void testSetOwner() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200));
    getMockAdlFileSystem().setOwner(new Path("/test1/test2"), "hadoop", "hdfs");

    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem()
        .setOwner(new Path("/test1/test2"), "hadoop", "hdfs");
  }

  @Test
  public void getAclStatusAsExpected() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(200)
        .setBody(TestADLResponseData.getGetAclStatusJSONResponse()));
    AclStatus aclStatus = getMockAdlFileSystem()
        .getAclStatus(new Path("/test1/test2"));
    Assert.assertEquals(aclStatus.getGroup(), "supergroup");
    Assert.assertEquals(aclStatus.getOwner(), "hadoop");
    Assert.assertEquals((Short) aclStatus.getPermission().toShort(),
        Short.valueOf("775", 8));

    for (AclEntry entry : aclStatus.getEntries()) {
      if (!(entry.toString().equalsIgnoreCase("user:carla:rw-") || entry
          .toString().equalsIgnoreCase("group::r-x"))) {
        Assert.fail("Unexpected entry : " + entry.toString());
      }
    }
  }

  @Test(expected=FileNotFoundException.class)
  public void getAclStatusNotExists() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(404)
        .setBody(TestADLResponseData.getFileNotFoundException()));

    getMockAdlFileSystem().getAclStatus(new Path("/test1/test2"));
  }

  @Test(expected=AccessControlException.class)
  public void testAclStatusDenied() throws URISyntaxException, IOException {
    getMockServer().enqueue(new MockResponse().setResponseCode(403)
        .setBody(TestADLResponseData.getAccessControlException()));

    getMockAdlFileSystem().getAclStatus(new Path("/test1/test2"));
  }
}
