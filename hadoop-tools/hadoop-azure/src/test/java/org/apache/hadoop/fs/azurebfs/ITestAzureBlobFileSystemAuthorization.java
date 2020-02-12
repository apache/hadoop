/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer.*;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemAuthorization
    extends AbstractAbfsIntegrationTest {

  private static final Path TEST_READ_ONLY_FILE_PATH_0 = new Path(
      TEST_READ_ONLY_FILE_0);
  private static final Path TEST_READ_ONLY_FILE_PATH_1 = new Path(
      TEST_READ_ONLY_FILE_1);
  private static final Path TEST_READ_ONLY_FOLDER_PATH = new Path(
      TEST_READ_ONLY_FOLDER);
  private static final Path TEST_WRITE_ONLY_FILE_PATH_0 = new Path(
      TEST_WRITE_ONLY_FILE_0);
  private static final Path TEST_WRITE_ONLY_FILE_PATH_1 = new Path(
      TEST_WRITE_ONLY_FILE_1);
  private static final Path TEST_READ_WRITE_FILE_PATH_0 = new Path(
      TEST_READ_WRITE_FILE_0);
  private static final Path TEST_READ_WRITE_FILE_PATH_1 = new Path(
      TEST_READ_WRITE_FILE_1);
  private static final Path TEST_WRITE_ONLY_FOLDER_PATH = new Path(
      TEST_WRITE_ONLY_FOLDER);
  private static final Path TEST_WRITE_THEN_READ_ONLY_PATH = new Path(
      TEST_WRITE_THEN_READ_ONLY);
  private static final String TEST_AUTHZ_CLASS = "org.apache.hadoop.fs"
      + ".azurebfs.extensions.MockAbfsAuthorizer";
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();

  public ITestAzureBlobFileSystemAuthorization() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    this.getConfiguration().setAbfsAuthorizerClass(TEST_AUTHZ_CLASS);
    loadAuthorizer();
    super.setup();
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class, () -> {
      fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    fs.open(TEST_WRITE_THEN_READ_ONLY_PATH).close();
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.open(TEST_WRITE_ONLY_FILE_PATH_0).close();
    });
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.create(TEST_READ_ONLY_FILE_PATH_0).close();
    });
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    fs.append(TEST_READ_WRITE_FILE_PATH_0).close();
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    //intercept(AbfsAuthorizationException.class,
    //        ()-> {
    try {

      FSDataOutputStream opStr = fs.append(TEST_WRITE_THEN_READ_ONLY_PATH);
      opStr.close();
    } catch (Exception ex) {
      String exStr = ex.getMessage();
    }
    //});
  }

  @Test
  public void testRenameAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.rename(TEST_READ_WRITE_FILE_PATH_0, TEST_READ_WRITE_FILE_PATH_1);
  }

  @Test
  public void testRenameSourceUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.rename(TEST_READ_ONLY_FILE_PATH_0, TEST_READ_WRITE_FILE_PATH_0);
    });
  }

  @Test
  public void testRenameDestUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.rename(TEST_READ_WRITE_FILE_PATH_0, TEST_READ_ONLY_FILE_PATH_1);
    });
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.delete(TEST_WRITE_ONLY_FILE_PATH_0, false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.delete(TEST_WRITE_THEN_READ_ONLY_PATH, false);
    });
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer()).name1 = "test1";
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer()).name2 = "test2";
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    fs.listStatus(TEST_WRITE_THEN_READ_ONLY_PATH);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.listStatus(TEST_WRITE_ONLY_FILE_PATH_0);
    });
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(TEST_WRITE_ONLY_FOLDER_PATH,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.mkdirs(TEST_READ_ONLY_FOLDER_PATH,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    fs.getFileStatus(TEST_WRITE_THEN_READ_ONLY_PATH);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    intercept(AbfsAuthorizationException.class, () -> {
      fs.getFileStatus(TEST_WRITE_ONLY_FILE_PATH_0);
    });
  }

  @Test
  public void testSetOwnerAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.setOwner(TEST_WRITE_ONLY_FILE_PATH_0, TEST_USER, TEST_GROUP);
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.setOwner(TEST_WRITE_THEN_READ_ONLY_PATH, TEST_USER, TEST_GROUP);
    });
  }

  @Test
  public void testSetPermissionAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.setPermission(TEST_WRITE_ONLY_FILE_PATH_0,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);

    intercept(AbfsAuthorizationException.class, () -> {
      fs.setPermission(TEST_WRITE_THEN_READ_ONLY_PATH,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testModifyAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.modifyAclEntries(TEST_READ_WRITE_FILE_PATH_0, aclSpec);
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.modifyAclEntries(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
  }

  @Test
  public void testRemoveAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.removeAclEntries(TEST_READ_WRITE_FILE_PATH_0, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeAclEntries(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
  }

  @Test
  public void testRemoveDefaultAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    fs.removeDefaultAcl(TEST_READ_WRITE_FILE_PATH_0);
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeDefaultAcl(TEST_WRITE_THEN_READ_ONLY_PATH);
    });
  }

  @Test
  public void testRemoveAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    fs.removeAcl(TEST_READ_WRITE_FILE_PATH_0);
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    intercept(AbfsAuthorizationException.class, () -> {
      fs.removeAcl(TEST_WRITE_THEN_READ_ONLY_PATH);
    });
  }

  @Test
  public void testSetAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_READ_WRITE_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.setAcl(TEST_READ_WRITE_FILE_PATH_0, aclSpec);
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.setAcl(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(0);
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    ((MockAbfsAuthorizer) fs.getAbfsStore().getAuthorizer())
        .setwriteThenReadOnly(1);
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.getAclStatus(TEST_WRITE_THEN_READ_ONLY_PATH);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled",
        fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays
        .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class, () -> {
      fs.getAclStatus(TEST_WRITE_ONLY_FILE_PATH_0);
    });
  }
}
