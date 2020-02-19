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

package org.apache.hadoop.fs.azurebfs;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemAuthorization extends AbstractAbfsIntegrationTest {

  private static final String TEST_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider";
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();

  public ITestAzureBlobFileSystemAuthorization() throws Exception {
    // The mock SAS token provider relies on the account key to generate SAS.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_AUTHZ_CLASS);
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
    super.setup();
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class,
        ()-> {
          fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
    fs.open(path).close();
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.open(path).close();
    });
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.create(path).close();
    });
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
    fs.append(path).close();
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.append(path).close();
    });
  }

  @Test
  public void testRenameAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    Path dst = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
    fs.rename(path, dst);
  }

  @Test
  public void testRenameUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    Path dst = new Path(UUID.randomUUID().toString());
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.rename(path, dst);
    });
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
    fs.delete(path, false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.delete(path, false);
    });
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.create(path).close();
    fs.listStatus(path);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.listStatus(path);
    });
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString());
    fs.getFileStatus(path);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.getFileStatus(path);
    });
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.setOwner(path, TEST_USER, TEST_GROUP);
    });
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.modifyAclEntries(path, aclSpec);
    });
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.removeAclEntries(path, aclSpec);
    });
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.removeDefaultAcl(path);
    });
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.removeAcl(path);
    });
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.setAcl(path, aclSpec);
    });
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    Path path = new Path(UUID.randomUUID().toString());
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.getAclStatus(path);
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(UUID.randomUUID().toString() + "unauthorized");
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(SASTokenProviderException.class,
        ()-> {
          fs.getAclStatus(path);
    });
  }
}
