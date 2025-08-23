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

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_AVAILABLE;
import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_ACLS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.CAPABILITY_SAFE_READAHEAD;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_INVALID_ABFS_STATE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test AzureBlobFileSystem initialization.
 */
public class ITestFileSystemInitialization extends AbstractAbfsIntegrationTest {
  public ITestFileSystemInitialization() throws Exception {
    super();
  }

  @Test
  public void ensureAzureBlobFileSystemIsInitialized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final String accountName = getAccountName();
    final String filesystem = getFileSystemName();

    String scheme = this.getAuthType() == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
            : FileSystemUriSchemes.ABFS_SECURE_SCHEME;
    assertEquals(fs.getUri(), new URI(scheme,
        filesystem + "@" + accountName, null, null, null));
    assertNotNull(fs.getWorkingDirectory(), "working directory");
  }

  @Test
  public void ensureSecureAzureBlobFileSystemIsInitialized() throws Exception {
    final String accountName = getAccountName();
    final String filesystem = getFileSystemName();
    final URI defaultUri = new URI(FileSystemUriSchemes.ABFS_SECURE_SCHEME,
        filesystem + "@" + accountName,
        null,
        null,
        null);
    Configuration rawConfig = getRawConfiguration();
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());

    try(SecureAzureBlobFileSystem fs = (SecureAzureBlobFileSystem) FileSystem.newInstance(rawConfig)) {
      assertEquals(fs.getUri(), new URI(FileSystemUriSchemes.ABFS_SECURE_SCHEME,
          filesystem + "@" + accountName, null, null, null));
      assertNotNull(fs.getWorkingDirectory(), "working directory");
    }
  }

  @Test
  public void testFileSystemCapabilities() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();

    final Path p = new Path("}");
    // etags always present
    assertThat(fs.hasPathCapability(p, ETAGS_AVAILABLE))
        .describedAs("path capability %s in %s", ETAGS_AVAILABLE, fs)
        .isTrue();
    // readahead always correct
    assertThat(fs.hasPathCapability(p, CAPABILITY_SAFE_READAHEAD))
        .describedAs("path capability %s in %s", CAPABILITY_SAFE_READAHEAD, fs)
        .isTrue();

    // etags-over-rename and ACLs are either both true or both false.
    final boolean etagsAcrossRename = fs.hasPathCapability(p, ETAGS_PRESERVED_IN_RENAME);
    final boolean acls = fs.hasPathCapability(p, FS_ACLS);
    assertThat(etagsAcrossRename)
        .describedAs("capabilities %s=%s and %s=%s in %s",
            ETAGS_PRESERVED_IN_RENAME, etagsAcrossRename,
            FS_ACLS, acls, fs)
        .isEqualTo(acls);
  }

  /**
   * Test that the AzureBlobFileSystem close without init works
   * @throws Exception if an error occurs
   */
  @Test
  public void testABFSCloseWithoutInit() throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    assertThat(fs.isClosed()).isTrue();
    fs.close();
    fs.initialize(this.getFileSystem().getUri(), getRawConfiguration());
    assertThat(fs.isClosed()).isFalse();
    fs.close();
    assertThat(fs.isClosed()).isTrue();
  }

  /**
   * Test that the AzureBlobFileSystem throws an exception
   * when trying to perform an operation without initialization.
   * @throws Exception if an error occurs
   */
  @Test
  public void testABFSUninitializedFileSystem() throws Exception {
    AzureBlobFileSystem fs = new AzureBlobFileSystem();
    assertThat(fs.isClosed()).isTrue();
    Path testPath = new Path("testPath");

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        fs::toString);

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.open(testPath, ONE_MB));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.create(testPath, FsPermission.getDefault(), false, ONE_MB,
            fs.getDefaultReplication(testPath), ONE_MB, null));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.createNonRecursive(testPath, FsPermission.getDefault(), false, ONE_MB,
            fs.getDefaultReplication(testPath), ONE_MB, null));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.append(testPath, ONE_MB, null));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.rename(testPath, testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.delete(testPath, true));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.listStatus(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.mkdirs(testPath, FsPermission.getDefault()));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.getFileStatus(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.breakLease(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.makeQualified(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.setOwner(testPath, EMPTY_STRING, EMPTY_STRING));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.setXAttr(testPath, "xattr", new byte[0],
            EnumSet.of(XAttrSetFlag.CREATE)));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.getXAttr(testPath, "xattr"));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.setPermission(testPath, FsPermission.getDefault()));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.modifyAclEntries(testPath, new ArrayList<>()));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.removeAclEntries(testPath, new ArrayList<>()));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.removeDefaultAcl(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.removeAcl(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.setAcl(testPath, new ArrayList<>()));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.getAclStatus(testPath));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.access(testPath, FsAction.ALL));

    intercept(IllegalStateException.class, ERR_INVALID_ABFS_STATE,
        () -> fs.exists(testPath));
  }
}
