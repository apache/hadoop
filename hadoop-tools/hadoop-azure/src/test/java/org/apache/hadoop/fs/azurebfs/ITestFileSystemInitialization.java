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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_AVAILABLE;
import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_ACLS;
import static org.apache.hadoop.fs.azurebfs.constants.InternalConstants.CAPABILITY_SAFE_READAHEAD;
import static org.junit.Assume.assumeTrue;

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
    assertEquals(fs.getUri(),
        new URI(scheme,
            filesystem + "@" + accountName,
            null,
            null,
            null));
    assertNotNull("working directory", fs.getWorkingDirectory());
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
          filesystem + "@" + accountName,
          null,
          null,
          null));
      assertNotNull("working directory", fs.getWorkingDirectory());
    }
  }

  @Test
  public void testFileSystemCapabilities() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();

    final Path p = new Path("}");
    // etags always present
    Assertions.assertThat(fs.hasPathCapability(p, ETAGS_AVAILABLE))
        .describedAs("path capability %s in %s", ETAGS_AVAILABLE, fs)
        .isTrue();
    // readahead always correct
    Assertions.assertThat(fs.hasPathCapability(p, CAPABILITY_SAFE_READAHEAD))
        .describedAs("path capability %s in %s", CAPABILITY_SAFE_READAHEAD, fs)
        .isTrue();

    // etags-over-rename and ACLs are either both true or both false.
    final boolean etagsAcrossRename = fs.hasPathCapability(p, ETAGS_PRESERVED_IN_RENAME);
    final boolean acls = fs.hasPathCapability(p, FS_ACLS);
    Assertions.assertThat(etagsAcrossRename)
        .describedAs("capabilities %s=%s and %s=%s in %s",
            ETAGS_PRESERVED_IN_RENAME, etagsAcrossRename,
            FS_ACLS, acls, fs)
        .isEqualTo(acls);
  }
}
