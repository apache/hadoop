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

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

/**
 * Test AzureBlobFileSystem registration.
 * Use casts to have interesting stack traces on failures.
 */
public class ITestFileSystemRegistration extends AbstractAbfsIntegrationTest {

  protected static final String ABFS = "org.apache.hadoop.fs.azurebfs.Abfs";
  protected static final String ABFSS = "org.apache.hadoop.fs.azurebfs.Abfss";

  public ITestFileSystemRegistration() throws Exception {
  }

  private void assertConfigMatches(Configuration conf, String key, String expected) {
    String v = conf.get(key);
    assertNotNull("No value for key " + key, v);
    assertEquals("Wrong value for key " + key, expected, v);
  }

  @Test
  public void testAbfsFileSystemRegistered() throws Throwable {
    assertConfigMatches(new Configuration(true),
        "fs.abfs.impl",
        "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
  }

  @Test
  public void testSecureAbfsFileSystemRegistered() throws Throwable {
    assertConfigMatches(new Configuration(true),
        "fs.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
  }

  @Test
  public void testAbfsFileContextRegistered() throws Throwable {
    assertConfigMatches(new Configuration(true),
        "fs.AbstractFileSystem.abfs.impl",
        ABFS);
  }

  @Test
  public void testSecureAbfsFileContextRegistered() throws Throwable {
    assertConfigMatches(new Configuration(true),
        "fs.AbstractFileSystem.abfss.impl",
        ABFSS);
  }

  @Test
  public void ensureAzureBlobFileSystemIsDefaultFileSystem() throws Exception {
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(getConfiguration());
    assertNotNull("filesystem", fs);

    if (this.getAuthType() == AuthType.OAuth) {
      Abfss afs = (Abfss) FileContext.getFileContext(getConfiguration()).getDefaultFileSystem();
      assertNotNull("filecontext", afs);
    } else {
      Abfs afs = (Abfs) FileContext.getFileContext(getConfiguration()).getDefaultFileSystem();
      assertNotNull("filecontext", afs);
    }

  }

  @Test
  public void ensureSecureAzureBlobFileSystemIsDefaultFileSystem() throws Exception {
    final String accountName = getAccountName();
    final String fileSystemName = getFileSystemName();

    final URI defaultUri = new URI(FileSystemUriSchemes.ABFS_SECURE_SCHEME,
        fileSystemName + "@" + accountName,
        null,
        null,
        null);
    getConfiguration().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        defaultUri.toString());

    SecureAzureBlobFileSystem fs = (SecureAzureBlobFileSystem) FileSystem.get(
        getConfiguration());
    assertNotNull("filesystem", fs);
    Abfss afs = (Abfss) FileContext.getFileContext(getConfiguration()).getDefaultFileSystem();
    assertNotNull("filecontext", afs);
  }
}
