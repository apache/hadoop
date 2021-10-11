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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.extensions.MockEncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CLIENT_PROVIDED_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;

public class ITestEncryptionContext extends AbstractAbfsIntegrationTest {
  public ITestEncryptionContext() throws Exception {
  }

  private AzureBlobFileSystem getEncryptionEnabledFS() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE,
        MockEncryptionContextProvider.class.getCanonicalName());
    configuration.unset(
        FS_AZURE_ENCRYPTION_CLIENT_PROVIDED_KEY + "." + getAccountName());
    return (AzureBlobFileSystem) FileSystem.newInstance(configuration);
  }

  private AzureBlobFileSystem getEncryptionDisabledFS() throws IOException {
    Configuration conf = getRawConfiguration();
    conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  @Test
  public void testCreateEncryptedFile() throws Exception {
    AzureBlobFileSystem fs = getEncryptionEnabledFS();
    Path testPath = path("/testFile");
    createEncryptedFile(fs, testPath);
  }

  @Test
  public void testOpenReadWrite() throws Exception {
    AzureBlobFileSystem fs = getEncryptionEnabledFS();
    Path testPath = path("/testFile");
    createEncryptedFile(fs, testPath).close();

    try (FSDataOutputStream out = fs.append(testPath)) {
      out.write("bytes".getBytes());
      out.flush();
    }
    try (FSDataInputStream in = fs.open(testPath)) {
      byte[] buffer = new byte[7];
      Assertions.assertThat(in.read(buffer))
          .describedAs("Incorrect read length").isEqualTo(5);
    }

    AzureBlobFileSystem fs1 = getEncryptionDisabledFS();
    LambdaTestUtils.intercept(IOException.class, () -> {
      try (FSDataOutputStream out = fs1.append(testPath)) {
        out.write("bytes".getBytes(StandardCharsets.UTF_8));
      }
    });
    LambdaTestUtils.intercept(IOException.class, () -> {
      try (FSDataInputStream in = fs1.open(testPath)) {
        in.read(new byte[5]);
      }
    });
  }

  @Test
  public void testGetSetPathProperties() throws Exception {
    AzureBlobFileSystem fs = getEncryptionEnabledFS();
    Path testPath = path("/testFile");
    createEncryptedFile(fs, testPath);
    Assertions.assertThat(fs.getFileStatus(testPath)).isNotNull();
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    fs.setXAttr(testPath, "attribute", value);
    assertArrayEquals(value, fs.getXAttr(testPath, "attribute"));

    AzureBlobFileSystem fs1 = getEncryptionDisabledFS();
    Assertions.assertThat(fs1.getFileStatus(testPath)).isNotNull();
    LambdaTestUtils.intercept(IOException.class,
        () -> fs1.setXAttr(testPath, "attribute", value));
    LambdaTestUtils.intercept(IOException.class,
        () -> fs1.getXAttr(testPath, "attribute"));
  }

  @Test
  public void testOtherApiWithoutEncryptionHeaders() throws Exception {
    testOtherApi(getEncryptionDisabledFS());
  }

  @Test
  public void testOtherApiWithEncryptionHeaders() throws Exception {
    testOtherApi(getEncryptionEnabledFS());
  }

  private void testOtherApi(AzureBlobFileSystem fs)
      throws Exception {
    Path testPath = path("/testFile");
    createEncryptedFile(getEncryptionEnabledFS(), testPath);
    Assertions.assertThat(fs.listStatus(testPath))
        .describedAs("ListStatus should succeed without encryption headers")
        .isNotEmpty();
    fs.setAcl(testPath,
        Collections.singletonList(aclEntry(ACCESS, USER, FsAction.ALL)));
    fs.modifyAclEntries(testPath,
        Collections.singletonList(aclEntry(ACCESS, USER, FsAction.EXECUTE)));
    fs.setPermission(testPath, FsPermission.getDefault());
    Assertions.assertThat(fs.getAclStatus(testPath))
        .describedAs("GetAcl should succeed without encryption headers")
        .isNotNull();
    fs.access(testPath, FsAction.EXECUTE);
    Path renamedPath = new Path(testPath + "_2");
    Assertions.assertThat(fs.rename(testPath, renamedPath))
        .describedAs("Rename should succeed without encryption headers")
        .isTrue();
    Assertions.assertThat(fs.delete(renamedPath, false))
        .describedAs("Delete should succeed without encryption headers")
        .isTrue();
  }

  private AbfsOutputStream createEncryptedFile(AzureBlobFileSystem fs,
      Path testPath) throws Exception {
    FSDataOutputStream out = fs.create(testPath);
    // verify file is encrypted by calling getPathStatus (with properties)
    // without encryption headers in request
    fs.getAbfsClient().setEncryptionType(EncryptionType.NONE);
    LambdaTestUtils.intercept(IOException.class, () -> fs.getAbfsClient()
        .getPathStatus(fs.getAbfsStore().getRelativePath(testPath), true,
            getTestTracingContext(fs, false)));
    fs.getAbfsClient().setEncryptionType(EncryptionType.ENCRYPTION_CONTEXT);
    return (AbfsOutputStream) out.getWrappedStream();
  }
}