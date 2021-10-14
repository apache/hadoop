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
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.security.EncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.util.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.extensions.MockEncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CLIENT_PROVIDED_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_REQUEST_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.GLOBAL_KEY;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.NONE;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;

@RunWith(Parameterized.class)
public class ITestEncryptionContext extends AbstractAbfsIntegrationTest {
  private final String cpk = "12345678901234567890123456789012";
  private final String cpkSHAEncoded = EncryptionAdapter.getBase64EncodedString(
      EncryptionAdapter.getSHA256Hash(cpk));

  @Parameterized.Parameter
  public EncryptionType fileEncryptionType;

  @Parameterized.Parameter(1)
  public EncryptionType requestEncryptionType;

  @Parameterized.Parameter(2)
  public FSOperationType operation;

  @Parameterized.Parameter(3)
  public boolean responseHeaderServerEnc;

  @Parameterized.Parameter(4)
  public boolean responseHeaderReqServerEnc;

  @Parameterized.Parameter(5)
  public boolean isExceptionCase;

  @Parameterized.Parameter(6)
  public boolean isCpkResponseHdrExpected;


  @Parameterized.Parameters(name = "{2}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][] {
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.READ, true,
            false, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.WRITE, false,
            true, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.SET_ACL, false,
            false, false, false},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.GET_ATTR, false,
            false, false, false},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.LISTSTATUS, false,
            false, false, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.WRITE, false, false, true,
            false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.GET_ATTR, false, false, true,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.READ, true, false, false,
            true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.WRITE, false, true, false,
            true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.SET_ACL, false, false, false,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.LISTSTATUS, false, false, false,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.RENAME, false, false, false,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.DELETE, false, false, false,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.GET_ATTR, false, false, false,
            false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.SET_ATTR, false, false, false,
            false},
        {GLOBAL_KEY, NONE, FSOperationType.READ, true, false, true,
            true},
        {GLOBAL_KEY, NONE, FSOperationType.WRITE, false, true, true,
            true},
        {GLOBAL_KEY, NONE, FSOperationType.SET_ACL, false, false, false,
            false},
        {GLOBAL_KEY, NONE, FSOperationType.RENAME, true, false, true,
            true},
        {GLOBAL_KEY, NONE, FSOperationType.LISTSTATUS, false, true, true,
            true},
        {GLOBAL_KEY, NONE, FSOperationType.DELETE, false, false, false,
            false},
    });
  }

  public ITestEncryptionContext() throws Exception {
    super();
  }

  @Test
  public void runTest() throws Exception {
    AzureBlobFileSystem fs = getFS();
    Path testPath = path("/testFile");
    EncryptionContextProvider ecp = createEncryptedFile(testPath);
    AbfsRestOperation op = callOperation(fs, testPath, ecp);
    if (op == null) {
      return;
    }
    AbfsHttpOperation httpOp = op.getResult();
    if (isCpkResponseHdrExpected) {
      Assertions.assertThat(httpOp.getResponseHeader(X_MS_ENCRYPTION_KEY_SHA256))
          .isEqualTo(cpkSHAEncoded);

      if (operation == FSOperationType.READ) {
        Assertions.assertThat(httpOp.getResponseHeader(X_MS_SERVER_ENCRYPTED)).isEqualTo("true");
      } else if (operation == FSOperationType.WRITE || operation == FSOperationType.APPEND) {
        Assertions.assertThat(httpOp.getResponseHeader(X_MS_REQUEST_SERVER_ENCRYPTED))
            .isEqualTo("true");
      }
    }
  }

  private AbfsRestOperation callOperation(AzureBlobFileSystem fs,
      Path testPath, EncryptionContextProvider ecp)
      throws Exception {
    AbfsClient client = fs.getAbfsClient();
    client.setEncryptionContextProvider(ecp);
    if (isExceptionCase) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        switch (operation) {
        case WRITE: try (FSDataOutputStream out = fs.append(testPath)) {
            out.write("bytes".getBytes());
          }
          break;
        case READ: try (FSDataInputStream in = fs.open(testPath)) {
            in.read(new byte[5]);
          }
          break;
        case SET_ATTR:
          fs.setXAttr(testPath, "attribute", "value".getBytes()); break;
        case GET_ATTR:
          fs.getXAttr(testPath, "attribute"); break;
        }
      });
      return null;
    } else {
      EncryptionAdapter encryptionAdapter = null;
      if (fileEncryptionType == ENCRYPTION_CONTEXT) {
        encryptionAdapter = new EncryptionAdapter(
//            client.getEncryptionContextProvider(),
            ecp,
            fs.getAbfsStore().getRelativePath(testPath),
            "context".getBytes(StandardCharsets.UTF_8));
      }
      String path = fs.getAbfsStore().getRelativePath(testPath);
      switch (operation) {
      case READ:
        return client.read(path, 0,
            new byte[5],
            0, 5, null, null, encryptionAdapter,
            getTestTracingContext(fs, true));
      case WRITE: return client.flush(path, 3, false, false, null, null,
          encryptionAdapter, getTestTracingContext(fs, false));
      case SET_ACL: return client.setAcl(path, AclEntry.aclSpecToString(
          Lists.newArrayList(aclEntry(ACCESS, USER, ALL))),
          getTestTracingContext(fs, false));
      case LISTSTATUS: return client.listPath(path, false, 5, null,
          getTestTracingContext(fs, true));
      case RENAME: return client.renamePath(path, new Path(path + "_2").toString(),
          null,
          getTestTracingContext(fs, true));
//        return client.renamePath(new Path(path + "_2").toString(), path, null,
//            getTestTracingContext(fs, true));
      case DELETE: return client.deletePath(path, false, null,
          getTestTracingContext(fs, false));
      case GET_ATTR: return client.getPathStatus(path, true,
          getTestTracingContext(fs, false));
      default: throw new NoSuchFieldException();
      }
    }
  }

  private AzureBlobFileSystem getECProviderEnabledFS() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE,
        MockEncryptionContextProvider.class.getCanonicalName());
    configuration.unset(
        FS_AZURE_ENCRYPTION_CLIENT_PROVIDED_KEY + "." + getAccountName());
    return (AzureBlobFileSystem) FileSystem.newInstance(configuration);
  }

  private AzureBlobFileSystem getCPKenabledFS() throws IOException {
    Configuration conf = getRawConfiguration();
    conf.set(FS_AZURE_ENCRYPTION_CLIENT_PROVIDED_KEY + "." + getAccountName(),
        cpk);
    conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private AzureBlobFileSystem getEncryptionDisabledFS() throws IOException {
    Configuration conf = getRawConfiguration();
    conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private AzureBlobFileSystem getFS() throws Exception {
    AzureBlobFileSystem fs;
    if (requestEncryptionType == ENCRYPTION_CONTEXT) {
      fs = getECProviderEnabledFS();
    } else if (requestEncryptionType == GLOBAL_KEY) {
      fs = getCPKenabledFS();
    } else {
      fs = getEncryptionDisabledFS();
    }
    return fs;
  }

  @Test
  public void testCreateEncryptedFile() throws Exception {
    Path testPath = path("/testFile");
    createEncryptedFile(testPath);
  }

  @Test
  public void testRead() throws Exception {
    AzureBlobFileSystem fs = getFS();
    Path testPath = path("/testFile");
    AbfsClient client = fs.getAbfsClient();
    EncryptionAdapter encryptionAdapter = null;
    if (fileEncryptionType == ENCRYPTION_CONTEXT) {
      encryptionAdapter = new EncryptionAdapter(
          client.getEncryptionContextProvider(),
          fs.getAbfsStore().getRelativePath(testPath),
          "context".getBytes(StandardCharsets.UTF_8));
    }
    AbfsRestOperation op = client.read(
        fs.getAbfsStore().getRelativePath(testPath), 0, new byte[5], 0, 5, null,
        null, encryptionAdapter, getTestTracingContext(fs, true));
    checkEncryptionResponseHeaders(op.getResult(), FSOperationType.READ);
  }

  @Test
  public void testWrite() throws Exception {
    AzureBlobFileSystem fs = getECProviderEnabledFS();
  }

  private void checkEncryptionResponseHeaders(AbfsHttpOperation op,
      FSOperationType type) {
    Assertions.assertThat(op.getResponseHeader(X_MS_ENCRYPTION_KEY_SHA256)).isEqualTo(cpkSHAEncoded);
    if (type == FSOperationType.READ) {
      Assertions.assertThat(op.getResponseHeader(X_MS_SERVER_ENCRYPTED)).isEqualTo("true");
    } else if (type == FSOperationType.WRITE || type == FSOperationType.APPEND) {
      Assertions.assertThat(op.getResponseHeader(X_MS_REQUEST_SERVER_ENCRYPTED))
          .isEqualTo("true");
    }
  }

  @Test
  public void testOpenReadWrite() throws Exception {
    AzureBlobFileSystem fs = getECProviderEnabledFS();
    Path testPath = path("/testFile");
    createEncryptedFile(testPath);

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
    AzureBlobFileSystem fs = getECProviderEnabledFS();
    Path testPath = path("/testFile");
    createEncryptedFile(testPath);
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
    testOtherApi(getECProviderEnabledFS());
  }

  private void testOtherApi(AzureBlobFileSystem fs)
      throws Exception {
    Path testPath = path("/testFile");
    createEncryptedFile(testPath);
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

  private EncryptionContextProvider createEncryptedFile(Path testPath) throws Exception {
    AzureBlobFileSystem fs = fileEncryptionType == ENCRYPTION_CONTEXT?
        getECProviderEnabledFS() : getCPKenabledFS();
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write("123".getBytes());
    }
    // verify file is encrypted by calling getPathStatus (with properties)
    // without encryption headers in request
    if (fileEncryptionType != EncryptionType.NONE) {
      fs.getAbfsClient().setEncryptionType(EncryptionType.NONE);
      LambdaTestUtils.intercept(IOException.class, () -> fs.getAbfsClient()
          .getPathStatus(fs.getAbfsStore().getRelativePath(testPath), true,
              getTestTracingContext(fs, false)));
      fs.getAbfsClient().setEncryptionType(fileEncryptionType);
    }
    return fs.getAbfsClient().getEncryptionContextProvider();
  }
}