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
import java.util.Base64;
import java.util.Hashtable;
import java.util.Random;

import org.apache.hadoop.fs.azurebfs.security.EncodingHelper;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockEncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.security.EncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Lists;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY_SHA;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_REQUEST_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.ENCRYPTION_KEY_LEN;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.APPEND_MODE;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.GLOBAL_KEY;
import static org.apache.hadoop.fs.azurebfs.utils.EncryptionType.NONE;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;

@RunWith(Parameterized.class)
public class ITestAbfsCustomEncryption extends AbstractAbfsIntegrationTest {
  private final byte[] cpk = new byte[ENCRYPTION_KEY_LEN];
  private final String cpkSHAEncoded;

  // Encryption type used by filesystem while creating file
  @Parameterized.Parameter
  public EncryptionType fileEncryptionType;

  // Encryption type used by filesystem to call different operations
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


  @Parameterized.Parameters(name = "{0} mode, {2}")
  public static Iterable<Object[]> params() {
    return Arrays.asList(new Object[][] {
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.READ, true, false, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.WRITE, false, true, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.APPEND, false, true, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.SET_ACL, false, false, false, false},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.GET_ATTR, true, false, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.SET_ATTR, false, true, false, true},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.LISTSTATUS, false, false, false, false},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.RENAME, false, false, false, false},
        {ENCRYPTION_CONTEXT, ENCRYPTION_CONTEXT, FSOperationType.DELETE, false, false, false, false},

        {ENCRYPTION_CONTEXT, NONE, FSOperationType.WRITE, false, false, true, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.GET_ATTR, true, false, true, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.READ, false, false, true, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.SET_ATTR, false, true, true, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.RENAME, false, false, false, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.LISTSTATUS, false, false, false, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.DELETE, false, false, false, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.SET_ACL, false, false, false, false},
        {ENCRYPTION_CONTEXT, NONE, FSOperationType.SET_PERMISSION, false, false, false, false},

        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.READ, true, false, false, true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.WRITE, false, true, false, true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.APPEND, false, true, false, true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.SET_ACL, false, false, false, false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.LISTSTATUS, false, false, false, false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.RENAME, false, false, false, false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.DELETE, false, false, false, false},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.GET_ATTR, true, false, false, true},
        {GLOBAL_KEY, GLOBAL_KEY, FSOperationType.SET_ATTR, false, true, false, true},

        {GLOBAL_KEY, NONE, FSOperationType.READ, true, false, true, true},
        {GLOBAL_KEY, NONE, FSOperationType.WRITE, false, true, true, true},
        {GLOBAL_KEY, NONE, FSOperationType.SET_ATTR, false, false, true, true},
        {GLOBAL_KEY, NONE, FSOperationType.SET_ACL, false, false, false, false},
        {GLOBAL_KEY, NONE, FSOperationType.RENAME, false, false, false, false},
        {GLOBAL_KEY, NONE, FSOperationType.LISTSTATUS, false, false, false, false},
        {GLOBAL_KEY, NONE, FSOperationType.DELETE, false, false, false, false},
        {GLOBAL_KEY, NONE, FSOperationType.SET_PERMISSION, false, false, false, false},
    });
  }

  public ITestAbfsCustomEncryption() throws Exception {
    super();
    Assume.assumeTrue("Account should be HNS enabled for CPK",
        getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT,
            false));
    new Random().nextBytes(cpk);
    cpkSHAEncoded = EncodingHelper.getBase64EncodedString(
        EncodingHelper.getSHA256Hash(cpk));
  }

  @Test
  public void testCustomEncryptionCombinations() throws Exception {
    AzureBlobFileSystem fs = getFS();
    Path testPath = path("/testFile");
    String relativePath = fs.getAbfsStore().getRelativePath(testPath);
    MockEncryptionContextProvider ecp =
        (MockEncryptionContextProvider) createEncryptedFile(testPath);
    AbfsRestOperation op = callOperation(fs, new Path(relativePath), ecp);
    if (op == null) {
      return;
    }
    AbfsHttpOperation httpOp = op.getResult();
    if (isCpkResponseHdrExpected) {
      if (requestEncryptionType == ENCRYPTION_CONTEXT) {
        String encryptionContext = ecp.getEncryptionContextForTest(relativePath);
        String expectedKeySHA = EncodingHelper.getBase64EncodedString(
            EncodingHelper.getSHA256Hash(
                ecp.getEncryptionKeyForTest(encryptionContext)));
        Assertions.assertThat(httpOp.getResponseHeader(X_MS_ENCRYPTION_KEY_SHA256))
            .isEqualTo(expectedKeySHA);
      } else {  // GLOBAL_KEY
        Assertions.assertThat(httpOp.getResponseHeader(X_MS_ENCRYPTION_KEY_SHA256))
            .isEqualTo(cpkSHAEncoded);
      }
    } else {
      Assertions.assertThat(httpOp.getResponseHeader(X_MS_ENCRYPTION_KEY_SHA256))
          .isEqualTo(null);
    }
    Assertions.assertThat(httpOp.getResponseHeader(X_MS_SERVER_ENCRYPTED))
        .isEqualTo(responseHeaderServerEnc? "true" : null);
    Assertions.assertThat(httpOp.getResponseHeader(X_MS_REQUEST_SERVER_ENCRYPTED))
        .isEqualTo(responseHeaderReqServerEnc? "true" : null);
  }

  /**
   * Executes a given operation at the AbfsClient level and returns
   * AbfsRestOperation instance to verify response headers. Asserts excetion
   * for combinations that should not succeed.
   * @param fs AzureBlobFileSystem instance
   * @param testPath path of file
   * @param ecp EncryptionContextProvider instance to support AbfsClient methods
   * @return Rest op or null depending on whether the request is allowed
   * @throws Exception error
   */
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
        case SET_ATTR: fs.setXAttr(testPath, "attribute", "value".getBytes());
          break;
        case GET_ATTR: fs.getXAttr(testPath, "attribute");
        default: throw new NoSuchFieldException();
        }
      });
      return null;
    } else {
      EncryptionAdapter encryptionAdapter = null;
      if (fileEncryptionType == ENCRYPTION_CONTEXT) {
        encryptionAdapter = new EncryptionAdapter(ecp,
            fs.getAbfsStore().getRelativePath(testPath),
            Base64.getEncoder().encode(
            ((MockEncryptionContextProvider) ecp).getEncryptionContextForTest(testPath.toString())
            .getBytes(StandardCharsets.UTF_8)));
      }
      String path = testPath.toString();
      switch (operation) {
      case READ: return client.read(path, 0, new byte[5], 0, 5, null,
          null, encryptionAdapter, getTestTracingContext(fs, true));
      case WRITE: return client.flush(path, 3, false, false, null,
          null, encryptionAdapter, getTestTracingContext(fs, false));
      case APPEND: return client.append(path, "val".getBytes(),
            new AppendRequestParameters(3, 0, 3, APPEND_MODE, false, null),
            null, encryptionAdapter, getTestTracingContext(fs, false));
      case SET_ACL: return client.setAcl(path, AclEntry.aclSpecToString(
          Lists.newArrayList(aclEntry(ACCESS, USER, ALL))),
          getTestTracingContext(fs, false));
      case LISTSTATUS: return client.listPath(path, false, 5, null,
          getTestTracingContext(fs, true));
      case RENAME: return client.renamePath(path, new Path(path + "_2").toString(),
          null, getTestTracingContext(fs, true), null, false).getOp();
      case DELETE: return client.deletePath(path, false, null,
          getTestTracingContext(fs, false));
      case GET_ATTR: return client.getPathStatus(path, true,
          getTestTracingContext(fs, false));
      case SET_ATTR:
        Hashtable<String, String> properties = new Hashtable<>();
        properties.put("key", "{ value: valueTest }");
        return client.setPathProperties(path, fs.getAbfsStore()
                .convertXmsPropertiesToCommaSeparatedString(properties),
            getTestTracingContext(fs, false));
      case SET_PERMISSION:
        return client.setPermission(path, FsPermission.getDefault().toString(),
            getTestTracingContext(fs, false));
      default: throw new NoSuchFieldException();
      }
    }
  }

  private AzureBlobFileSystem getECProviderEnabledFS() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE + "."
        + getAccountName(), MockEncryptionContextProvider.class.getCanonicalName());
    configuration.unset(FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY + "."
        + getAccountName());
    configuration.unset(FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY_SHA + "."
        + getAccountName());
    return (AzureBlobFileSystem) FileSystem.newInstance(configuration);
  }

  private AzureBlobFileSystem getCPKEnabledFS() throws IOException {
    Configuration conf = getRawConfiguration();
    String cpkEncoded = EncodingHelper.getBase64EncodedString(cpk);
    String cpkEncodedSHA = EncodingHelper.getBase64EncodedString(
        EncodingHelper.getSHA256Hash(cpk));
    conf.set(FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY + "."
        + getAccountName(), cpkEncoded);
    conf.set(FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY_SHA + "."
        + getAccountName(), cpkEncodedSHA);
    conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private AzureBlobFileSystem getFS() throws Exception {
    if (getFileSystem().getAbfsClient().getEncryptionType() == requestEncryptionType) {
      return getFileSystem();
    }
    if (requestEncryptionType == ENCRYPTION_CONTEXT) {
      return getECProviderEnabledFS();
    } else if (requestEncryptionType == GLOBAL_KEY) {
      return getCPKEnabledFS();
    } else {
      Configuration conf = getRawConfiguration();
      conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
      return (AzureBlobFileSystem) FileSystem.newInstance(conf);
    }
  }

  private EncryptionContextProvider createEncryptedFile(Path testPath) throws Exception {
    AzureBlobFileSystem fs;
    if (getFileSystem().getAbfsClient().getEncryptionType() == fileEncryptionType) {
      fs = getFileSystem();
    } else {
      fs = fileEncryptionType == ENCRYPTION_CONTEXT
          ? getECProviderEnabledFS()
          : getCPKEnabledFS();
    }
    String relativePath = fs.getAbfsStore().getRelativePath(testPath);
    try (FSDataOutputStream out = fs.create(new Path(relativePath))) {
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
