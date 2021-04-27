/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 ("License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode;
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_REQUEST_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_SERVER_ENCRYPTED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_CPK_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;

public class ITestCustomerProvidedKey extends AbstractAbfsIntegrationTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(ITestCustomerProvidedKey.class);

  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int INT_512 = 512;
  private static final int INT_50 = 50;
  private static final int ENCRYPTION_KEY_LEN = 32;
  private static final int FILE_SIZE = 10 * ONE_MB;
  private static final int FILE_SIZE_FOR_COPY_BETWEEN_ACCOUNTS = 24 * ONE_MB;

  public ITestCustomerProvidedKey() throws Exception {
    boolean isCPKTestsEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_CPK_ENABLED, false);
    Assume.assumeTrue(isCPKTestsEnabled);
  }

  @Test
  public void testReadWithCPK() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    String fileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, fileName, FILE_SIZE);

    AbfsClient abfsClient = fs.getAbfsClient();
    int length = FILE_SIZE;
    byte[] buffer = new byte[length];
    final AbfsRestOperation op = abfsClient.getPathStatus(fileName, false);
    final String eTag = op.getResult()
        .getResponseHeader(HttpHeaderConfigurations.ETAG);
    AbfsRestOperation abfsRestOperation = abfsClient
        .read(fileName, 0, buffer, 0, length, eTag, null);
    assertCPKHeaders(abfsRestOperation, true);
    assertResponseHeader(abfsRestOperation, true, X_MS_ENCRYPTION_KEY_SHA256,
        getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeader(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");

    //  Trying to read with different CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    try (AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
         FSDataInputStream iStream = fs2.open(new Path(fileName))) {
      int len = 8 * ONE_MB;
      byte[] b = new byte[len];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(b, 0, len);
      });
    }

    //  Trying to read with no CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    try (AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem
        .get(conf); FSDataInputStream iStream = fs3.open(new Path(fileName))) {
      int len = 8 * ONE_MB;
      byte[] b = new byte[len];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(b, 0, len);
      });
    }
  }

  @Test
  public void testReadWithoutCPK() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(false);
    String fileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, fileName, FILE_SIZE);

    AbfsClient abfsClient = fs.getAbfsClient();
    int length = INT_512;
    byte[] buffer = new byte[length * 4];
    final AbfsRestOperation op = abfsClient.getPathStatus(fileName, false);
    final String eTag = op.getResult()
        .getResponseHeader(HttpHeaderConfigurations.ETAG);
    AbfsRestOperation abfsRestOperation = abfsClient
        .read(fileName, 0, buffer, 0, length, eTag, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertResponseHeader(abfsRestOperation, false, X_MS_ENCRYPTION_KEY_SHA256,
        getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeader(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");

    //  Trying to read with CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "12345678901234567890123456789012");

    try (AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
         AbfsClient abfsClient2 = fs2.getAbfsClient()) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        abfsClient2.read(fileName, 0, buffer, 0, length, eTag, null);
      });
    }
  }

  @Test
  public void testAppendWithCPK() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    final String fileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, fileName, FILE_SIZE);

    //  Trying to append with correct CPK headers
    AppendRequestParameters appendRequestParameters =
        new AppendRequestParameters(
        0, 0, 5, Mode.APPEND_MODE, false, null);
    byte[] buffer = getRandomBytesArray(5);
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .append(fileName, buffer, appendRequestParameters, null);
    assertCPKHeaders(abfsRestOperation, true);
    assertResponseHeader(abfsRestOperation, true, X_MS_ENCRYPTION_KEY_SHA256,
        getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, true, X_MS_REQUEST_SERVER_ENCRYPTED,
        "true");

    //  Trying to append with different CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    try (AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
         AbfsClient abfsClient2 = fs2.getAbfsClient()) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        abfsClient2.append(fileName, buffer, appendRequestParameters, null);
      });
    }

    //  Trying to append with no CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    try (AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem
        .get(conf); AbfsClient abfsClient3 = fs3.getAbfsClient()) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        abfsClient3.append(fileName, buffer, appendRequestParameters, null);
      });
    }
  }

  @Test
  public void testAppendWithoutCPK() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(false);
    final String fileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, fileName, FILE_SIZE);

    //  Trying to append without CPK headers
    AppendRequestParameters appendRequestParameters =
        new AppendRequestParameters(
        0, 0, 5, Mode.APPEND_MODE, false, null);
    byte[] buffer = getRandomBytesArray(5);
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .append(fileName, buffer, appendRequestParameters, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertResponseHeader(abfsRestOperation, false, X_MS_ENCRYPTION_KEY_SHA256,
        "");
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, true, X_MS_REQUEST_SERVER_ENCRYPTED,
        "true");

    //  Trying to append with CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "12345678901234567890123456789012");
    try (AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
         AbfsClient abfsClient2 = fs2.getAbfsClient()) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        abfsClient2.append(fileName, buffer, appendRequestParameters, null);
      });
    }
  }

  @Test
  public void testSetGetXAttr() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    String fileName = methodName.getMethodName();
    createFileAndGetContent(fs, fileName, FILE_SIZE);

    String valSent = "testValue";
    String attrName = "testXAttr";

    //  set get and verify
    fs.setXAttr(new Path(fileName), attrName,
        valSent.getBytes(StandardCharsets.UTF_8),
        EnumSet.of(XAttrSetFlag.CREATE));
    byte[] valBytes = fs.getXAttr(new Path(fileName), attrName);
    String valRecieved = new String(valBytes);
    assertEquals(valSent, valRecieved);

    //  set new value get and verify
    valSent = "new value";
    fs.setXAttr(new Path(fileName), attrName,
        valSent.getBytes(StandardCharsets.UTF_8),
        EnumSet.of(XAttrSetFlag.REPLACE));
    valBytes = fs.getXAttr(new Path(fileName), attrName);
    valRecieved = new String(valBytes);
    assertEquals(valSent, valRecieved);

    //  Read without CPK header
    LambdaTestUtils.intercept(IOException.class, () -> {
      getAbfs(false).getXAttr(new Path(fileName), attrName);
    });

    //  Wrong CPK
    LambdaTestUtils.intercept(IOException.class, () -> {
      getSameFSWithWrongCPK(fs).getXAttr(new Path(fileName), attrName);
    });
  }

  @Test
  public void testCopyBetweenAccounts() throws Exception {
    String accountName = getRawConfiguration()
        .get(FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT);
    String accountKey = getRawConfiguration()
        .get(FS_AZURE_TEST_CPK_ENABLED_SECONDARY_ACCOUNT_KEY);
    Assume.assumeTrue(accountName != null && !accountName.isEmpty());
    Assume.assumeTrue(accountKey != null && !accountKey.isEmpty());
    String fileSystemName = "cpkfs";

    //  Create fs1 and a file with CPK
    AzureBlobFileSystem fs1 = getAbfs(true);
    int fileSize = FILE_SIZE_FOR_COPY_BETWEEN_ACCOUNTS;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs1, "fs1-file.txt", fileContent);

    //  Create fs2 with different CPK
    Configuration conf = new Configuration();
    conf.addResource(TEST_CONFIGURATION_FILE_NAME);
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    conf.unset(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_ABFS_ACCOUNT_NAME, accountName);
    conf.set(FS_AZURE_ACCOUNT_KEY + "." + accountName, accountKey);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "123456789012345678901234567890ab");
    conf.set("fs.defaultFS", "abfs://" + fileSystemName + "@" + accountName);
    AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);

    //  Read from fs1 and write to fs2, fs1 and fs2 are having different CPK
    Path fs2DestFilePath = new Path("fs2-dest-file.txt");
    FSDataOutputStream ops = fs2.create(fs2DestFilePath);
    try (FSDataInputStream iStream = fs1.open(testFilePath)) {
      long totalBytesRead = 0;
      do {
        int length = 8 * ONE_MB;
        byte[] buffer = new byte[length];
        int bytesRead = iStream.read(buffer, 0, length);
        totalBytesRead += bytesRead;
        ops.write(buffer);
      } while (totalBytesRead < fileContent.length);
      ops.close();
    }

    //  Trying to read fs2DestFilePath with different CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    try (AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem
        .get(conf); FSDataInputStream iStream = fs3.open(fs2DestFilePath)) {
      int length = 8 * ONE_MB;
      byte[] buffer = new byte[length];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(buffer, 0, length);
      });
    }

    //  Trying to read fs2DestFilePath with no CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    try (AzureBlobFileSystem fs4 = (AzureBlobFileSystem) FileSystem
        .get(conf); FSDataInputStream iStream = fs4.open(fs2DestFilePath)) {
      int length = 8 * ONE_MB;
      byte[] buffer = new byte[length];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(buffer, 0, length);
      });
    }

    //  Read fs2DestFilePath and verify the content with the initial random
    //  bytes created and wrote into the source file at fs1
    try (FSDataInputStream iStream = fs2.open(fs2DestFilePath)) {
      long totalBytesRead = 0;
      int pos = 0;
      do {
        int length = 8 * ONE_MB;
        byte[] buffer = new byte[length];
        int bytesRead = iStream.read(buffer, 0, length);
        totalBytesRead += bytesRead;
        for (int i = 0; i < bytesRead; i++) {
          assertEquals(fileContent[pos + i], buffer[i]);
        }
        pos = pos + bytesRead;
      } while (totalBytesRead < fileContent.length);
    }
  }

  @Test
  public void testListPathWithCPK() throws Exception {
    testListPath(true);
  }

  @Test
  public void testListPathWithoutCPK() throws Exception {
    testListPath(false);
  }

  private void testListPath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    String testDirName = "/" + methodName.getMethodName();
    final Path testPath = new Path(testDirName);
    fs.mkdirs(testPath);
    createFileAndGetContent(fs, testDirName + "/aaa", FILE_SIZE);
    createFileAndGetContent(fs, testDirName + "/bbb", FILE_SIZE);
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .listPath(testDirName, false, INT_50, null);
    assertListstatus(fs, abfsRestOperation, testPath);

    //  Trying with different CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
    AbfsClient abfsClient2 = fs2.getAbfsClient();
    abfsRestOperation = abfsClient2.listPath(testDirName, false, INT_50, null);
    assertListstatus(fs, abfsRestOperation, testPath);

    if (isWithCPK) {
      //  Trying with no CPK headers
      conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
      AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem.get(conf);
      AbfsClient abfsClient3 = fs3.getAbfsClient();
      abfsRestOperation = abfsClient3
          .listPath(testDirName, false, INT_50, null);
      assertListstatus(fs, abfsRestOperation, testPath);
    }
  }

  private void assertListstatus(AzureBlobFileSystem fs,
      AbfsRestOperation abfsRestOperation, Path testPath) throws IOException {
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    FileStatus[] listStatuses = fs.listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);

    listStatuses = getSameFSWithWrongCPK(fs).listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);
  }

  @Test
  public void testCreatePathWithCPK() throws Exception {
    testCreatePath(true);
  }

  @Test
  public void testCreatePathWithoutCPK() throws Exception {
    testCreatePath(false);
  }

  private void testCreatePath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, testFileName, FILE_SIZE);

    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    boolean isNamespaceEnabled = fs.getIsNamespaceEnabled();
    AbfsRestOperation abfsRestOperation = abfsClient
        .createPath(testFileName, true, true,
            isNamespaceEnabled ? getOctalNotation(permission) : null,
            isNamespaceEnabled ? getOctalNotation(umask) : null, false, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeader(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, true, X_MS_REQUEST_SERVER_ENCRYPTED,
        "true");

    FileStatus[] listStatuses = fs.listStatus(new Path(testFileName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);

    listStatuses = getSameFSWithWrongCPK(fs).listStatus(new Path(testFileName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);
  }

  @Test
  public void testRenamePathWithCPK() throws Exception {
    testRenamePath(true);
  }

  @Test
  public void testRenamePathWithoutCPK() throws Exception {
    testRenamePath(false);
  }

  private void testRenamePath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, testFileName, FILE_SIZE);

    FileStatus fileStatusBeforeRename = fs
        .getFileStatus(new Path(testFileName));

    String newName = "/newName";
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .renamePath(testFileName, newName, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    LambdaTestUtils.intercept(FileNotFoundException.class,
        (() -> fs.getFileStatus(new Path(testFileName))));

    FileStatus fileStatusAfterRename = fs.getFileStatus(new Path(newName));
    Assertions.assertThat(fileStatusAfterRename.getLen())
        .describedAs("File size has to be same before and after rename")
        .isEqualTo(fileStatusBeforeRename.getLen());
  }

  @Test
  public void testFlushWithCPK() throws Exception {
    testFlush(true);
  }

  @Test
  public void testFlushWithoutCPK() throws Exception {
    testFlush(false);
  }

  private void testFlush(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    String expectedCPKSha = getCPKSha(fs);

    byte[] fileContent = getRandomBytesArray(FILE_SIZE);
    Path testFilePath = new Path(testFileName + "1");
    FSDataOutputStream oStream = fs.create(testFilePath);
    oStream.write(fileContent);

    //  Trying to read with different CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    try (AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.newInstance(conf);
         AbfsClient abfsClient2 = fs2.getAbfsClient()) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        abfsClient2.flush(testFileName, 0, false, false, null, null);
      });
    }

    //  Trying to read with no CPK headers
    if (isWithCPK) {
      conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
      try (AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem
          .get(conf); AbfsClient abfsClient3 = fs3.getAbfsClient()) {
        LambdaTestUtils.intercept(IOException.class, () -> {
          abfsClient3.flush(testFileName, 0, false, false, null, null);
        });
      }
    }

    //  With correct CPK
    AbfsRestOperation abfsRestOperation = abfsClient
        .flush(testFileName, 0, false, false, null, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeader(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, expectedCPKSha);
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, true, X_MS_REQUEST_SERVER_ENCRYPTED,
        isWithCPK + "");
  }

  @Test
  public void testSetPathPropertiesWithCPK() throws Exception {
    testSetPathProperties(true);
  }

  @Test
  public void testSetPathPropertiesWithoutCPK() throws Exception {
    testSetPathProperties(false);
  }

  private void testSetPathProperties(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, testFileName, FILE_SIZE);

    AbfsClient abfsClient = fs.getAbfsClient();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "val");
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPathProperties(testFileName,
            convertXmsPropertiesToCommaSeparatedString(properties));
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeader(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, true, X_MS_REQUEST_SERVER_ENCRYPTED,
        "true");
  }

  @Test
  public void testGetPathStatusFileWithCPK() throws Exception {
    testGetPathStatusFile(true);
  }

  @Test
  public void testGetPathStatusFileWithoutCPK() throws Exception {
    testGetPathStatusFile(false);
  }

  private void testGetPathStatusFile(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, testFileName, FILE_SIZE);

    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .getPathStatus(testFileName, false);
    assertCPKHeaders(abfsRestOperation, false);
    assertResponseHeader(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeader(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");

    abfsRestOperation = abfsClient.getPathStatus(testFileName, true);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeader(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeader(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeader(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");
  }

  @Test
  public void testDeletePathWithCPK() throws Exception {
    testDeletePath(false);
  }

  @Test
  public void testDeletePathWithoutCPK() throws Exception {
    testDeletePath(false);
  }

  private void testDeletePath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    createFileAndGetContent(fs, testFileName, FILE_SIZE);

    FileStatus[] listStatuses = fs.listStatus(new Path(testFileName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);

    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .deletePath(testFileName, false, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    Assertions.assertThatThrownBy(() -> fs.listStatus(new Path(testFileName)))
        .isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void testSetPermissionWithCPK() throws Exception {
    testSetPermission(true);
  }

  @Test
  public void testSetPermissionWithoutCPK() throws Exception {
    testSetPermission(false);
  }

  private void testSetPermission(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    createFileAndGetContent(fs, testFileName, FILE_SIZE);
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPermission(testFileName, permission.toString());
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Test
  public void testSetAclWithCPK() throws Exception {
    testSetAcl(true);
  }

  @Test
  public void testSetAclWithoutCPK() throws Exception {
    testSetAcl(false);
  }

  private void testSetAcl(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    createFileAndGetContent(fs, testFileName, FILE_SIZE);
    AbfsClient abfsClient = fs.getAbfsClient();

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(ACCESS, USER, ALL));
    final Map<String, String> aclEntries = AbfsAclHelper
        .deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));

    AbfsRestOperation abfsRestOperation = abfsClient
        .setAcl(testFileName, AbfsAclHelper.serializeAclSpec(aclEntries));
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Test
  public void testGetAclWithCPK() throws Exception {
    testGetAcl(true);
  }

  @Test
  public void testGetAclWithoutCPK() throws Exception {
    testGetAcl(false);
  }

  private void testGetAcl(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    createFileAndGetContent(fs, testFileName, FILE_SIZE);
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getAclStatus(testFileName);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Test
  public void testCheckAccessWithCPK() throws Exception {
    testCheckAccess(true);
  }

  @Test
  public void testCheckAccessWithoutCPK() throws Exception {
    testCheckAccess(false);
  }

  private void testCheckAccess(final boolean isWithCPK) throws Exception {
    boolean isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        isHNSEnabled);
    Assume.assumeTrue("AuthType has to be OAuth",
        getAuthType() == AuthType.OAuth);

    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .checkAccess(testFileName, "rwx");
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  private byte[] createFileAndGetContent(AzureBlobFileSystem fs,
      String fileName, int fileSize) throws IOException {
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    ContractTestUtils.verifyFileContents(fs, testFilePath, fileContent);
    return fileContent;
  }

  private void assertCPKHeaders(AbfsRestOperation abfsRestOperation,
      boolean isCPKHeaderExpected) {
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_KEY, isCPKHeaderExpected);
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_KEY_SHA256,
        isCPKHeaderExpected);
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_ALGORITHM,
        isCPKHeaderExpected);
  }

  private void assertNoCPKResponseHeadersPresent(
      AbfsRestOperation abfsRestOperation) {
    assertResponseHeader(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");
    assertResponseHeader(abfsRestOperation, false, X_MS_ENCRYPTION_KEY_SHA256,
        "");
  }

  private void assertResponseHeader(AbfsRestOperation abfsRestOperation,
      boolean isHeaderExpected, String headerName, String expectedValue) {
    final AbfsHttpOperation result = abfsRestOperation.getResult();
    final String value = result.getResponseHeader(headerName);
    if (isHeaderExpected) {
      Assertions.assertThat(value).isEqualTo(expectedValue);
    } else {
      Assertions.assertThat(value).isNull();
    }
  }

  private void assertHeader(AbfsRestOperation abfsRestOperation,
      String headerName, boolean isCPKHeaderExpected) {
    assertTrue(abfsRestOperation != null);
    Optional<AbfsHttpHeader> header = abfsRestOperation.getRequestHeaders()
        .stream().filter(abfsHttpHeader -> abfsHttpHeader.getName()
            .equalsIgnoreCase(headerName)).findFirst();
    String desc;
    if (isCPKHeaderExpected) {
      desc =
          "CPK header " + headerName + " is expected, but the same is absent.";
    } else {
      desc = "CPK header " + headerName
          + " is not expected, but the same is present.";
    }
    Assertions.assertThat(header.isPresent()).describedAs(desc)
        .isEqualTo(isCPKHeaderExpected);
  }

  private byte[] getSHA256Hash(String key) throws IOException {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  private String getCPKSha(final AzureBlobFileSystem abfs) throws IOException {
    Configuration conf = abfs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    String encryptionKey = conf
        .get(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    if (encryptionKey == null || encryptionKey.isEmpty()) {
      return "";
    }
    return getBase64EncodedString(getSHA256Hash(encryptionKey));
  }

  private String getBase64EncodedString(byte[] bytes) {
    return java.util.Base64.getEncoder().encodeToString(bytes);
  }

  private Path createFileWithContent(FileSystem fs, String fileName,
      byte[] fileContent) throws IOException {
    Path testFilePath = new Path(fileName);
    try (FSDataOutputStream oStream = fs.create(testFilePath)) {
      oStream.write(fileContent);
      oStream.flush();
    }
    return testFilePath;
  }

  private String convertXmsPropertiesToCommaSeparatedString(
      final Hashtable<String, String> properties)
      throws CharacterCodingException {
    StringBuilder commaSeparatedProperties = new StringBuilder();
    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING)
        .newEncoder();
    for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
      String key = propertyEntry.getKey();
      String value = propertyEntry.getValue();
      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        throw new CharacterCodingException();
      }
      String encodedPropertyValue = Base64
          .encode(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties.append(key).append(AbfsHttpConstants.EQUAL)
          .append(encodedPropertyValue);
      commaSeparatedProperties.append(AbfsHttpConstants.COMMA);
    }
    if (commaSeparatedProperties.length() != 0) {
      commaSeparatedProperties
          .deleteCharAt(commaSeparatedProperties.length() - 1);
    }
    return commaSeparatedProperties.toString();
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String
        .format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private AzureBlobFileSystem getAbfs(boolean withCPK) throws IOException {
    return getAbfs(withCPK, "12345678901234567890123456789012");
  }

  private AzureBlobFileSystem getAbfs(boolean withCPK, String cpk)
      throws IOException {
    Configuration conf = getRawConfiguration();
    if (withCPK) {
      conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName(),
          cpk);
    } else {
      conf.unset(
          FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName());
    }
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private AzureBlobFileSystem getSameFSWithWrongCPK(
      final AzureBlobFileSystem fs) throws IOException {
    AbfsConfiguration abfsConf = fs.getAbfsStore().getAbfsConfiguration();
    Configuration conf = abfsConf.getRawConfiguration();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    String cpk = conf
        .get(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    if (cpk == null || cpk.isEmpty()) {
      cpk = "01234567890123456789012345678912";
    }
    cpk = "different-" + cpk;
    String differentCpk = cpk.substring(0, ENCRYPTION_KEY_LEN - 1);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        differentCpk);
    conf.set("fs.defaultFS",
        "abfs://" + getFileSystemName() + "@" + accountName);
    AzureBlobFileSystem sameFSWithDifferentCPK =
        (AzureBlobFileSystem) FileSystem.newInstance(conf);
    return sameFSWithDifferentCPK;
  }

}
