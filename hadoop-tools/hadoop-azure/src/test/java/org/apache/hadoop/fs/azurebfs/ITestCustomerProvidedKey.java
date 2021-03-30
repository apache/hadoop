/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.*;
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

  public ITestCustomerProvidedKey() throws Exception {
  }

  @Ignore
  @Test
  public void testWriteReadAndVerifyWithCPK() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    int fileSize = 8 * ONE_MB;
    byte[] fileContent = getRandomBytesArray(fileSize);
    String fileName = methodName.getMethodName();
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      byte[] buffer = new byte[fileSize];
      int bytesRead = iStream.read(buffer, 0, fileSize);
      assertEquals(bytesRead, fileSize);
      for (int i = 0; i < fileSize; i++) {
        assertEquals(fileContent[i], buffer[i]);
      }
    }

    //  Trying to read fs2DestFilePath with different CPK headers
    Configuration conf = fs.getConf();
    String accountName = conf.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    //  Trying to read fs2DestFilePath with different CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "different-1234567890123456789012");
    conf.set("fs.abfs.impl.disable.cache", "true");
    AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.get(conf);
    try (FSDataInputStream iStream = fs2.open(testFilePath)) {
      int length = 8 * ONE_MB;
      byte[] buffer = new byte[length];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(buffer, 0, length);
      });
    }

    //  Trying to read fs2DestFilePath with no CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem.get(conf);
    try (FSDataInputStream iStream = fs3.open(testFilePath)) {
      int length = 8 * ONE_MB;
      byte[] buffer = new byte[length];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(buffer, 0, length);
      });
    }
  }

  @Ignore
  @Test
  public void testSetGetXAttr() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    String fileName = methodName.getMethodName();
    fs.create(new Path(fileName));

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

  @Ignore
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
    int fileSize = 24 * ONE_MB;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs1, "fs1-file.txt", fileContent);

    //  Create fs2 with different CPK
    Configuration conf = new Configuration();
    conf.set("fs.abfs.impl.disable.cache", "true");
    conf.addResource(TEST_CONFIGURATION_FILE_NAME);
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    conf.unset(FS_AZURE_ABFS_ACCOUNT_NAME);
    conf.set(FS_AZURE_ABFS_ACCOUNT_NAME, accountName);
    conf.set(FS_AZURE_ACCOUNT_KEY + "." + accountName, accountKey);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        "123456789012345678901234567890ab");
    conf.set("fs.defaultFS", "abfs://" + fileSystemName + "@" + accountName);
    AzureBlobFileSystem fs2 = (AzureBlobFileSystem) FileSystem.get(conf);

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
    AzureBlobFileSystem fs3 = (AzureBlobFileSystem) FileSystem.get(conf);
    try (FSDataInputStream iStream = fs3.open(fs2DestFilePath)) {
      int length = 8 * ONE_MB;
      byte[] buffer = new byte[length];
      LambdaTestUtils.intercept(IOException.class, () -> {
        iStream.read(buffer, 0, length);
      });
    }

    //  Trying to read fs2DestFilePath with no CPK headers
    conf.unset(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName);
    AzureBlobFileSystem fs4 = (AzureBlobFileSystem) FileSystem.get(conf);
    try (FSDataInputStream iStream = fs4.open(fs2DestFilePath)) {
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

  @Ignore
  @Test
  public void testAppendWithCPK() throws Exception {
    testAppend(true);
  }

  @Ignore
  @Test
  public void testAppendWithoutCPK() throws Exception {
    testAppend(false);
  }

  private void testAppend(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AppendRequestParameters appendRequestParameters =
        new AppendRequestParameters(
        0, 0, 5, Mode.APPEND_MODE, false);
    byte[] buffer = getRandomBytesArray(5);
    AbfsRestOperation abfsRestOperation = abfsClient
        .append(testFileName, buffer, appendRequestParameters, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeaders(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeaders(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, true,
        X_MS_REQUEST_SERVER_ENCRYPTED, "true");
  }

  @Ignore
  @Test
  public void testListPathWithCPK() throws Exception {
    testListPath(true);
  }

  @Ignore
  @Test
  public void testListPathWithoutCPK() throws Exception {
    testListPath(false);
  }

  private void testListPath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    String testDirName = "/" + methodName.getMethodName();
    final Path testPath = new Path(testDirName);
    fs.mkdirs(testPath);
    fs.mkdirs(new Path(testDirName + "/aaa"));
    fs.mkdirs(new Path(testDirName + "/bbb"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .listPath(testDirName, false, INT_50, null);

    //  assert cpk headers are not added
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    FileStatus[] listStatuses = fs.listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);

    listStatuses = getSameFSWithWrongCPK(fs).listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);
  }

  @Ignore
  @Test
  public void testGetFileSystemPropertiesWithCPK() throws Exception {
    testGetFileSystemProperties(true);
  }

  @Ignore
  @Test
  public void testGetFileSystemPropertiesWithoutCPK() throws Exception {
    testGetFileSystemProperties(false);
  }

  private void testGetFileSystemProperties(final boolean isWithCPK)
      throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getFilesystemProperties();

    //  assert cpk headers are not added
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Ignore
  @Test
  public void testDeleteFileSystemWithCPK() throws Exception {
    testDeleteFileSystem(true);
  }

  @Ignore
  @Test
  public void testDeleteFileSystemWithoutCPK() throws Exception {
    testDeleteFileSystem(false);
  }

  private void testDeleteFileSystem(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.deleteFilesystem();
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Ignore
  @Test
  public void testCreatePathWithCPK() throws Exception {
    testCreatePath(true);
  }

  @Ignore
  @Test
  public void testCreatePathWithoutCPK() throws Exception {
    testCreatePath(false);
  }

  private void testCreatePath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
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
    assertResponseHeaders(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeaders(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, true,
        X_MS_REQUEST_SERVER_ENCRYPTED, "true");

    FileStatus[] listStatuses = fs.listStatus(new Path(testFileName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);

    listStatuses = getSameFSWithWrongCPK(fs).listStatus(new Path(testFileName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);
  }

  @Ignore
  @Test
  public void testRenamePathWithCPK() throws Exception {
    testRenamePath(true);
  }

  @Ignore
  @Test
  public void testRenamePathWithoutCPK() throws Exception {
    testRenamePath(false);
  }

  private void testRenamePath(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Path testPath = new Path(testFileName);
    fs.create(testPath);

    FileStatus[] listStatuses = fs.listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);

    String newName = "/newName";
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .renamePath(testFileName, newName, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    Assertions.assertThatThrownBy(() -> fs.listStatus(testPath))
        .isInstanceOf(FileNotFoundException.class);

    listStatuses = fs.listStatus(new Path(newName));
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);
  }

  @Ignore
  @Test
  public void testFlushWithCPK() throws Exception {
    testFlush(true);
  }

  @Ignore
  @Test
  public void testFlushWithoutCPK() throws Exception {
    testFlush(false);
  }

  private void testFlush(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .flush(testFileName, 0, false, false, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeaders(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeaders(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, true,
        X_MS_REQUEST_SERVER_ENCRYPTED, isWithCPK + "");
  }

  @Ignore
  @Test
  public void testSetPathPropertiesWithCPK() throws Exception {
    testSetPathProperties(true);
  }

  @Ignore
  @Test
  public void testSetPathPropertiesWithoutCPK() throws Exception {
    testSetPathProperties(false);
  }

  private void testSetPathProperties(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "val");
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPathProperties(testFileName,
            convertXmsPropertiesToCommaSeparatedString(properties));
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeaders(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeaders(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, true,
        X_MS_REQUEST_SERVER_ENCRYPTED, "true");
  }

  @Ignore
  @Test
  public void testGetPathStatusWithCPK() throws Exception {
    testGetPathStatus(true);
  }

  @Ignore
  @Test
  public void testGetPathStatusWithoutCPK() throws Exception {
    testGetPathStatus(false);
  }

  private void testGetPathStatus(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Path testPath = new Path(testFileName);
    fs.mkdirs(testPath);
    fs.mkdirs(new Path(testFileName + "/aaa"));
    fs.mkdirs(new Path(testFileName + "/bbb"));

    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .getPathStatus(testFileName, false);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeaders(abfsRestOperation, false, X_MS_ENCRYPTION_KEY_SHA256,
        "");
    assertResponseHeaders(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeaders(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");

    FileStatus[] listStatuses = fs.listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);

    listStatuses = getSameFSWithWrongCPK(fs).listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 2 entries").isEqualTo(2);
  }

  @Ignore
  @Test
  public void testReadWithCPK() throws Exception {
    testRead(true);
  }

  @Ignore
  @Test
  public void testReadWithoutCPK() throws Exception {
    testRead(false);
  }

  private void testRead(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    int fileSize = ONE_MB;
    byte[] fileContent = getRandomBytesArray(fileSize);
    createFileWithContent(fs, testFileName, fileContent);
    AbfsClient abfsClient = fs.getAbfsClient();
    int length = INT_512;
    byte[] buffer = new byte[length * 4];
    final AbfsRestOperation op = abfsClient.getPathStatus(testFileName, false);
    final String eTag = op.getResult()
        .getResponseHeader(HttpHeaderConfigurations.ETAG);
    AbfsRestOperation abfsRestOperation = abfsClient
        .read(testFileName, 0, buffer, 0, length, eTag, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
    assertResponseHeaders(abfsRestOperation, isWithCPK,
        X_MS_ENCRYPTION_KEY_SHA256, getCPKSha(fs));
    assertResponseHeaders(abfsRestOperation, true, X_MS_SERVER_ENCRYPTED,
        "true");
    assertResponseHeaders(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");
  }

  @Ignore
  @Test
  public void testDeletePathWithCPK() throws Exception {
    testDeletePathWithoutCPK(false);
  }

  @Ignore
  @Test
  public void testDeletePathWithoutCPK() throws Exception {
    testDeletePathWithoutCPK(false);
  }

  private void testDeletePathWithoutCPK(final boolean isWithCPK)
      throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    final Path testPath = new Path(testFileName);
    fs.create(testPath);

    FileStatus[] listStatuses = fs.listStatus(testPath);
    Assertions.assertThat(listStatuses.length)
        .describedAs("listStatuses should have 1 entry").isEqualTo(1);

    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .deletePath(testFileName, false, null);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);

    Assertions.assertThatThrownBy(() -> fs.listStatus(testPath))
        .isInstanceOf(FileNotFoundException.class);
  }

  @Ignore
  @Test
  public void testSetPermissionWithCPK() throws Exception {
    testSetPermission(true);
  }

  @Ignore
  @Test
  public void testSetPermissionWithoutCPK() throws Exception {
    testSetPermission(false);
  }

  private void testSetPermission(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPermission(testFileName, permission.toString());
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Ignore
  @Test
  public void testSetAclWithCPK() throws Exception {
    testSetAcl(true);
  }

  @Ignore
  @Test
  public void testSetAclWithoutCPK() throws Exception {
    testSetAcl(false);
  }

  private void testSetAcl(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(ACCESS, USER, ALL));
    final Map<String, String> aclEntries = AbfsAclHelper
        .deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));

    AbfsRestOperation abfsRestOperation = abfsClient
        .setAcl(testFileName, AbfsAclHelper.serializeAclSpec(aclEntries));
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Ignore
  @Test
  public void testGetAclWithCPK() throws Exception {
    testGetAcl(true);
  }

  @Ignore
  @Test
  public void testGetAclWithoutCPK() throws Exception {
    testGetAcl(false);
  }

  private void testGetAcl(final boolean isWithCPK) throws Exception {
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    final String testFileName = "/" + methodName.getMethodName();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path(testFileName));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getAclStatus(testFileName);
    assertCPKHeaders(abfsRestOperation, false);
    assertNoCPKResponseHeadersPresent(abfsRestOperation);
  }

  @Ignore
  @Test
  public void testCheckAccessWithCPK() throws Exception {
    testCheckAccess(true);
  }

  @Ignore
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
    assertResponseHeaders(abfsRestOperation, false, X_MS_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, false,
        X_MS_REQUEST_SERVER_ENCRYPTED, "");
    assertResponseHeaders(abfsRestOperation, false, X_MS_ENCRYPTION_KEY_SHA256,
        "");
  }

  private void assertResponseHeaders(AbfsRestOperation abfsRestOperation,
      boolean isHeaderExpected, String headerName, String expectedValue) {
    boolean isHeaderFound = false;
    String key, val = null;
    for (AbfsHttpHeader respHeader : abfsRestOperation.getResponseHeaders()) {
      key = respHeader.getName();
      val = respHeader.getValue();
      if (key.equals(headerName)) {
        isHeaderFound = true;
        break;
      }
    }
    Assertions.assertThat(isHeaderFound).isEqualTo(isHeaderExpected);
    if (!isHeaderExpected) {
      return;
    }
    Assertions.assertThat(val).isEqualTo(expectedValue);
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
    Path testFilePath = path(fileName);
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
    String differentCpk = cpk.substring(0, 31);
    conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + accountName,
        differentCpk);
    conf.set("fs.defaultFS",
        "abfs://" + getFileSystemName() + "@" + accountName);
    conf.set("fs.abfs.impl.disable.cache", "true");
    AzureBlobFileSystem sameFSWithDifferentCPK =
        (AzureBlobFileSystem) FileSystem
        .get(conf);
    return sameFSWithDifferentCPK;
  }

}
