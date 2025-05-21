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
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_FILE_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemDelegationSAS extends AbstractAbfsIntegrationTest {
  private static final String TEST_GROUP = UUID.randomUUID().toString();

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAzureBlobFileSystemDelegationSAS.class);

  private boolean isHNSEnabled;

  public ITestAzureBlobFileSystemDelegationSAS() throws Exception {
    // These tests rely on specific settings in azure-auth-keys.xml:
    String sasProvider = getRawConfiguration().get(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    Assume.assumeTrue(MockDelegationSASTokenProvider.class.getCanonicalName().equals(sasProvider));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_ID));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID));
    Assume.assumeNotNull(getRawConfiguration().get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID));
    // The test uses shared key to create a random filesystem and then creates another
    // instance of this filesystem using SAS authorization.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    if (!isHNSEnabled) {
      assumeBlobServiceType();
    }
    createFilesystemForSASTests();
    super.setup();
  }

  @Test
  // Test filesystem operations access, create, mkdirs, setOwner, getFileStatus
  public void testCheckAccess() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();

    Path rootPath = new Path("/");
    fs.setOwner(rootPath, MockDelegationSASTokenProvider.TEST_OWNER, null);
    fs.setPermission(rootPath, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.EXECUTE));
    FileStatus rootStatus = fs.getFileStatus(rootPath);
    assertEquals("The directory permissions are not expected.", "rwxr-x--x", rootStatus.getPermission().toString());
    assertEquals("The directory owner is not expected.",
        MockDelegationSASTokenProvider.TEST_OWNER,
        rootStatus.getOwner());

    Path dirPath = new Path(UUID.randomUUID().toString());
    fs.mkdirs(dirPath);

    Path filePath = new Path(dirPath, "file1");
    fs.create(filePath).close();
    fs.setPermission(filePath, new FsPermission(FsAction.READ, FsAction.READ, FsAction.NONE));

    FileStatus dirStatus = fs.getFileStatus(dirPath);
    FileStatus fileStatus = fs.getFileStatus(filePath);

    assertEquals("The owner is not expected.", MockDelegationSASTokenProvider.TEST_OWNER, dirStatus.getOwner());
    assertEquals("The owner is not expected.", MockDelegationSASTokenProvider.TEST_OWNER, fileStatus.getOwner());
    assertEquals("The directory permissions are not expected.", "rwxr-xr-x", dirStatus.getPermission().toString());
    assertEquals("The file permissions are not expected.", "r--r-----", fileStatus.getPermission().toString());

    assertTrue(isAccessible(fs, dirPath, FsAction.READ_WRITE));
    assertFalse(isAccessible(fs, filePath, FsAction.READ_WRITE));

    fs.setPermission(filePath, new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE));
    fileStatus = fs.getFileStatus(filePath);
    assertEquals("The file permissions are not expected.", "rw-r-----", fileStatus.getPermission().toString());
    assertTrue(isAccessible(fs, filePath, FsAction.READ_WRITE));

    fs.setPermission(dirPath, new FsPermission(FsAction.EXECUTE, FsAction.NONE, FsAction.NONE));
    dirStatus = fs.getFileStatus(dirPath);
    assertEquals("The file permissions are not expected.", "--x------", dirStatus.getPermission().toString());
    assertFalse(isAccessible(fs, dirPath, FsAction.READ_WRITE));
    assertTrue(isAccessible(fs, dirPath, FsAction.EXECUTE));

    fs.setPermission(dirPath, new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));
    dirStatus = fs.getFileStatus(dirPath);
    assertEquals("The file permissions are not expected.", "---------", dirStatus.getPermission().toString());
    assertFalse(isAccessible(fs, filePath, FsAction.READ_WRITE));
  }

  private boolean isAccessible(FileSystem fs, Path path, FsAction fsAction)
      throws IOException {
    try {
      fs.access(path, fsAction);
    } catch (AccessControlException ace) {
      return false;
    }
    return true;
  }

  @Test
  // Test filesystem operations create, create with overwrite, append and open.
  // Test output stream operation write, flush and close
  // Test input stream operation, read
  public void testReadAndWrite() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path reqPath = new Path(UUID.randomUUID().toString());

    final String msg1 = "purple";
    final String msg2 = "yellow";
    int expectedFileLength = msg1.length() * 2;

    byte[] readBuffer = new byte[1024];

    // create file with content "purplepurple"
    try (FSDataOutputStream stream = fs.create(reqPath)) {
      stream.writeBytes(msg1);
      stream.hflush();
      stream.writeBytes(msg1);
    }

    // open file and verify content is "purplepurple"
    try (FSDataInputStream stream = fs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg1 + msg1, fileContent);
    }

    // overwrite file with content "yellowyellow"
    try (FSDataOutputStream stream = fs.create(reqPath)) {
      stream.writeBytes(msg2);
      stream.hflush();
      stream.writeBytes(msg2);
    }

    // open file and verify content is "yellowyellow"
    try (FSDataInputStream stream = fs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg2 + msg2, fileContent);
    }

    // append to file so final content is "yellowyellowpurplepurple"
    try (FSDataOutputStream stream = fs.append(reqPath)) {
      stream.writeBytes(msg1);
      stream.hflush();
      stream.writeBytes(msg1);
    }

    // open file and verify content is "yellowyellowpurplepurple"
    try (FSDataInputStream stream = fs.open(reqPath)) {
      int bytesRead = stream.read(readBuffer, 0, readBuffer.length);
      assertEquals(2 * expectedFileLength, bytesRead);
      String fileContent = new String(readBuffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals(msg2 + msg2 + msg1 + msg1, fileContent);
    }
  }

  @Test
  public void checkExceptionForRenameOverwrites() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = new Path("a/b/f1.txt");
    Path dest = new Path("a/b/f2.txt");
    touch(src);
    touch(dest);

    intercept(FileAlreadyExistsException.class, ERR_FILE_ALREADY_EXISTS, () -> fs.rename(src, dest));
  }

  @Test
  // Test rename file and rename folder
  public void testRename() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = new Path(UUID.randomUUID().toString());
    Path sourcePath = new Path(sourceDir, UUID.randomUUID().toString());
    Path destinationPath = new Path(sourceDir, UUID.randomUUID().toString());
    Path destinationDir = new Path(UUID.randomUUID().toString());

    // create file with content "hello"
    try (FSDataOutputStream stream = fs.create(sourcePath)) {
      stream.writeBytes("hello");
    }

    assertPathDoesNotExist(fs, "This path should not exist", destinationPath);
    fs.rename(sourcePath, destinationPath);
    assertPathDoesNotExist(fs, "This path should not exist", sourcePath);
    assertPathExists(fs, "This path should exist", destinationPath);

    assertPathDoesNotExist(fs, "This path should not exist", destinationDir);
    fs.rename(sourceDir, destinationDir);
    assertPathDoesNotExist(fs, "This path should not exist", sourceDir);
    assertPathExists(fs, "This path should exist", destinationDir);
  }

  @Test
  // Test delete file and delete folder
  public void testDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dirPath = new Path(UUID.randomUUID().toString());
    Path filePath = new Path(dirPath, UUID.randomUUID().toString());

    // create file with content "hello"
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.writeBytes("hello");
    }

    assertPathExists(fs, "This path should exist", filePath);
    fs.delete(filePath, false);
    assertPathDoesNotExist(fs, "This path should not exist", filePath);

    assertPathExists(fs, "This path should exist", dirPath);
    fs.delete(dirPath, false);
    assertPathDoesNotExist(fs, "This path should not exist", dirPath);
  }

  @Test
  // Test delete folder recursive
  public void testDeleteRecursive() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dirPath = new Path(UUID.randomUUID().toString());
    Path filePath = new Path(dirPath, UUID.randomUUID().toString());

    // create file with content "hello"
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.writeBytes("hello");
    }

    assertPathExists(fs, "This path should exist", dirPath);
    assertPathExists(fs, "This path should exist", filePath);
    fs.delete(dirPath, true);
    assertPathDoesNotExist(fs, "This path should not exist", filePath);
    assertPathDoesNotExist(fs, "This path should not exist", dirPath);
  }

  @Test
  // Test list on file, directory and root path
  public void testList() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path dirPath = new Path(UUID.randomUUID().toString());
    Path filePath = new Path(dirPath, UUID.randomUUID().toString());
    Path filePath2 = new Path(dirPath, UUID.randomUUID().toString());

    fs.mkdirs(dirPath);

    // create file with content "hello"
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.writeBytes("hello");
    }

    // create file with content "bye"
    try (FSDataOutputStream stream = fs.create(filePath2)) {
      stream.writeBytes("bye");
    }

    fs.listStatus(filePath);
    fs.listStatus(dirPath);
    fs.listStatus(new Path("/"));
  }

  @Test
  // Test filesystem operations setAcl, getAclStatus, removeAcl
  // setPermissions and getFileStatus
  public void testAcl() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();
    Path reqPath = new Path(UUID.randomUUID().toString());

    fs.create(reqPath).close();

    fs.setAcl(reqPath, Arrays
        .asList(aclEntry(ACCESS, GROUP, TEST_GROUP, FsAction.ALL)));

    AclStatus acl = fs.getAclStatus(reqPath);
    assertEquals(MockDelegationSASTokenProvider.TEST_OWNER, acl.getOwner());
    assertEquals("[group::r--, group:" + TEST_GROUP + ":rwx]", acl.getEntries().toString());

    fs.removeAcl(reqPath);
    acl = fs.getAclStatus(reqPath);
    assertEquals("[]", acl.getEntries().toString());

    fs.setPermission(reqPath,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));

    FileStatus status = fs.getFileStatus(reqPath);
    assertEquals("rwx------", status.getPermission().toString());

    acl = fs.getAclStatus(reqPath);
    assertEquals("rwx------", acl.getPermission().toString());
  }

  @Test
  // Test getFileStatus and getAclStatus operations on root path
  public void testRootPath() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();
    Path rootPath = new Path(AbfsHttpConstants.ROOT_PATH);

    fs.setOwner(rootPath, MockDelegationSASTokenProvider.TEST_OWNER, null);
    FileStatus status = fs.getFileStatus(rootPath);
    assertEquals("rwxr-x---", status.getPermission().toString());
    assertEquals(MockDelegationSASTokenProvider.TEST_OWNER, status.getOwner());
    assertTrue(status.isDirectory());

    AclStatus acl = fs.getAclStatus(rootPath);
    assertEquals("rwxr-x---", acl.getPermission().toString());

    List<AclEntry> aclSpec = new ArrayList<>();
    int count = 0;
    for (AclEntry entry: acl.getEntries()) {
      aclSpec.add(entry);
       if (entry.getScope() == AclEntryScope.DEFAULT) {
         count++;
       }
    }
    assertEquals(0, count);

    aclSpec.add(aclEntry(DEFAULT, USER, "cd548981-afec-4ab9-9d39-f6f2add2fd9b", FsAction.EXECUTE));

    fs.modifyAclEntries(rootPath, aclSpec);

    acl = fs.getAclStatus(rootPath);

    count = 0;
    for (AclEntry entry: acl.getEntries()) {
      aclSpec.add(entry);
      if (entry.getScope() == AclEntryScope.DEFAULT) {
        count++;
      }
    }
    assertEquals(5, count);

    fs.removeDefaultAcl(rootPath);

    acl = fs.getAclStatus(rootPath);

    count = 0;
    for (AclEntry entry: acl.getEntries()) {
      aclSpec.add(entry);
      if (entry.getScope() == AclEntryScope.DEFAULT) {
        count++;
      }
    }
    assertEquals(0, count);
  }

  @Test
  // Test filesystem operations getXAttr and setXAttr
  public void testProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path reqPath = new Path(UUID.randomUUID().toString());

    fs.create(reqPath).close();

    final String propertyName = "user.mime_type";
    final byte[] propertyValue = "text/plain".getBytes("utf-8");
    fs.setXAttr(reqPath, propertyName, propertyValue);

    assertArrayEquals(propertyValue, fs.getXAttr(reqPath, propertyName));
  }

  @Test
  // FileSystemProperties are not supported by delegation SAS and should throw exception
  public void testSetFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String>
        properties = new Hashtable<>();
    properties.put("FileSystemProperties", "true");
    TracingContext tracingContext = getTestTracingContext(fs, true);
    assertThrows(IOException.class, () -> fs.getAbfsStore()
        .setFilesystemProperties(properties, tracingContext));
    assertThrows(IOException.class,
        () -> fs.getAbfsStore().getFilesystemProperties(tracingContext));
  }

  @Test
  //Test list and delete operation on implicit paths
  public void testListAndDeleteImplicitPaths() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    AbfsBlobClient client = ((AbfsBlobClient) getFileSystem().getAbfsClient());

    Path file1 = new Path("/testDir/dir1/file1");
    Path file2 = new Path("/testDir/dir1/file2");
    Path implicitDir = file1.getParent();

    createAzCopyFolder(implicitDir);
    createAzCopyFile(file1);
    createAzCopyFile(file2);

    AbfsRestOperation op = client.listPath(
        implicitDir.toString(), false, 2, null,
        getTestTracingContext(getFileSystem(), false), null).getOp();
    List<? extends ListResultEntrySchema> list = op.getResult()
        .getListResultSchema()
        .paths();
    Assertions.assertThat(list).hasSize(2);

    client.deletePath(implicitDir.toString(), true, "",
        getTestTracingContext(fs, false));

    Assertions.assertThat(fs.exists(file1))
        .describedAs("Deleted file1 should not exist.").isFalse();
    Assertions.assertThat(fs.exists(file2))
        .describedAs("Deleted file2 should not exist.").isFalse();
    Assertions.assertThat(fs.exists(implicitDir))
        .describedAs("The parent dir should not exist.")
        .isFalse();
  }


  /**
   * Spies on the AzureBlobFileSystem's store and client to enable mocking and verification
   * of client interactions in tests. It replaces the actual store and client with mocked versions.
   *
   * @param fs the AzureBlobFileSystem instance
   * @return the spied AbfsClient for interaction verification
   */
  private AbfsClient addSpyHooksOnClient(final AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();
    return client;
  }

  /**
   * Asserts the signature masking in the URL and encoded URL of the AbfsRestOperation.
   *
   * @param op the AbfsRestOperation
   */
  private void checkSignatureMaskAssertions(AbfsRestOperation op) {
    AbfsHttpOperation result = op.getResult();
    String url = result.getMaskedUrl();
    String encodedUrl = result.getMaskedEncodedUrl();
    Assertions.assertThat(url.substring(url.indexOf("sig=")))
        .describedAs("Signature query param should be masked")
        .startsWith("sig=XXXXX");
    Assertions.assertThat(encodedUrl.substring(encodedUrl.indexOf("sig%3D")))
        .describedAs("Signature query param should be masked")
        .startsWith("sig%3DXXXXX");
  }

  @Test
  // Test masking of signature for rename operation for Blob
  public void testSignatureMaskforBlob() throws Exception {
    assumeBlobServiceType();
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    fs.getAbfsStore().setClient(client);
    String src = String.format("/testABC/test%s.xt", UUID.randomUUID());
    String dest = "/testABC" + "/abc.txt";
    fs.create(new Path(src)).close();

    Mockito.doAnswer(answer -> {
          Path srcCopy = answer.getArgument(0);
          Path dstCopy = answer.getArgument(1);
          String leaseId = answer.getArgument(2);
          TracingContext tracingContext = answer.getArgument(3);
          AbfsRestOperation op
              = ((AbfsBlobClient) getFileSystem().getAbfsClient()).copyBlob(srcCopy,
              dstCopy, leaseId, tracingContext);
          checkSignatureMaskAssertions(op);
          return answer.callRealMethod();
        })
        .when(client)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(String.class), Mockito.any(TracingContext.class));

    Mockito.doAnswer(answer -> {
          Path blobPath = answer.getArgument(0);
          String leaseId = answer.getArgument(1);
          TracingContext tracingContext = answer.getArgument(2);
          AbfsRestOperation op
              = ((AbfsBlobClient) getFileSystem().getAbfsClient()).deleteBlobPath(
              blobPath,
              leaseId, tracingContext);
          checkSignatureMaskAssertions(op);
          return answer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.any(String.class),
            Mockito.any(TracingContext.class));

    client.renamePath(src, dest, null,
        getTestTracingContext(fs, false), null,
        false);
  }

  // Test masking of signature for rename operation for DFS
  @Test
  public void testSignatureMask() throws Exception {
    assumeDfsServiceType();
    final AzureBlobFileSystem fs = getFileSystem();
    String src = String.format("/testABC/test%s.xt", UUID.randomUUID());
    fs.create(new Path(src)).close();
    AbfsRestOperation abfsHttpRestOperation = fs.getAbfsClient()
        .renamePath(src, "/testABC" + "/abc.txt", null,
            getTestTracingContext(fs, false), null,
            false)
        .getOp();
    checkSignatureMaskAssertions(abfsHttpRestOperation);
  }

  @Test
  public void testSignatureMaskOnExceptionMessage() throws Exception {
    intercept(IOException.class, "sig=XXXX",
        () -> getFileSystem().getAbfsClient()
            .renamePath("testABC/test.xt", "testABC/abc.txt",
                null, getTestTracingContext(getFileSystem(), false),
                null, false));
  }

  @Test
  // SetPermission should fail when saoid is not the owner and succeed when it is.
  public void testSetPermissionForNonOwner() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();

    Path rootPath = new Path("/");
    FileStatus rootStatus = fs.getFileStatus(rootPath);
    assertEquals("The permissions are not expected.",
        "rwxr-x---",
        rootStatus.getPermission().toString());
    assertNotEquals("The owner is not expected.",
        MockDelegationSASTokenProvider.TEST_OWNER,
        rootStatus.getOwner());

    // Attempt to set permission without being the owner.
    intercept(AccessDeniedException.class,
        AUTHORIZATION_PERMISSION_MISS_MATCH.getErrorCode(), () -> {
          fs.setPermission(rootPath, new FsPermission(FsAction.ALL,
              FsAction.READ_EXECUTE, FsAction.EXECUTE));
          return "Set permission should fail because saoid is not the owner.";
        });

    // Attempt to set permission as the owner.
    fs.setOwner(rootPath, MockDelegationSASTokenProvider.TEST_OWNER, null);
    fs.setPermission(rootPath, new FsPermission(FsAction.ALL,
        FsAction.READ_EXECUTE, FsAction.EXECUTE));
    rootStatus = fs.getFileStatus(rootPath);
    assertEquals("The permissions are not expected.",
        "rwxr-x--x",
        rootStatus.getPermission().toString());
    assertEquals("The directory owner is not expected.",
        MockDelegationSASTokenProvider.TEST_OWNER,
        rootStatus.getOwner());
  }

  @Test
  // Without saoid or suoid, setPermission should succeed with sp=p for a non-owner.
  public void testSetPermissionWithoutAgentForNonOwner() throws Exception {
    assumeHnsEnabled();
    final AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path(MockDelegationSASTokenProvider.NO_AGENT_PATH);
    fs.create(path).close();

    FileStatus status = fs.getFileStatus(path);
    assertEquals("The permissions are not expected.",
        "rw-r--r--",
        status.getPermission().toString());
    assertNotEquals("The owner is not expected.",
        TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID,
        status.getOwner());

    fs.setPermission(path, new FsPermission(FsAction.READ, FsAction.READ, FsAction.NONE));

    FileStatus fileStatus = fs.getFileStatus(path);
    assertEquals("The permissions are not expected.",
        "r--r-----",
        fileStatus.getPermission().toString());
  }

  @Test
  public void testSASQuesMarkPrefix() throws Exception {
    AbfsConfiguration testConfig = this.getConfiguration();
    // the SAS Token Provider is changed
    testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, "org.apache.hadoop.fs.azurebfs.extensions.MockWithPrefixSASTokenProvider");

    AzureBlobFileSystem testFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    Path testFile = new Path("/testSASPrefixQuesMark");

    // the creation of this filesystem should work correctly even when a SAS Token is generated with a ? prefix
    testFs.create(testFile).close();
  }
}
