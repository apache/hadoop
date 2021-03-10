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
import java.lang.reflect.InvocationTargetException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformerInterface;
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

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_IDENTITY_TRANSFORM_CLASS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;

public class ITestAbfsClientCustomerProvidedKey
    extends AbstractAbfsIntegrationTest {

  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int INT_512 = 512;
  private static final int INT_50 = 50;

  private final IdentityTransformerInterface identityTransformer;

  public ITestAbfsClientCustomerProvidedKey() throws Exception {
    final Class<? extends IdentityTransformerInterface> identityTransformerClass = getRawConfiguration()
        .getClass(FS_AZURE_IDENTITY_TRANSFORM_CLASS, IdentityTransformer.class,
            IdentityTransformerInterface.class);
    try {
      this.identityTransformer = identityTransformerClass
          .getConstructor(Configuration.class)
          .newInstance(getRawConfiguration());
    } catch (IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException(e);
    }
  }

  @Test
  public void testAppendWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test"));
    AbfsClient abfsClient = fs.getAbfsClient();
    int length = 5;
    AppendRequestParameters appendRequestParameters =
        new AppendRequestParameters(
        0, 0, length, Mode.APPEND_MODE, false);
    byte[] buffer = getRandomBytesArray(5);
    AbfsRestOperation abfsRestOperation = abfsClient
        .append("/test", buffer, appendRequestParameters, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testAppendWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AppendRequestParameters appendRequestParameters =
        new AppendRequestParameters(
        0, 0, 5, Mode.APPEND_MODE, false);
    byte[] buffer = getRandomBytesArray(5);
    AbfsRestOperation abfsRestOperation = abfsClient
        .append("/test", buffer, appendRequestParameters, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testListPathWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .listPath("/test1", false, INT_50, null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testListPathWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .listPath("/test1", false, INT_50, null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testGetFileSystemPropertiesWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getFilesystemProperties();
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testGetFileSystemPropertiesWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getFilesystemProperties();
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testDeleteFileSystemWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.deleteFilesystem();
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testDeleteFileSystemWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.deleteFilesystem();
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testCreatePathWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    boolean isNamespaceEnabled = fs.getIsNamespaceEnabled();
    AbfsRestOperation abfsRestOperation = abfsClient
        .createPath("/test", true, true,
            isNamespaceEnabled ? getOctalNotation(permission) : null,
            isNamespaceEnabled ? getOctalNotation(umask) : null, false, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testCreatePathWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    boolean isNamespaceEnabled = fs.getIsNamespaceEnabled();
    AbfsRestOperation abfsRestOperation = abfsClient
        .createPath("/test", true, true,
            isNamespaceEnabled ? getOctalNotation(permission) : null,
            isNamespaceEnabled ? getOctalNotation(umask) : null, false, null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testRenamePathWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .renamePath("/test1", "/test2", null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testRenamePathWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .renamePath("/test1", "/test2", null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testFlushWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .flush("/test1", 0, false, false, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testFlushWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .flush("/test1", 0, false, false, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testSetPathPropertiesWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "val");
    AbfsRestOperation abfsRestOperation = abfsClient.setPathProperties("/test1",
        convertXmsPropertiesToCommaSeparatedString(properties));
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testSetPathPropertiesWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "val");
    AbfsRestOperation abfsRestOperation = abfsClient.setPathProperties("/test1",
        convertXmsPropertiesToCommaSeparatedString(properties));
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testGetPathStatusWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .getPathStatus("/test1", false);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testGetPathStatusWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .getPathStatus("/test1", false);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testReadWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    int fileSize = ONE_MB;
    byte[] fileContent = getRandomBytesArray(fileSize);
    createFileWithContent(fs, "/test1", fileContent);
    AbfsClient abfsClient = fs.getAbfsClient();
    int length = INT_512;
    byte[] buffer = new byte[length * 4];
    final AbfsRestOperation op = abfsClient.getPathStatus("/test1", false);
    final String eTag = op.getResult().getResponseHeader(
        HttpHeaderConfigurations.ETAG);
    AbfsRestOperation abfsRestOperation = abfsClient
        .read("/test1", 0, buffer, 0, length, eTag, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testReadWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    int fileSize = ONE_MB;
    byte[] fileContent = getRandomBytesArray(fileSize);
    createFileWithContent(fs, "/test1", fileContent);
    AbfsClient abfsClient = fs.getAbfsClient();
    int length = INT_512;
    byte[] buffer = new byte[length * 4];
    final AbfsRestOperation op = abfsClient.getPathStatus("/test1", false);
    final String eTag = op.getResult().getResponseHeader(
        HttpHeaderConfigurations.ETAG);
    AbfsRestOperation abfsRestOperation = abfsClient
        .read("/test1", 0, buffer, 0, length, eTag, null);
    assertCPKHeaders(abfsRestOperation, isWithCPK);
  }

  @Test
  public void testDeletePathWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .deletePath("/test1", false, null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testDeletePathWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .deletePath("/test1", false, null);
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testSetPermissionWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPermission("/test1", permission.toString());
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testSetPermissionWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    FsPermission permission = new FsPermission(FsAction.EXECUTE,
        FsAction.EXECUTE, FsAction.EXECUTE);
    AbfsRestOperation abfsRestOperation = abfsClient
        .setPermission("/test1", permission.toString());
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testSetAclWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(ACCESS, USER, ALL));
    final Map<String, String> aclEntries = AbfsAclHelper
        .deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));

    AbfsRestOperation abfsRestOperation = abfsClient
        .setAcl("/test1", AbfsAclHelper.serializeAclSpec(aclEntries));
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testSetAclWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();

    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(ACCESS, USER, ALL));
    final Map<String, String> aclEntries = AbfsAclHelper
        .deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));

    AbfsRestOperation abfsRestOperation = abfsClient
        .setAcl("/test1", AbfsAclHelper.serializeAclSpec(aclEntries));
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testGetAclWithCPK() throws Exception {
    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getAclStatus("/test1");
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testGetAclWithoutCPK() throws Exception {
    boolean isWithCPK = false;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient.getAclStatus("/test1");
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testCheckAccessWithCPK() throws Exception {
    boolean isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        isHNSEnabled);
    Assume.assumeTrue("AuthType has to be OAuth",
        getAuthType() == AuthType.OAuth);

    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .checkAccess("/test1", "rwx");
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testCheckAccessWithoutCPK() throws Exception {
    boolean isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        isHNSEnabled);
    Assume.assumeTrue("AuthType has to be OAuth",
        getAuthType() == AuthType.OAuth);

    boolean isWithCPK = true;
    final AzureBlobFileSystem fs = getAbfs(isWithCPK);
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
    fs.create(new Path("/test1"));
    AbfsClient abfsClient = fs.getAbfsClient();
    AbfsRestOperation abfsRestOperation = abfsClient
        .checkAccess("/test1", "rwx");
    assertCPKHeaders(abfsRestOperation, false);
  }

  @Test
  public void testWriteReadAndVerify() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    int fileSize = 2 * ONE_MB;
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
  }

  @Test
  public void testSetGetXAttr() throws Exception {
    final AzureBlobFileSystem fs = getAbfs(true);
    String fileName = methodName.getMethodName();
    fs.create(new Path(fileName));
    String valSent = "testValue";
    String attrName = "testXAttr";
    fs.setXAttr(new Path(fileName), attrName,
        valSent.getBytes(StandardCharsets.UTF_8),
        EnumSet.of(XAttrSetFlag.CREATE));
    byte[] valBytes = fs.getXAttr(new Path(fileName), attrName);
    String valRecieved = new String(valBytes);
    assertEquals(valSent, valRecieved);
    valSent = "new value";
    fs.setXAttr(new Path(fileName), attrName,
        valSent.getBytes(StandardCharsets.UTF_8),
        EnumSet.of(XAttrSetFlag.REPLACE));
    valBytes = fs.getXAttr(new Path(fileName), attrName);
    valRecieved = new String(valBytes);
    assertEquals(valSent, valRecieved);
  }

  private void assertCPKHeaders(AbfsRestOperation abfsRestOperation,
      boolean isCPKHeaderExpected) {
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_KEY, isCPKHeaderExpected);
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_KEY_SHA256,
        isCPKHeaderExpected);
    assertHeader(abfsRestOperation, X_MS_ENCRYPTION_ALGORITHM,
        isCPKHeaderExpected);
  }

  private void assertHeader(AbfsRestOperation abfsRestOperation,
      String headerName, boolean isCPKHeaderExpected) {
    assertTrue(abfsRestOperation != null);
    Optional<AbfsHttpHeader> header = abfsRestOperation.getRequestHeaders()
        .stream().filter(abfsHttpHeader -> abfsHttpHeader.getName()
            .equalsIgnoreCase(headerName)).findFirst();
    String desc;
    if (isCPKHeaderExpected) {
      desc = "CPK hear should be resent";
    } else {
      desc = "CPK hear should not be resent";
    }
    Assertions.assertThat(header.isPresent())
        .describedAs(desc)
        .isEqualTo(isCPKHeaderExpected);
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
    Configuration conf = getRawConfiguration();
    if (withCPK) {
      conf.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName(),
          "testkey");
    } else {
      conf.unset(
          FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName());
    }
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

}
