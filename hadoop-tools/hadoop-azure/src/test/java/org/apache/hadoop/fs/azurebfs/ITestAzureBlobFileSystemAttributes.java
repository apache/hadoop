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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test attribute operations.
 */
public class ITestAzureBlobFileSystemAttributes extends AbstractAbfsIntegrationTest {
  private static final EnumSet<XAttrSetFlag> CREATE_FLAG = EnumSet.of(XAttrSetFlag.CREATE);
  private static final EnumSet<XAttrSetFlag> REPLACE_FLAG = EnumSet.of(XAttrSetFlag.REPLACE);

  public ITestAzureBlobFileSystemAttributes() throws Exception {
    super();
  }

  @Test
  public void testSetGetXAttr() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = path(getMethodName());
    fs.create(testPath);
    testGetSetXAttrHelper(fs, testPath);
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String decodedAttributeValue =  "one";
    byte[] attributeValue = fs.getAbfsStore().encodeAttribute(decodedAttributeValue);
    String attributeName = "user.someAttribute";
    Path testFile = path(getMethodName());

    // after creating a file, it must be possible to create a new xAttr
    touch(testFile);
    fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG);
    assertAttributeEqual(fs.getXAttr(testFile, attributeName), attributeValue, decodedAttributeValue);

    // however after the xAttr is created, creating it again must fail
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG));
  }

  @Test
  public void testSetGetXAttrReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String decodedAttribute1 = "one";
    String decodedAttribute2 = "two";
    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttribute1);
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttribute2);
    String attributeName = "user.someAttribute";
    Path testFile = path(getMethodName());

    // after creating a file, it must not be possible to replace an xAttr
    intercept(IOException.class, () -> {
      touch(testFile);
      fs.setXAttr(testFile, attributeName, attributeValue1, REPLACE_FLAG);
    });

    // however after the xAttr is created, replacing it must succeed
    fs.setXAttr(testFile, attributeName, attributeValue1, CREATE_FLAG);
    fs.setXAttr(testFile, attributeName, attributeValue2, REPLACE_FLAG);
    assertAttributeEqual(fs.getXAttr(testFile, attributeName), attributeValue2,
        decodedAttribute2);
  }

  /**
   * Test get and set xattr fails fine on root.
   * @throws Exception if test fails
   */
  @Test
  public void testGetSetXAttrOnRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    String attributeName = "user.attribute1";
    byte[] attributeValue = fs.getAbfsStore().encodeAttribute("hi");
    final Path testPath = new Path(ROOT_PATH);

    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class, () -> {
      fs.getXAttr(testPath, attributeName);
    });

    Assertions.assertThat(ex.getStatusCode())
        .describedAs("GetXAttr() on root should fail with Bad Request")
        .isEqualTo(HTTP_BAD_REQUEST);

    ex = intercept(AbfsRestOperationException.class, () -> {
      fs.setXAttr(testPath, attributeName, attributeValue, CREATE_FLAG);
    });

    Assertions.assertThat(ex.getStatusCode())
        .describedAs("SetXAttr() on root should fail with Bad Request")
        .isEqualTo(HTTP_BAD_REQUEST);
  }

  /**
   * Test get and set xattr works fine on marker blobs for directory.
   * @throws Exception if test fails
   */
  @Test
  public void testGetSetXAttrOnExplicitDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = path(getMethodName());
    fs.mkdirs(testPath);
    testGetSetXAttrHelper(fs, testPath);

    // Assert that the folder is now explicit
    DirectoryStateHelper.isExplicitDirectory(testPath, fs, getTestTracingContext(fs, true));
  }

  /**
   * Test get and set xattr fails fine on non-existing path.
   * @throws Exception if test fails
   */
  @Test
  public void testGetSetXAttrOnNonExistingPath() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = path(getMethodName());
    intercept(FileNotFoundException.class, String.valueOf(HTTP_NOT_FOUND),
        "get/set XAttr() should fail for non-existing files",
        () -> testGetSetXAttrHelper(fs, testPath));
  }

  /**
   * Trying to set same attribute multiple times should result in no failure
   * @throws Exception if test fails
   */
  @Test
  public void testSetXAttrMultipleOperations() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path path = path(getMethodName());
    fs.create(path);

    String attributeName = "user.attribute1";
    String decodedAttributeValue = "hi";
    byte[] attributeValue = fs.getAbfsStore().encodeAttribute(decodedAttributeValue);

    // Attribute not present initially
    assertAttributeNull(fs.getXAttr(path, attributeName));

    // Set the Attributes Multiple times
    // Filesystem internally adds create and replace flags
    fs.setXAttr(path, attributeName, attributeValue);
    fs.setXAttr(path, attributeName, attributeValue);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(path, attributeName);
    assertAttributeEqual(rv, attributeValue, decodedAttributeValue);
  }

  /**
   * Test get and set xattr works fine on implicit directory and ends up creating marker blobs.
   * @throws Exception if test fails
   */
  @Test
  public void testGetSetXAttrOnImplicitDir() throws Exception {
    assumeBlobServiceType();
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = path(getMethodName());
    createAzCopyFolder(testPath);
    testGetSetXAttrHelper(fs, testPath);

    // Assert that the folder is now explicit
    DirectoryStateHelper.isExplicitDirectory(testPath, fs, getTestTracingContext(fs, true));
    DirectoryStateHelper.isExplicitDirectory(testPath.getParent(), fs, getTestTracingContext(fs, true));
  }

  private void testGetSetXAttrHelper(final AzureBlobFileSystem fs,
      final Path testPath) throws Exception {

    String attributeName1 = "user.attribute1";
    String attributeName2 = "user.attribute2";
    String decodedAttributeValue1 = "hi";
    String decodedAttributeValue2 = "hello";
    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);

    // Attribute not present initially
    assertAttributeNull(fs.getXAttr(testPath, attributeName1));
    assertAttributeNull(fs.getXAttr(testPath, attributeName2));

    // Set the Attributes
    fs.registerListener(
        new TracingHeaderValidator(fs.getAbfsStore().getAbfsConfiguration()
            .getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.SET_ATTR, true, 0));
    fs.setXAttr(testPath, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    fs.setListenerOperation(FSOperationType.GET_ATTR);
    byte[] rv = fs.getXAttr(testPath, attributeName1);
    assertAttributeEqual(rv, attributeValue1, decodedAttributeValue1);
    fs.registerListener(null);

    // Set the second Attribute
    fs.setXAttr(testPath, attributeName2, attributeValue2);

    // Check all the attributes present and previous Attribute not overridden
    rv = fs.getXAttr(testPath, attributeName1);
    assertAttributeEqual(rv, attributeValue1, decodedAttributeValue1);

    rv = fs.getXAttr(testPath, attributeName2);
    assertAttributeEqual(rv, attributeValue2, decodedAttributeValue2);
  }

  private void assertAttributeNull(byte[] rv) {
    Assertions.assertThat(rv)
        .describedAs("Cannot get attribute before setting it")
        .isNull();
  }

  private void assertAttributeEqual(byte[] rv, byte[] attributeValue,
      String decodedAttributeValue) throws Exception {
    Assertions.assertThat(rv)
        .describedAs("Retrieved Attribute Does not Matches in Encoded Form")
        .containsExactly(attributeValue);
    Assertions.assertThat(getFileSystem().getAbfsStore().decodeAttribute(rv))
        .describedAs("Retrieved Attribute Does not Matches in Decoded Form")
        .isEqualTo(decodedAttributeValue);
  }
}
