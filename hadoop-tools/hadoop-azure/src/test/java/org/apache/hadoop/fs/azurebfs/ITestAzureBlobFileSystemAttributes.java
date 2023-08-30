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
import java.util.Arrays;
import java.util.EnumSet;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

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
    AbfsConfiguration conf = fs.getAbfsStore().getAbfsConfiguration();
    final Path testPath = path("setGetXAttr");
    fs.create(testPath);
    testGetSetXAttrHelper(fs, testPath, testPath);
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    byte[] attributeValue = fs.getAbfsStore().encodeAttribute("one");
    String attributeName = "user.someAttribute";
    Path testFile = path("createReplaceXAttr");

    // after creating a file, it must be possible to create a new xAttr
    touch(testFile);
    fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG);
    assertArrayEquals(attributeValue, fs.getXAttr(testFile, attributeName));

    // however after the xAttr is created, creating it again must fail
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG));
  }

  @Test
  public void testSetGetXAttrReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute("one");
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute("two");
    String attributeName = "user.someAttribute";
    Path testFile = path("replaceXAttr");

    // after creating a file, it must not be possible to replace an xAttr
    intercept(IOException.class, () -> {
      touch(testFile);
      fs.setXAttr(testFile, attributeName, attributeValue1, REPLACE_FLAG);
    });

    // however after the xAttr is created, replacing it must succeed
    fs.setXAttr(testFile, attributeName, attributeValue1, CREATE_FLAG);
    fs.setXAttr(testFile, attributeName, attributeValue2, REPLACE_FLAG);
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName));
  }

  @Test
  public void testGetSetXAttrOnRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = new Path("a/b");
    final Path testPath = new Path("/");
    fs.create(filePath);
    testGetSetXAttrHelper(fs, filePath, testPath);
  }

  private void testGetSetXAttrHelper(final AzureBlobFileSystem fs,
      final Path filePath, final Path testPath) throws Exception {

    String attributeName1 = "user.attribute1";
    String attributeName2 = "user.attribute2";
    String decodedAttributeValue1 = "hi";
    String decodedAttributeValue2 = "hello";
    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);

    // Attribute not present initially
    assertNull(fs.getXAttr(testPath, attributeName1));
    assertNull(fs.getXAttr(testPath, attributeName2));

    // Set the Attributes
    fs.setXAttr(testPath, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(testPath, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(decodedAttributeValue1, fs.getAbfsStore().decodeAttribute(rv));

    // Set the second Attribute
    fs.setXAttr(testPath, attributeName2, attributeValue2);

    // Check all the attributes present and previous Attribute not overridden
    rv = fs.getXAttr(testPath, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(decodedAttributeValue1, fs.getAbfsStore().decodeAttribute(rv));
    rv = fs.getXAttr(testPath, attributeName2);
    assertTrue(Arrays.equals(rv, attributeValue2));
    assertEquals(decodedAttributeValue2, fs.getAbfsStore().decodeAttribute(rv));
  }
}
