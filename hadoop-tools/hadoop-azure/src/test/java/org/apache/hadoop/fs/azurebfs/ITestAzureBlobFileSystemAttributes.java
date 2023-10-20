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
import java.util.EnumSet;

import org.assertj.core.api.Assertions;
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
    testGetSetXAttrHelper(fs, testPath);
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
    Assertions.assertThat(fs.getXAttr(testFile, attributeName))
        .describedAs("Retrieved Attribute Value is Not as Expected")
        .containsExactly(attributeValue);

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
    Assertions.assertThat(fs.getXAttr(testFile, attributeName))
        .describedAs("Retrieved Attribute Value is Not as Expected")
        .containsExactly(attributeValue2);
  }

  @Test
  public void testGetSetXAttrOnRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path testPath = new Path("/");
    testGetSetXAttrHelper(fs, testPath);
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
    Assertions.assertThat(fs.getXAttr(testPath, attributeName1))
        .describedAs("Cannot get attribute before setting it")
        .isNull();
    Assertions.assertThat(fs.getXAttr(testPath, attributeName2))
        .describedAs("Cannot get attribute before setting it")
        .isNull();

    // Set the Attributes
    fs.registerListener(
        new TracingHeaderValidator(fs.getAbfsStore().getAbfsConfiguration()
            .getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.SET_ATTR, true, 0));
    fs.setXAttr(testPath, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    fs.setListenerOperation(FSOperationType.GET_ATTR);
    byte[] rv = fs.getXAttr(testPath, attributeName1);
    Assertions.assertThat(rv)
        .describedAs("Retrieved Attribute Does not Matches in Encoded Form")
        .containsExactly(attributeValue1);
    Assertions.assertThat(fs.getAbfsStore().decodeAttribute(rv))
        .describedAs("Retrieved Attribute Does not Matches in Decoded Form")
        .isEqualTo(decodedAttributeValue1);
    fs.registerListener(null);

    // Set the second Attribute
    fs.setXAttr(testPath, attributeName2, attributeValue2);

    // Check all the attributes present and previous Attribute not overridden
    rv = fs.getXAttr(testPath, attributeName1);
    Assertions.assertThat(rv)
        .describedAs("Retrieved Attribute Does not Matches in Encoded Form")
        .containsExactly(attributeValue1);
    Assertions.assertThat(fs.getAbfsStore().decodeAttribute(rv))
        .describedAs("Retrieved Attribute Does not Matches in Decoded Form")
        .isEqualTo(decodedAttributeValue1);

    rv = fs.getXAttr(testPath, attributeName2);
    Assertions.assertThat(rv)
        .describedAs("Retrieved Attribute Does not Matches in Encoded Form")
        .containsExactly(attributeValue2);
    Assertions.assertThat(fs.getAbfsStore().decodeAttribute(rv))
        .describedAs("Retrieved Attribute Does not Matches in Decoded Form")
        .isEqualTo(decodedAttributeValue2);
  }
}
