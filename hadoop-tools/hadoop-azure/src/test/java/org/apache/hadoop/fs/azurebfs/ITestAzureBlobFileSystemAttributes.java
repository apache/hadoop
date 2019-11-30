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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.junit.Assume;
import org.junit.Test;

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
    Assume.assumeTrue(fs.getIsNamespaceEnabled());

    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute("你好");
    String attributeName1 = "user.asciiAttribute";
    String attributeName2 = "user.unicodeAttribute";
    Path testFile = path("setGetXAttr");

    // after creating a file, the xAttr should not be present
    touch(testFile);
    assertNull(fs.getXAttr(testFile, attributeName1));

    // after setting the xAttr on the file, the value should be retrievable
    fs.setXAttr(testFile, attributeName1, attributeValue1);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));

    // after setting a second xAttr on the file, the first xAttr values should not be overwritten
    fs.setXAttr(testFile, attributeName2, attributeValue2);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName2));
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
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
    Assume.assumeTrue(fs.getIsNamespaceEnabled());
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
}
