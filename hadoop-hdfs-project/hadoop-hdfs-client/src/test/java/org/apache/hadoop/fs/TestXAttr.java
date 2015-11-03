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

package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for <code>XAttr</code> objects.
 */
public class TestXAttr {
  private static XAttr XATTR, XATTR1, XATTR2, XATTR3, XATTR4, XATTR5;
  
  @BeforeClass
  public static void setUp() throws Exception {
    byte[] value = {0x31, 0x32, 0x33};
    XATTR = new XAttr.Builder()
      .setName("name")
      .setValue(value)
      .build();
    XATTR1 = new XAttr.Builder()
      .setNameSpace(XAttr.NameSpace.USER)
      .setName("name")
      .setValue(value)
      .build();
    XATTR2 = new XAttr.Builder()
      .setNameSpace(XAttr.NameSpace.TRUSTED)
      .setName("name")
      .setValue(value)
      .build();
    XATTR3 = new XAttr.Builder()
      .setNameSpace(XAttr.NameSpace.SYSTEM)
      .setName("name")
      .setValue(value)
      .build();
    XATTR4 = new XAttr.Builder()
      .setNameSpace(XAttr.NameSpace.SECURITY)
      .setName("name")
      .setValue(value)
      .build();
    XATTR5 = new XAttr.Builder()
      .setNameSpace(XAttr.NameSpace.RAW)
      .setName("name")
      .setValue(value)
      .build();
  }
  
  @Test
  public void testXAttrEquals() {
    assertNotSame(XATTR1, XATTR2);
    assertNotSame(XATTR2, XATTR3);
    assertNotSame(XATTR3, XATTR4);
    assertNotSame(XATTR4, XATTR5);
    assertEquals(XATTR, XATTR1);
    assertEquals(XATTR1, XATTR1);
    assertEquals(XATTR2, XATTR2);
    assertEquals(XATTR3, XATTR3);
    assertEquals(XATTR4, XATTR4);
    assertEquals(XATTR5, XATTR5);
    assertFalse(XATTR1.equals(XATTR2));
    assertFalse(XATTR2.equals(XATTR3));
    assertFalse(XATTR3.equals(XATTR4));
    assertFalse(XATTR4.equals(XATTR5));
  }
  
  @Test
  public void testXAttrHashCode() {
    assertEquals(XATTR.hashCode(), XATTR1.hashCode());
    assertFalse(XATTR1.hashCode() == XATTR2.hashCode());
    assertFalse(XATTR2.hashCode() == XATTR3.hashCode());
    assertFalse(XATTR3.hashCode() == XATTR4.hashCode());
    assertFalse(XATTR4.hashCode() == XATTR5.hashCode());
  }
}
