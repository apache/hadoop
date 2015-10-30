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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestXAttrFeature {

  static final String name1 = "system.a1";
  static final byte[] value1 = {0x31, 0x32, 0x33};
  static final String name2 = "security.a2";
  static final byte[] value2 = {0x37, 0x38, 0x39};
  static final String name3 = "trusted.a3";
  static final String name4 = "user.a4";
  static final byte[] value4 = {0x01, 0x02, 0x03};
  static final String name5 = "user.a5";
  static final byte[] value5 = randomBytes(2000);
  static final String name6 = "user.a6";
  static final byte[] value6 = randomBytes(1800);
  static final String name7 = "raw.a7";
  static final byte[] value7 = {0x011, 0x012, 0x013};
  static final String name8 = "user.a8";
  static final String bigXattrKey = "user.big.xattr.key";
  static final byte[] bigXattrValue = new byte[128];

  static {
    for (int i = 0; i < bigXattrValue.length; i++) {
      bigXattrValue[i] = (byte) (i & 0xff);
    }
  }

  static byte[] randomBytes(int len) {
    Random rand = new Random();
    byte[] bytes = new byte[len];
    rand.nextBytes(bytes);
    return bytes;
  }
  @Test
  public void testXAttrFeature() throws Exception {
    List<XAttr> xAttrs = new ArrayList<>();
    XAttrFeature feature = new XAttrFeature(xAttrs);

    // no XAttrs in the feature
    assertTrue(feature.getXAttrs().isEmpty());

    // one XAttr in the feature
    XAttr a1 = XAttrHelper.buildXAttr(name1, value1);
    xAttrs.add(a1);
    feature = new XAttrFeature(xAttrs);

    XAttr r1 = feature.getXAttr(name1);
    assertTrue(a1.equals(r1));
    assertEquals(feature.getXAttrs().size(), 1);

    // more XAttrs in the feature
    XAttr a2 = XAttrHelper.buildXAttr(name2, value2);
    XAttr a3 = XAttrHelper.buildXAttr(name3);
    XAttr a4 = XAttrHelper.buildXAttr(name4, value4);
    XAttr a5 = XAttrHelper.buildXAttr(name5, value5);
    XAttr a6 = XAttrHelper.buildXAttr(name6, value6);
    XAttr a7 = XAttrHelper.buildXAttr(name7, value7);
    XAttr bigXattr = XAttrHelper.buildXAttr(bigXattrKey, bigXattrValue);
    xAttrs.add(a2);
    xAttrs.add(a3);
    xAttrs.add(a4);
    xAttrs.add(a5);
    xAttrs.add(a6);
    xAttrs.add(a7);
    xAttrs.add(bigXattr);
    feature = new XAttrFeature(xAttrs);

    XAttr r2 = feature.getXAttr(name2);
    assertTrue(a2.equals(r2));
    XAttr r3 = feature.getXAttr(name3);
    assertTrue(a3.equals(r3));
    XAttr r4 = feature.getXAttr(name4);
    assertTrue(a4.equals(r4));
    XAttr r5 = feature.getXAttr(name5);
    assertTrue(a5.equals(r5));
    XAttr r6 = feature.getXAttr(name6);
    assertTrue(a6.equals(r6));
    XAttr r7 = feature.getXAttr(name7);
    assertTrue(a7.equals(r7));
    XAttr rBigXattr = feature.getXAttr(bigXattrKey);
    assertTrue(bigXattr.equals(rBigXattr));
    List<XAttr> rs = feature.getXAttrs();
    assertEquals(rs.size(), xAttrs.size());
    for (int i = 0; i < rs.size(); i++) {
      assertTrue(xAttrs.contains(rs.get(i)));
    }

    // get non-exist XAttr in the feature
    XAttr r8 = feature.getXAttr(name8);
    assertTrue(r8 == null);
  }
}
