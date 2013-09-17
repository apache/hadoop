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
package org.apache.hadoop.hdfs.util;

import junit.framework.Assert;

import org.apache.hadoop.hdfs.util.XMLUtils.UnmanglingError;
import org.junit.Test;

public class TestXMLUtils {
  private static void testRoundTrip(String str, String expectedMangled) {
    String mangled = XMLUtils.mangleXmlString(str);
    Assert.assertEquals(mangled, expectedMangled);
    String unmangled = XMLUtils.unmangleXmlString(mangled);
    Assert.assertEquals(unmangled, str);
  }

  @Test
  public void testMangleEmptyString() throws Exception {
    testRoundTrip("", "");
  }

  @Test
  public void testMangleVanillaString() throws Exception {
    testRoundTrip("abcdef", "abcdef");
  }

  @Test
  public void testMangleStringWithBackSlash() throws Exception {
    testRoundTrip("a\\bcdef", "a\\005c;bcdef");
    testRoundTrip("\\\\", "\\005c;\\005c;");
  }  

  @Test
  public void testMangleStringWithForbiddenCodePoint() throws Exception {
    testRoundTrip("a\u0001bcdef", "a\\0001;bcdef");
    testRoundTrip("a\u0002\ud800bcdef", "a\\0002;\\d800;bcdef");
  }

  @Test
  public void testInvalidSequence() throws Exception {
    try {
      XMLUtils.unmangleXmlString("\\000g;foo");
      Assert.fail("expected an unmangling error");
    } catch (UnmanglingError e) {
      // pass through
    }
    try {
      XMLUtils.unmangleXmlString("\\0");
      Assert.fail("expected an unmangling error");
    } catch (UnmanglingError e) {
      // pass through
    }
  }
}
