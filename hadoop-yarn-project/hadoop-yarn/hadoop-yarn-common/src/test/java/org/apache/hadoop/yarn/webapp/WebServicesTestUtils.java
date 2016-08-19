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

package org.apache.hadoop.yarn.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response.StatusType;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class WebServicesTestUtils {

  public static long getXmlLong(Element element, String name) {
    String val = getXmlString(element, name);
    return Long.parseLong(val);
  }

  public static int getXmlInt(Element element, String name) {
    String val = getXmlString(element, name);
    return Integer.parseInt(val);
  }

  public static Boolean getXmlBoolean(Element element, String name) {
    String val = getXmlString(element, name);
    return Boolean.parseBoolean(val);
  }

  public static float getXmlFloat(Element element, String name) {
    String val = getXmlString(element, name);
    return Float.parseFloat(val);
  }

  public static List<String> getXmlStrings(Element element, String name) {
    NodeList id = element.getElementsByTagName(name);
    List<String> strings = new ArrayList<>();
    int len = id.getLength();
    if (id.getLength() == 0) {
      return strings;
    }
    for (int i = 0; i < len; i++) {
      Element line = (Element) id.item(i);
      if (line == null) {
        continue;
      }
      Node first = line.getFirstChild();
      if (first == null) {
        continue;
      }
      String val = first.getNodeValue();
      if (val == null) {
        continue;
      }
      strings.add(val);
    }
    return strings;
  }

  public static String getXmlString(Element element, String name) {
    NodeList id = element.getElementsByTagName(name);
    Element line = (Element) id.item(0);
    if (line == null) {
      return null;
    }
    Node first = line.getFirstChild();
    // handle empty <key></key>
    if (first == null) {
      return "";
    }
    String val = first.getNodeValue();
    if (val == null) {
      return "";
    }
    return val;
  }

  public static String getXmlAttrString(Element element, String name) {
    Attr at = element.getAttributeNode(name);
    if (at != null) {
      return at.getValue();
    }
    return null;
  }

  public static void checkStringMatch(String print, String expected, String got) {
    assertTrue(
        print + " doesn't match, got: " + got + " expected: " + expected,
        got.matches(expected));
  }

  public static void checkStringContains(String print, String expected, String got) {
    assertTrue(
        print + " doesn't contain expected string, got: " + got + " expected: " + expected,
        got.contains(expected));
  }

  public static void checkStringEqual(String print, String expected, String got) {
    assertTrue(
        print + " is not equal, got: " + got + " expected: " + expected,
        got.equals(expected));
  }

  public static void assertResponseStatusCode(StatusType expected,
      StatusType actual) {
    assertResponseStatusCode(null, expected, actual);
  }

  public static void assertResponseStatusCode(String errmsg,
      StatusType expected, StatusType actual) {
    assertEquals(errmsg, expected.getStatusCode(), actual.getStatusCode());
  }
}
