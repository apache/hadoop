/**
 * Copyright 2007 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

/**
 * Test order preservation characteristics of ordered Base64 dialect
 */
public class TestBase64 extends TestCase {
  // Note: uris is sorted. We need to prove that the ordered Base64
  // preserves that ordering
  private String[] uris = {
      "dns://dns.powerset.com/www.powerset.com",
      "dns:www.powerset.com",
      "file:///usr/bin/java",
      "filename",
      "ftp://one.two.three/index.html",
      "http://one.two.three/index.html",
      "https://one.two.three:9443/index.html",
      "r:dns://com.powerset.dns/www.powerset.com",
      "r:ftp://three.two.one/index.html",
      "r:http://three.two.one/index.html",
      "r:https://three.two.one:9443/index.html"
  };

  /**
   * the test
   * @throws UnsupportedEncodingException
   */
  public void testBase64() throws UnsupportedEncodingException {
    TreeMap<String, String> sorted = new TreeMap<String, String>();

    for (int i = 0; i < uris.length; i++) {
      byte[] bytes = uris[i].getBytes("UTF-8");
      sorted.put(Base64.encodeBytes(bytes, Base64.ORDERED), uris[i]);
    }
    System.out.println();

    int i = 0;
    for (Map.Entry<String, String> e: sorted.entrySet()) {
      assertTrue(uris[i++].compareTo(e.getValue()) == 0);
    }
  }
}
