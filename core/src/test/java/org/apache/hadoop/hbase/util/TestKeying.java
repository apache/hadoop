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

import junit.framework.TestCase;

/**
 * Tests url transformations
 */
public class TestKeying extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test url transformations
   * @throws Exception
   */
  public void testURI() throws Exception {
    checkTransform("http://abc:bcd@www.example.com/index.html" +
      "?query=something#middle");
    checkTransform("file:///usr/bin/java");
    checkTransform("dns:www.powerset.com");
    checkTransform("dns://dns.powerset.com/www.powerset.com");
    checkTransform("http://one.two.three/index.html");
    checkTransform("https://one.two.three:9443/index.html");
    checkTransform("ftp://one.two.three/index.html");

    checkTransform("filename");
  }

  private void checkTransform(final String u) {
    String k = Keying.createKey(u);
    String uri = Keying.keyToUri(k);
    System.out.println("Original url " + u + ", Transformed url " + k);
    assertEquals(u, uri);
  }
}