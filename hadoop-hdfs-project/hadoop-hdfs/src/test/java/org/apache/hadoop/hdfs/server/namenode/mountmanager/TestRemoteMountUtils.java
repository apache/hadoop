/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.hdfs.util.RemoteMountUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestRemoteMountUtils {
  @Test
  public void testKVParsing() {
    String xConfig = "abc=xyz,cba=abc";
    Map<String, String> configs = RemoteMountUtils.decodeConfig(xConfig);
    assertEquals(2, configs.size());
    assertEquals("xyz", configs.get("abc"));
    assertEquals("abc", configs.get("cba"));
  }

  @Test
  public void testSpecialCharInValue() {
    String xConfig = "abc=xyz,cba=ab\\=c\\,d";
    Map<String, String> configs = RemoteMountUtils.decodeConfig(xConfig);
    assertEquals(2, configs.size());
    assertEquals("xyz", configs.get("abc"));
    assertEquals("ab=c,d", configs.get("cba"));
  }

  @Test
  public void testEncodingSpecialChars() {
    Map<String, String> configs = new HashMap<>();
    configs.put("abc", "xyz");
    configs.put("cba", "ab=c,d");
    String encoded = RemoteMountUtils.encodeConfig(configs);
    assertEquals("abc=xyz,cba=ab\\=c\\,d", encoded);
  }

  @Test
  public void testEncodeNull() {
    String encoded = RemoteMountUtils.encodeConfig(null);
    assertEquals("", encoded);
  }

  @Test
  public void testEncodeEmptyMap() {
    String encoded = RemoteMountUtils.encodeConfig(Collections.emptyMap());
    assertEquals("", encoded);
  }
}