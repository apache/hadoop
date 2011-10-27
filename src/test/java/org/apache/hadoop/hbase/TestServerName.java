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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestServerName {
  @Test
  public void testRegexPatterns() {
    assertTrue(Pattern.matches(Addressing.VALID_PORT_REGEX, "123"));
    assertFalse(Pattern.matches(Addressing.VALID_PORT_REGEX, ""));
    assertTrue(Pattern.matches(Addressing.VALID_HOSTNAME_REGEX, "example.org"));
    assertTrue(Pattern.matches(Addressing.VALID_HOSTNAME_REGEX,
      "www1.example.org"));
    assertTrue(ServerName.SERVERNAME_PATTERN.matcher("www1.example.org,1234,567").matches());
  }

  @Test public void testParseOfBytes() {
    final String snStr = "www.example.org,1234,5678";
    ServerName sn = new ServerName(snStr);
    byte [] versionedBytes = sn.getVersionedBytes();
    assertEquals(snStr, ServerName.parseVersionedServerName(versionedBytes).toString());
    final String hostnamePortStr = "www.example.org:1234";
    byte [] bytes = Bytes.toBytes(hostnamePortStr);
    String expecting =
      hostnamePortStr.replace(":", ServerName.SERVERNAME_SEPARATOR) +
      ServerName.SERVERNAME_SEPARATOR + ServerName.NON_STARTCODE;
    assertEquals(expecting, ServerName.parseVersionedServerName(bytes).toString());
  }

  @Test
  public void testServerName() {
    ServerName sn = new ServerName("www.example.org", 1234, 5678);
    ServerName sn2 = new ServerName("www.example.org", 1234, 5678);
    ServerName sn3 = new ServerName("www.example.org", 1234, 56789);
    assertTrue(sn.equals(sn2));
    assertFalse(sn.equals(sn3));
    assertEquals(sn.hashCode(), sn2.hashCode());
    assertNotSame(sn.hashCode(), sn3.hashCode());
    assertEquals(sn.toString(),
      ServerName.getServerName("www.example.org", 1234, 5678));
    assertEquals(sn.toString(),
      ServerName.getServerName("www.example.org:1234", 5678));
    assertEquals(sn.toString(),
      "www.example.org" + ServerName.SERVERNAME_SEPARATOR +
      "1234" + ServerName.SERVERNAME_SEPARATOR + "5678");
  }

  @Test
  public void getServerStartcodeFromServerName() {
    ServerName sn = new ServerName("www.example.org", 1234, 5678);
    assertEquals(5678,
      ServerName.getServerStartcodeFromServerName(sn.toString()));
    assertNotSame(5677,
      ServerName.getServerStartcodeFromServerName(sn.toString()));
  }
}