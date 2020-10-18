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
package org.apache.hadoop.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ZKUtil.BadAclFormatException;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;

public class TestZKUtil {
  private static final String TEST_ROOT_DIR = GenericTestUtils.getTempPath(
      "TestZKUtil");
  private static final File TEST_FILE = new File(TEST_ROOT_DIR,
      "test-file");
  
  /** A path which is expected not to exist */
  private static final String BOGUS_FILE =
      new File("/xxxx-this-does-not-exist").getPath();

  @Test
  public void testEmptyACL() {
    List<ACL> result = ZKUtil.parseACLs("");
    assertTrue(result.isEmpty());
  }
  
  @Test
  public void testNullACL() {
    List<ACL> result = ZKUtil.parseACLs(null);
    assertTrue(result.isEmpty());
  }
  
  @Test
  public void testInvalidACLs() {
    badAcl("a:b",
        "ACL 'a:b' not of expected form scheme:id:perm"); // not enough parts
    badAcl("a",
        "ACL 'a' not of expected form scheme:id:perm"); // not enough parts
    badAcl("password:foo:rx",
        "Invalid permission 'x' in permission string 'rx'");
  }
  
  private static void badAcl(String acls, String expectedErr) {
    try {
      ZKUtil.parseACLs(acls);
      fail("Should have failed to parse '" + acls + "'");
    } catch (BadAclFormatException e) {
      assertEquals(expectedErr, e.getMessage());
    }
  }

  @Test
  public void testRemoveSpecificPerms() {
    int perms = Perms.ALL;
    int remove = Perms.CREATE;
    int newPerms = ZKUtil.removeSpecificPerms(perms, remove);
    assertEquals("Removal failed", 0, newPerms & Perms.CREATE);
  }

  @Test
  public void testGoodACLs() {
    List<ACL> result = ZKUtil.parseACLs(
        "sasl:hdfs/host1@MY.DOMAIN:cdrwa, sasl:hdfs/host2@MY.DOMAIN:ca");
    ACL acl0 = result.get(0);
    assertEquals(Perms.CREATE | Perms.DELETE | Perms.READ |
        Perms.WRITE | Perms.ADMIN, acl0.getPerms());
    assertEquals("sasl", acl0.getId().getScheme());
    assertEquals("hdfs/host1@MY.DOMAIN", acl0.getId().getId());
    
    ACL acl1 = result.get(1);
    assertEquals(Perms.CREATE | Perms.ADMIN, acl1.getPerms());
    assertEquals("sasl", acl1.getId().getScheme());
    assertEquals("hdfs/host2@MY.DOMAIN", acl1.getId().getId());
  }
  
  @Test
  public void testEmptyAuth() {
    List<ZKAuthInfo> result = ZKUtil.parseAuth("");
    assertTrue(result.isEmpty());
  }
  
  @Test
  public void testNullAuth() {
    List<ZKAuthInfo> result = ZKUtil.parseAuth(null);
    assertTrue(result.isEmpty());
  }
  
  @Test
  public void testGoodAuths() {
    List<ZKAuthInfo> result = ZKUtil.parseAuth(
        "scheme:data,\n   scheme2:user:pass");
    assertEquals(2, result.size());
    ZKAuthInfo auth0 = result.get(0);
    assertEquals("scheme", auth0.getScheme());
    assertEquals("data", new String(auth0.getAuth()));
    
    ZKAuthInfo auth1 = result.get(1);
    assertEquals("scheme2", auth1.getScheme());
    assertEquals("user:pass", new String(auth1.getAuth()));
  }
  
  @Test
  public void testConfIndirection() throws IOException {
    assertNull(ZKUtil.resolveConfIndirection(null));
    assertEquals("x", ZKUtil.resolveConfIndirection("x"));
    
    TEST_FILE.getParentFile().mkdirs();
    Files.asCharSink(TEST_FILE, Charsets.UTF_8).write("hello world");
    assertEquals("hello world", ZKUtil.resolveConfIndirection(
        "@" + TEST_FILE.getAbsolutePath()));
    
    try {
      ZKUtil.resolveConfIndirection("@" + BOGUS_FILE);
      fail("Did not throw for non-existent file reference");
    } catch (FileNotFoundException fnfe) {
      assertTrue(fnfe.getMessage().startsWith(BOGUS_FILE));
    }
  }
}
