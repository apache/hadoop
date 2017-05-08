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
package org.apache.hadoop.crypto.key.kms.server;

import static org.apache.hadoop.crypto.key.kms.server.KMSConfiguration.*;
import static org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KEY_ACL;
import static org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class TestKMSACLs {
  @Rule
  public final Timeout globalTimeout = new Timeout(180000);

  @Test
  public void testDefaults() {
    final KMSACLs acls = new KMSACLs(new Configuration(false));
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      Assert.assertTrue(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser("foo")));
    }
  }

  @Test
  public void testCustom() {
    final Configuration conf = new Configuration(false);
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      conf.set(type.getAclConfigKey(), type.toString() + " ");
    }
    final KMSACLs acls = new KMSACLs(conf);
    for (KMSACLs.Type type : KMSACLs.Type.values()) {
      Assert.assertTrue(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser(type.toString())));
      Assert.assertFalse(acls.hasAccess(type,
          UserGroupInformation.createRemoteUser("foo")));
    }
  }

  @Test
  public void testKeyAclConfigurationLoad() {
    final Configuration conf = new Configuration(false);
    conf.set(KEY_ACL + "test_key_1.MANAGEMENT", "CREATE");
    conf.set(KEY_ACL + "test_key_2.ALL", "CREATE");
    conf.set(KEY_ACL + "test_key_3.NONEXISTOPERATION", "CREATE");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "ROLLOVER");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "DECRYPT_EEK");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "ALL", "invalid");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "ALL", "invalid");
    final KMSACLs acls = new KMSACLs(conf);
    Assert.assertTrue("expected key ACL size is 2 but got "
        + acls.keyAcls.size(), acls.keyAcls.size() == 2);
    Assert.assertTrue("expected whitelist ACL size is 1 but got "
        + acls.whitelistKeyAcls.size(), acls.whitelistKeyAcls.size() == 1);
    Assert.assertFalse("ALL should not be allowed for whitelist ACLs.",
        acls.whitelistKeyAcls.containsKey(KeyOpType.ALL));
    Assert.assertTrue("expected default ACL size is 1 but got "
        + acls.defaultKeyAcls.size(), acls.defaultKeyAcls.size() == 1);
    Assert.assertTrue("ALL should not be allowed for default ACLs.",
        acls.defaultKeyAcls.size() == 1);
  }

  @Test
  public void testKeyAclDuplicateEntries() {
    final Configuration conf = new Configuration(false);
    conf.set(KEY_ACL + "test_key_1.DECRYPT_EEK", "decrypt1");
    conf.set(KEY_ACL + "test_key_2.ALL", "all2");
    conf.set(KEY_ACL + "test_key_1.DECRYPT_EEK", "decrypt2");
    conf.set(KEY_ACL + "test_key_2.ALL", "all1,all3");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "default1");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "whitelist1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    final KMSACLs acls = new KMSACLs(conf);
    Assert.assertTrue("expected key ACL size is 2 but got "
        + acls.keyAcls.size(), acls.keyAcls.size() == 2);
    assertKeyAcl("test_key_1", acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("test_key_2", acls, KeyOpType.ALL, "all1", "all3");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK);
    AccessControlList acl = acls.whitelistKeyAcls.get(KeyOpType.DECRYPT_EEK);
    Assert.assertNotNull(acl);
    Assert.assertTrue(acl.isAllAllowed());
  }

  @Test
  public void testKeyAclReload() {
    Configuration conf = new Configuration(false);
    conf.set(DEFAULT_KEY_ACL_PREFIX + "READ", "read1");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "*");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "decrypt1");
    conf.set(KEY_ACL + "testuser1.ALL", "testkey1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "READ", "admin_read1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK", "*");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "admin_decrypt1");
    final KMSACLs acls = new KMSACLs(conf);

    // update config and hot-reload.
    conf.set(DEFAULT_KEY_ACL_PREFIX + "READ", "read2");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "mgmt1,mgmt2");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "GENERATE_EEK", "");
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "decrypt2");
    conf.set(KEY_ACL + "testkey1.ALL", "testkey1,testkey2");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "READ", "admin_read2");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "admin_mgmt,admin_mgmt1");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "GENERATE_EEK", "");
    conf.set(WHITELIST_KEY_ACL_PREFIX + "DECRYPT_EEK", "admin_decrypt2");
    acls.setKeyACLs(conf);

    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
        "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");

    // reloading same config, nothing should change.
    acls.setKeyACLs(conf);
    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "decrypt2");
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
        "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");

    // test wildcard.
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "*");
    acls.setKeyACLs(conf);
    AccessControlList acl = acls.defaultKeyAcls.get(KeyOpType.DECRYPT_EEK);
    Assert.assertTrue(acl.isAllAllowed());
    Assert.assertTrue(acl.getUsers().isEmpty());
    // everything else should still be the same.
    assertDefaultKeyAcl(acls, KeyOpType.READ, "read2");
    assertDefaultKeyAcl(acls, KeyOpType.MANAGEMENT, "mgmt1", "mgmt2");
    assertDefaultKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertKeyAcl("testuser1", acls, KeyOpType.ALL, "testkey1");
    assertWhitelistKeyAcl(acls, KeyOpType.READ, "admin_read2");
    assertWhitelistKeyAcl(acls, KeyOpType.MANAGEMENT,
        "admin_mgmt", "admin_mgmt1");
    assertWhitelistKeyAcl(acls, KeyOpType.GENERATE_EEK);
    assertWhitelistKeyAcl(acls, KeyOpType.DECRYPT_EEK, "admin_decrypt2");

    // test new configuration should clear other items
    conf = new Configuration();
    conf.set(DEFAULT_KEY_ACL_PREFIX + "DECRYPT_EEK", "new");
    acls.setKeyACLs(conf);
    assertDefaultKeyAcl(acls, KeyOpType.DECRYPT_EEK, "new");
    Assert.assertTrue(acls.keyAcls.isEmpty());
    Assert.assertTrue(acls.whitelistKeyAcls.isEmpty());
    Assert.assertEquals("Got unexpected sized acls:"
        + acls.defaultKeyAcls, 1, acls.defaultKeyAcls.size());
  }

  private void assertDefaultKeyAcl(final KMSACLs acls, final KeyOpType op,
      final String... names) {
    final AccessControlList acl = acls.defaultKeyAcls.get(op);
    assertAcl(acl, op, names);
  }

  private void assertWhitelistKeyAcl(final KMSACLs acls, final KeyOpType op,
      final String... names) {
    final AccessControlList acl = acls.whitelistKeyAcls.get(op);
    assertAcl(acl, op, names);
  }

  private void assertKeyAcl(final String keyName, final KMSACLs acls,
      final KeyOpType op, final String... names) {
    Assert.assertTrue(acls.keyAcls.containsKey(keyName));
    final HashMap<KeyOpType, AccessControlList> keyacl =
        acls.keyAcls.get(keyName);
    Assert.assertNotNull(keyacl.get(op));
    assertAcl(keyacl.get(op), op, names);
  }

  private void assertAcl(final AccessControlList acl,
      final KeyOpType op, final String... names) {
    Assert.assertNotNull(acl);
    Assert.assertFalse(acl.isAllAllowed());
    final Collection<String> actual = acl.getUsers();
    final HashSet<String> expected = new HashSet<>();
    for (String name : names) {
      expected.add(name);
    }
    Assert.assertEquals("defaultKeyAcls don't match for op:" + op,
        expected, actual);
  }
}
