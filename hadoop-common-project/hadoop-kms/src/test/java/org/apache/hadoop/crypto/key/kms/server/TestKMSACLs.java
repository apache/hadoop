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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class TestKMSACLs {

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
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "test_key_1.MANAGEMENT", "CREATE");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "test_key_2.ALL", "CREATE");
    conf.set(KeyAuthorizationKeyProvider.KEY_ACL + "test_key_3.NONEXISTOPERATION", "CREATE");
    conf.set(KMSConfiguration.DEFAULT_KEY_ACL_PREFIX + "MANAGEMENT", "ROLLOVER");
    conf.set(KMSConfiguration.WHITELIST_KEY_ACL_PREFIX + "MANAGEMENT", "DECRYPT_EEK");
    final KMSACLs acls = new KMSACLs(conf);
    Assert.assertTrue("expected key ACL size is 2 but got " + acls.keyAcls.size(),
        acls.keyAcls.size() == 2);
  }
}
