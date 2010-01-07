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
package org.apache.hadoop.security.authorize;

import java.security.CodeSource;
import java.security.CodeSigner;
import java.security.PermissionCollection;
import java.security.ProtectionDomain;
import java.net.URL;
import java.net.NetPermission;
import java.net.MalformedURLException;

import javax.security.auth.Subject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.SecurityUtil.AccessControlList;

import junit.framework.TestCase;

public class TestConfiguredPolicy extends TestCase {
  private static final String USER1 = "drwho";
  private static final String USER2 = "joe";
  private static final String[] GROUPS1 = new String[]{"tardis"};
  private static final String[] GROUPS2 = new String[]{"users"};
  
  private static final String KEY_1 = "test.policy.1";
  private static final String KEY_2 = "test.policy.2";

  public static class Protocol1 {
    int i;
  }
  public static class Protocol2 {
    int j;
  }
  
  private static class TestPolicyProvider extends PolicyProvider {
    @Override
    public Service[] getServices() {
      return new Service[] {
          new Service(KEY_1, Protocol1.class),
          new Service(KEY_2, Protocol2.class),
          };
    }
  }
  
  public void testConfiguredPolicy() throws Exception {
    ConfiguredPolicy policy = createConfiguredPolicy();
    SecurityUtil.setPolicy(policy);
    
    Subject user1 = 
      SecurityUtil.getSubject(new UnixUserGroupInformation(USER1, GROUPS1));

    // Should succeed
    ServiceAuthorizationManager.authorize(user1, Protocol1.class);
    
    // Should fail
    Subject user2 = 
      SecurityUtil.getSubject(new UnixUserGroupInformation(USER2, GROUPS2));
    boolean failed = false;
    try {
      ServiceAuthorizationManager.authorize(user2, Protocol2.class);
    } catch (AuthorizationException ae) {
      failed = true;
    }
    assertTrue(failed);
  }

  /**
   * Create a configured policy with some keys
   * @return a new configured policy
   */
  private ConfiguredPolicy createConfiguredPolicy() {
    Configuration conf = new Configuration();
    conf.set(KEY_1, AccessControlList.WILDCARD_ACL_VALUE);
    conf.set(KEY_2, USER1 + " " + GROUPS1[0]);

    return new ConfiguredPolicy(conf, new TestPolicyProvider());
  }

  /**
   * Create a test code source against a test URL
   * @return a new code source
   * @throws MalformedURLException
   */
  private CodeSource createCodeSource() throws MalformedURLException {
    return new CodeSource(new URL("http://hadoop.apache.org"),
        (CodeSigner[]) null);
  }

  /**
   * Assert that a permission collection can have new permissions added
   * @param permissions the collection to check
   */
  private void assertWritable(PermissionCollection permissions) {
    assertFalse(permissions.isReadOnly());
    NetPermission netPermission = new NetPermission("something");
    permissions.add(netPermission);
  }

  /**
   * test that the {@link PermissionCollection} returned by
   * {@link ConfiguredPolicy#getPermissions(CodeSource)} is writeable
   * @throws Throwable on any failure
   */
  public void testPolicyWritable() throws Throwable {
    ConfiguredPolicy policy = createConfiguredPolicy();
    CodeSource source = createCodeSource();
    PermissionCollection permissions = policy.getPermissions(source);
    assertWritable(permissions);
  }

  /**
   * test that the {@link PermissionCollection} returned by
   * {@link ConfiguredPolicy#getPermissions(CodeSource)} is writeable
   * @throws Throwable on any failure
   */
  public void testProtectionDomainPolicyWritable() throws Throwable {
    ConfiguredPolicy policy = createConfiguredPolicy();
    CodeSource source = createCodeSource();
    PermissionCollection permissions = policy.getPermissions(new ProtectionDomain(source, null));
    assertWritable(permissions);
  }
}
