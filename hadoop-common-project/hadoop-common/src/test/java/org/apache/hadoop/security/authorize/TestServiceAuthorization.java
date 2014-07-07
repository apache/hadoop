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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.junit.Test;

public class TestServiceAuthorization {

  private static final String ACL_CONFIG = "test.protocol.acl";
  private static final String ACL_CONFIG1 = "test.protocol1.acl";

  public interface TestProtocol1 extends TestProtocol {};

  private static class TestPolicyProvider extends PolicyProvider {

    @Override
    public Service[] getServices() {
      return new Service[] { new Service(ACL_CONFIG, TestProtocol.class), 
          new Service(ACL_CONFIG1, TestProtocol1.class),
      };
    }
  }

  @Test
  public void testDefaultAcl() {
    ServiceAuthorizationManager serviceAuthorizationManager = 
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();
    //test without setting a default acl
    conf.set(ACL_CONFIG, "user1 group1");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    AccessControlList acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol.class);
    assertEquals("user1 group1", acl.getAclString());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol1.class);
    assertEquals(AccessControlList.WILDCARD_ACL_VALUE, acl.getAclString());

    //test with a default acl
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL, 
        "user2 group2");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol.class);
    assertEquals("user1 group1", acl.getAclString());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol1.class);
    assertEquals("user2 group2", acl.getAclString());
  }
}
