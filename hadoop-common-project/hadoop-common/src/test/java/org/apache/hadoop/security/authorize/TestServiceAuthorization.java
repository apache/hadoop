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
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class TestServiceAuthorization {

  private static final String ACL_CONFIG = "test.protocol.acl";
  private static final String ACL_CONFIG1 = "test.protocol1.acl";
  private static final String ADDRESS =  "0.0.0.0";
  private static final String HOST_CONFIG = "test.protocol.hosts";
  private static final String BLOCKED_HOST_CONFIG = "test.protocol.hosts.blocked";
  private static final String AUTHORIZED_IP = "1.2.3.4";
  private static final String UNAUTHORIZED_IP = "1.2.3.5";
  private static final String IP_RANGE = "10.222.0.0/16,10.113.221.221";

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
    // test without setting a default acl
    conf.set(ACL_CONFIG, "user1 group1");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    AccessControlList acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol.class);
    assertEquals("user1 group1", acl.getAclString());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol1.class);
    assertEquals(AccessControlList.WILDCARD_ACL_VALUE, acl.getAclString());

    // test with a default acl
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_ACL, 
        "user2 group2");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol.class);
    assertEquals("user1 group1", acl.getAclString());
    acl = serviceAuthorizationManager.getProtocolsAcls(TestProtocol1.class);
    assertEquals("user2 group2", acl.getAclString());
  }

  @Test
  public void testBlockedAcl() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });

    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();

    // test without setting a blocked acl
    conf.set(ACL_CONFIG, "user1 group1");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }
    // now set a blocked acl with another user and another group
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "drwho2 group3");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }
    // now set a blocked acl with the user and another group
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "drwho group3");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
      fail();
    } catch (AuthorizationException e) {

    }
    // now set a blocked acl with another user and another group
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "drwho2 group3");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }
    // now set a blocked acl with another user and group that the user belongs to
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "drwho2 group2");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
      fail();
    } catch (AuthorizationException e) {
      // expects Exception
    }
    // reset blocked acl so that there is no blocked ACL
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }
  }

  @Test
  public void testDefaultBlockedAcl() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });

    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();

    // test without setting a default blocked acl
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol1.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }

    // set a restrictive default blocked acl and an non-restricting blocked acl for TestProtocol
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_DEFAULT_BLOCKED_ACL,
        "user2 group2");
    conf.set(ACL_CONFIG + ServiceAuthorizationManager.BLOCKED, "user2");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    // drwho is authorized to access TestProtocol
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(ADDRESS));
    } catch (AuthorizationException e) {
      fail();
    }
    // drwho is not authorized to access TestProtocol1 because it uses the default blocked acl.
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol1.class, conf,
          InetAddress.getByName(ADDRESS));
      fail();
    } catch (AuthorizationException e) {
      // expects Exception
    }
  }

  @Test
  public void testMachineList() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });
    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();
    conf.set(HOST_CONFIG, "1.2.3.4");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(AUTHORIZED_IP));
    } catch (AuthorizationException e) {
      fail();
    }
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(UNAUTHORIZED_IP));
      fail();
    } catch (AuthorizationException e) {
    // expects Exception
    }
  }

  @Test
  public void testDefaultMachineList() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });
    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();
    // test without setting a default MachineList
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(UNAUTHORIZED_IP));
    } catch (AuthorizationException e) {
      fail();
    }
    // test with a default MachineList
    conf.set(
        "security.service.authorization.default.hosts",
        IP_RANGE);
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName(UNAUTHORIZED_IP));
      fail();
    } catch (AuthorizationException e) {
    // expects Exception
    }
    try {
      serviceAuthorizationManager.authorize(drwho, TestProtocol.class, conf,
          InetAddress.getByName("10.222.0.0"));
    } catch (AuthorizationException e) {
      fail();
    }
  }

  @Test
  public void testBlockedMachineList() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });

    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();

    // test without setting a blocked MachineList
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol.class, conf, InetAddress.getByName("10.222.0.0"));
    } catch (AuthorizationException e) {
      fail();
    }
    // now set a blocked MachineList
    conf.set(BLOCKED_HOST_CONFIG, IP_RANGE);
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol.class, conf, InetAddress.getByName("10.222.0.0"));
      fail();
    } catch (AuthorizationException e) {
    // expects Exception
    }
    // reset blocked MachineList
    conf.set(BLOCKED_HOST_CONFIG, "");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol.class, conf, InetAddress.getByName("10.222.0.0"));
    } catch (AuthorizationException e) {
      fail();
    }
  }

  @Test
  public void testDefaultBlockedMachineList() throws UnknownHostException {
    UserGroupInformation drwho =
        UserGroupInformation.createUserForTesting("drwho@EXAMPLE.COM",
            new String[] { "group1", "group2" });

    ServiceAuthorizationManager serviceAuthorizationManager =
        new ServiceAuthorizationManager();
    Configuration conf = new Configuration ();

    // test without setting a default blocked MachineList
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol1.class, conf, InetAddress.getByName("10.222.0.0"));
    } catch (AuthorizationException e) {
      fail();
    }
    // set a  default blocked MachineList and a blocked MachineList for TestProtocol
    conf.set(
        "security.service.authorization.default.hosts.blocked",
        IP_RANGE);
    conf.set(BLOCKED_HOST_CONFIG, "1.2.3.4");
    serviceAuthorizationManager.refresh(conf, new TestPolicyProvider());
    // TestProtocol can be accessed from "10.222.0.0" because it blocks only "1.2.3.4"
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol.class, conf, InetAddress.getByName("10.222.0.0"));
    } catch (AuthorizationException e) {
      fail();
    }
    // TestProtocol cannot be accessed from  "1.2.3.4"
    try {
      serviceAuthorizationManager.authorize(drwho,
          TestProtocol.class, conf, InetAddress.getByName("1.2.3.4"));
      fail();
    } catch (AuthorizationException e) {
      //expects Exception
    }
    // TestProtocol1 can be accessed from "1.2.3.4" because it uses default block list
    try {
      serviceAuthorizationManager.authorize(drwho,
         TestProtocol1.class, conf, InetAddress.getByName("1.2.3.4"));
    } catch (AuthorizationException e) {
      fail();
    }
    // TestProtocol1 cannot be accessed from "10.222.0.0", 
    // because "10.222.0.0" is in default block list
    try {
      serviceAuthorizationManager.authorize(drwho,
         TestProtocol1.class, conf, InetAddress.getByName("10.222.0.0"));
      fail();
    } catch (AuthorizationException e) {
      //expects Exception
    }
  }
}
