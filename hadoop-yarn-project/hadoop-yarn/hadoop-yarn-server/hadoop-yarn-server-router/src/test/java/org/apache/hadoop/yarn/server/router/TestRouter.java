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
package org.apache.hadoop.yarn.server.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Router}.
 */
public class TestRouter {

  @Test
  public void testJVMMetricsService() {
    YarnConfiguration conf = new YarnConfiguration();
    Router router = new Router();
    router.init(conf);
    assertEquals(3, router.getServices().size());
  }

  @Test
  public void testServiceACLRefresh() {
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        true);
    String aclsString = "alice,bob users,wheel";
    conf.set("security.applicationclient.protocol.acl", aclsString);
    conf.set("security.resourcemanager-administration.protocol.acl",
        aclsString);

    Router router = new Router();
    router.init(conf);
    router.start();

    // verify service Acls refresh for RouterClientRMService
    ServiceAuthorizationManager clientRMServiceManager =
        router.clientRMProxyService.getServer().
        getServiceAuthorizationManager();
    verifyServiceACLsRefresh(clientRMServiceManager,
        org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
        aclsString);

    // verify service Acls refresh for RouterRMAdminService
    ServiceAuthorizationManager routerAdminServiceManager =
        router.rmAdminProxyService.getServer().getServiceAuthorizationManager();
    verifyServiceACLsRefresh(routerAdminServiceManager,
        org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB.class,
        aclsString);

    router.stop();

  }

  private void verifyServiceACLsRefresh(ServiceAuthorizationManager manager,
      Class<?> protocol, String aclString) {
    if (manager.getProtocolsWithAcls().size() == 0) {
      fail("Acls are not refreshed for protocol " + protocol);
    }
    for (Class<?> protocolClass : manager.getProtocolsWithAcls()) {
      AccessControlList accessList = manager.getProtocolsAcls(protocolClass);
      if (protocolClass == protocol) {
        Assert.assertEquals(accessList.getAclString(), aclString);
      }
    }
  }

}
