/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.router.web.RouterWebHDFSContract;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import org.junit.rules.ExpectedException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.router.SecurityConfUtil.initSecurity;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KEYTAB_FILE_KEY;


/**
 * Test secure router start up scenarios.
 */
public class TestRouterWithSecureStartup {

  private static final String HTTP_KERBEROS_PRINCIPAL_CONF_KEY =
      "hadoop.http.authentication.kerberos.principal";

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  /*
   * hadoop.http.authentication.kerberos.principal has default value, so if we
   * don't config the spnego principal, cluster will still start normally
   */
  @Test
  public void testStartupWithoutSpnegoPrincipal() throws Exception {
    Configuration conf = initSecurity();
    conf.unset(HTTP_KERBEROS_PRINCIPAL_CONF_KEY);
    RouterWebHDFSContract.createCluster(conf);
    assertNotNull(RouterWebHDFSContract.getCluster());
  }

  @Test
  public void testStartupWithoutKeytab() throws Exception {
    testCluster(DFS_ROUTER_KEYTAB_FILE_KEY,
        "Running in secure mode, but config doesn't have a keytab");
  }

  @Test
  public void testSuccessfulStartup() throws Exception {
    Configuration conf = initSecurity();
    RouterWebHDFSContract.createCluster(conf);
    assertNotNull(RouterWebHDFSContract.getCluster());
  }

  private void testCluster(String configToTest, String message)
      throws Exception {
    Configuration conf = initSecurity();
    conf.unset(configToTest);
    exceptionRule.expect(IOException.class);
    exceptionRule.expectMessage(message);
    RouterWebHDFSContract.createCluster(conf);
  }
}
