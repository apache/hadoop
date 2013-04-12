/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

public class TestProxyUserFromEnv {
  /** Test HADOOP_PROXY_USER for impersonation */
  @Test
  public void testProxyUserFromEnvironment() throws IOException {
    String proxyUser = "foo.bar";
    System.setProperty(UserGroupInformation.HADOOP_PROXY_USER, proxyUser);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertEquals(proxyUser, ugi.getUserName());

    UserGroupInformation realUgi = ugi.getRealUser();
    assertNotNull(realUgi);
    // get the expected real user name
    Process pp = Runtime.getRuntime().exec("whoami");
    BufferedReader br = new BufferedReader
                          (new InputStreamReader(pp.getInputStream()));
    String realUser = br.readLine().trim();
    assertEquals(realUser, realUgi.getUserName());
  }
}
