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

package org.apache.hadoop.yarn.service.client;

import static org.junit.Assert.*;

import java.io.File;

import javax.security.sasl.Sasl;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Spnego Client Login.
 */
public class TestSecureApiServiceClient extends KerberosSecurityTestcase {

  private String clientPrincipal = "client";

  private String server1Protocol = "HTTP";

  private String server2Protocol = "server2";

  private String host = "localhost";

  private String server1Principal = server1Protocol + "/" + host;

  private String server2Principal = server2Protocol + "/" + host;

  private File keytabFile;

  private Configuration conf = new Configuration();

  private Map<String, String> props;

  @Before
  public void setUp() throws Exception {
    keytabFile = new File(getWorkDir(), "keytab");
    getKdc().createPrincipal(keytabFile, clientPrincipal, server1Principal,
        server2Principal);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
  }

  @Test
  public void testHttpSpnegoChallenge() throws Exception {
    UserGroupInformation.loginUserFromKeytab(clientPrincipal, keytabFile
        .getCanonicalPath());
    ApiServiceClient asc = new ApiServiceClient();
    String challenge = asc.generateToken("localhost");
    assertNotNull(challenge);
  }

}
