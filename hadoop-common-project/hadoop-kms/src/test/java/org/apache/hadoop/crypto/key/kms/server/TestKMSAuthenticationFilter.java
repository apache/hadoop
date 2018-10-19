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
import org.apache.hadoop.crypto.key.kms.KMSDelegationToken;
import org.apache.hadoop.security.token.delegation.web
    .DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web
    .PseudoDelegationTokenAuthenticationHandler;
import org.junit.Test;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Test KMS Authentication Filter.
 */
public class TestKMSAuthenticationFilter {

  @Test public void testConfiguration() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.kms.authentication.type", "simple");

    Properties prop = new KMSAuthenticationFilter().getKMSConfiguration(conf);
    assertEquals(prop.getProperty(KMSAuthenticationFilter.AUTH_TYPE),
        PseudoDelegationTokenAuthenticationHandler.class.getName());
    assertEquals(
        prop.getProperty(DelegationTokenAuthenticationHandler.TOKEN_KIND),
        KMSDelegationToken.TOKEN_KIND_STR);
  }
}

