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
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.security.sasl.Sasl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSaslPropertiesResolver extends AbstractHadoopTestBase {

  private static final InetAddress LOCALHOST = new InetSocketAddress("127.0.0.1", 1).getAddress();

  private SaslPropertiesResolver resolver;

  @BeforeEach
  public void setup() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_RPC_PROTECTION, "privacy");
    resolver = new SaslPropertiesResolver();
    resolver.setConf(conf);
  }

  @Test
  public void testResolverDoesNotMutate() {
    assertPrivacyQop(resolver.getDefaultProperties());
    setAuthenticationQop(resolver.getDefaultProperties());
    // After changing the map returned by SaslPropertiesResolver, future maps are not affected
    assertPrivacyQop(resolver.getDefaultProperties());

    assertPrivacyQop(resolver.getClientProperties(LOCALHOST));
    setAuthenticationQop(resolver.getClientProperties(LOCALHOST));
    // After changing the map returned by SaslPropertiesResolver, future maps are not affected
    assertPrivacyQop(resolver.getClientProperties(LOCALHOST));

    assertPrivacyQop(resolver.getServerProperties(LOCALHOST));
    setAuthenticationQop(resolver.getServerProperties(LOCALHOST));
    // After changing the map returned by SaslPropertiesResolver, future maps are not affected
    assertPrivacyQop(resolver.getServerProperties(LOCALHOST));
  }

  private static void setAuthenticationQop(Map<String, String> saslProperties) {
    saslProperties.put(Sasl.QOP, SaslRpcServer.QualityOfProtection.AUTHENTICATION.getSaslQop());
  }

  private static void assertPrivacyQop(Map<String, String> saslProperties) {
    assertThat(SaslRpcServer.QualityOfProtection.PRIVACY.getSaslQop()).isEqualTo(
        saslProperties.get(Sasl.QOP));
  }

}
