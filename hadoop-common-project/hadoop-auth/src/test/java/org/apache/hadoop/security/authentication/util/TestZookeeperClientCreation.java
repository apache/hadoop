/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.security.authentication.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ConfigurableZookeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.hadoop.security.authentication.util.ZookeeperClient.SASLOwnerACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ClientX509Util;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for ZookeeperClient class, to check if it creates CuratorFramework by providing expected
 * parameter values to the CuratorFrameworkFactory.Builder instance.
 */
public class TestZookeeperClientCreation {

  private ZookeeperClient clientConfigurer;
  private CuratorFrameworkFactory.Builder cfBuilder;

  @BeforeEach
  public void setup() {
    clientConfigurer = spy(ZookeeperClient.configure());
    clientConfigurer.withConnectionString("dummy");
    cfBuilder = spy(CuratorFrameworkFactory.builder());

    when(clientConfigurer.createFrameworkFactoryBuilder()).thenReturn(cfBuilder);
  }

  //Positive tests
  @Test
  public void testConnectionStringSet() {
    clientConfigurer.withConnectionString("conn").create();

    verify(cfBuilder).connectString("conn");

    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }

  @Test
  public void testZookeeperFactorySet() {
    ZookeeperFactory zkFactory = mock(ZookeeperFactory.class);
    clientConfigurer.withZookeeperFactory(zkFactory).create();

    verify(cfBuilder).zookeeperFactory(zkFactory);

    verifyDummyConnectionString();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }

  @Test
  public void testNameSpaceSet() {
    clientConfigurer.withNamespace("someNS/someSubSpace").create();

    verify(cfBuilder).namespace("someNS/someSubSpace");

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }

  @Test
  public void testSessionTimeoutSet() {
    clientConfigurer.withSessionTimeout(20000).create();

    verify(cfBuilder).sessionTimeoutMs(20000);

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }


  @Test
  public void testDefaultSessionTimeoutIsAffectedBySystemProperty() {
    System.setProperty("curator-default-session-timeout", "20000");
    setup();
    clientConfigurer.create();

    verify(cfBuilder).sessionTimeoutMs(20000);

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
    System.clearProperty("curator-default-session-timeout");
  }

  @Test
  public void testConnectionTimeoutSet() {
    clientConfigurer.withConnectionTimeout(50).create();

    verify(cfBuilder).connectionTimeoutMs(50);

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }

  @Test
  public void testDefaultConnectionTimeoutIsAffectedBySystemProperty() {
    System.setProperty("curator-default-connection-timeout", "50");
    setup();
    clientConfigurer.create();

    verify(cfBuilder).connectionTimeoutMs(50);

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
    System.clearProperty("curator-default-connection-timeout");
  }

  @Test
  public void testRetryPolicySet() {
    RetryPolicy policy = mock(RetryPolicy.class);
    clientConfigurer.withRetryPolicy(policy).create();

    verify(cfBuilder).retryPolicy(policy);

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultAclProvider();
    verifyDefaultZKClientConfig();
  }

  @Test
  public void testSaslAutTypeWithIBMJava() {
    testSaslAuthType("IBMJava");
  }

  @Test
  public void testSaslAuthTypeWithNonIBMJava() {
    testSaslAuthType("OracleJava");
  }

  @Test
  public void testSSLConfiguration() {
    clientConfigurer
        .enableSSL(true)
        .withKeystore("keystoreLoc")
        .withKeystorePassword("ksPass")
        .withTruststore("truststoreLoc")
        .withTruststorePassword("tsPass")
        .create();

    ArgumentCaptor<ZKClientConfig> clientConfCaptor = forClass(ZKClientConfig.class);
    verify(cfBuilder).zkClientConfig(clientConfCaptor.capture());
    ZKClientConfig conf = clientConfCaptor.getValue();

    assertThat(conf.getProperty(ZKClientConfig.SECURE_CLIENT), is("true"));
    assertThat(conf.getProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET),
        is("org.apache.zookeeper.ClientCnxnSocketNetty"));
    try (ClientX509Util sslOpts = new ClientX509Util()) {
      assertThat(conf.getProperty(sslOpts.getSslKeystoreLocationProperty()), is("keystoreLoc"));
      assertThat(conf.getProperty(sslOpts.getSslKeystorePasswdProperty()), is("ksPass"));
      assertThat(conf.getProperty(sslOpts.getSslTruststoreLocationProperty()), is("truststoreLoc"));
      assertThat(conf.getProperty(sslOpts.getSslTruststorePasswdProperty()), is("tsPass"));
    }

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultAclProvider();
  }

  //Negative tests
  @Test
  public void testNoConnectionString(){
    clientConfigurer.withConnectionString(null);

    Throwable t = assertThrows(NullPointerException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), containsString("Zookeeper connection string cannot be null!"));
  }

  @Test
  public void testNoRetryPolicy() {
    clientConfigurer.withRetryPolicy(null);

    Throwable t = assertThrows(NullPointerException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), containsString("Zookeeper connection retry policy cannot be null!"));
  }

  @Test
  public void testNoAuthType() {
    clientConfigurer.withAuthType(null);

    Throwable t = assertThrows(NullPointerException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), containsString("Zookeeper authType cannot be null!"));
  }

  @Test
  public void testUnrecognizedAuthType() {
    clientConfigurer.withAuthType("something");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("Zookeeper authType must be one of [none, sasl]!"));
  }

  @Test
  public void testSaslAuthTypeWithoutKeytab() {
    clientConfigurer.withAuthType("sasl");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("Zookeeper client's Kerberos Keytab must be specified!"));
  }

  @Test
  public void testSaslAuthTypeWithEmptyKeytab() {
    clientConfigurer
        .withAuthType("sasl")
        .withKeytab("");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("Zookeeper client's Kerberos Keytab must be specified!"));
  }

  @Test
  public void testSaslAuthTypeWithoutPrincipal() {
    clientConfigurer
        .withAuthType("sasl")
        .withKeytab("keytabLoc");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("Zookeeper client's Kerberos Principal must be specified!"));
  }

  @Test
  public void testSaslAuthTypeWithEmptyPrincipal() {
    clientConfigurer
        .withAuthType("sasl")
        .withKeytab("keytabLoc")
        .withPrincipal("");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("Zookeeper client's Kerberos Principal must be specified!"));
  }

  @Test
  public void testSaslAuthTypeWithoutJaasLoginEntryName() {
    clientConfigurer
        .withAuthType("sasl")
        .withKeytab("keytabLoc")
        .withPrincipal("principal")
        .withJaasLoginEntryName(null);

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("JAAS Login Entry name must be specified!"));
  }

  @Test
  public void testSaslAuthTypeWithEmptyJaasLoginEntryName() {
    clientConfigurer
        .withAuthType("sasl")
        .withKeytab("keytabLoc")
        .withPrincipal("principal")
        .withJaasLoginEntryName("");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(), is("JAAS Login Entry name must be specified!"));
  }

  @Test
  public void testSSLWithoutKeystore() {
    clientConfigurer
        .enableSSL(true);

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(),
        is("The keystore location parameter is empty for the ZooKeeper client connection."));
  }

  @Test
  public void testSSLWithEmptyKeystore() {
    clientConfigurer
        .enableSSL(true)
        .withKeystore("");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(),
        is("The keystore location parameter is empty for the ZooKeeper client connection."));
  }

  @Test
  public void testSSLWithoutTruststore() {
    clientConfigurer
        .enableSSL(true)
        .withKeystore("keyStoreLoc");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(),
        is("The truststore location parameter is empty for the ZooKeeper client connection."));
  }

  @Test
  public void testSSLWithEmptyTruststore() {
    clientConfigurer
        .enableSSL(true)
        .withKeystore("keyStoreLoc")
        .withTruststore("");

    Throwable t = assertThrows(IllegalArgumentException.class, () -> clientConfigurer.create());
    assertThat(t.getMessage(),
        is("The truststore location parameter is empty for the ZooKeeper client connection."));
  }

  private void testSaslAuthType(String vendor) {
    String origVendor = System.getProperty("java.vendor");
    System.setProperty("java.vendor", vendor);
    Configuration origConf = Configuration.getConfiguration();

    try {
      clientConfigurer
          .withAuthType("sasl")
          .withKeytab("keytabLoc")
          .withPrincipal("principal@some.host/SOME.REALM")
          .withJaasLoginEntryName("TestEntry")
          .create();

      ArgumentCaptor<SASLOwnerACLProvider> aclProviderCaptor = forClass(SASLOwnerACLProvider.class);
      verify(cfBuilder).aclProvider(aclProviderCaptor.capture());
      SASLOwnerACLProvider aclProvider = aclProviderCaptor.getValue();

      assertThat(aclProvider.getDefaultAcl().size(), is(1));
      assertThat(aclProvider.getDefaultAcl().get(0).getId().getScheme(), is("sasl"));
      assertThat(aclProvider.getDefaultAcl().get(0).getId().getId(), is("principal"));
      assertThat(aclProvider.getDefaultAcl().get(0).getPerms(), is(ZooDefs.Perms.ALL));

      Arrays.stream(new String[] {"/", "/foo", "/foo/bar/baz", "/random/path"})
          .forEach(s -> {
            assertThat(aclProvider.getAclForPath(s).size(), is(1));
            assertThat(aclProvider.getAclForPath(s).get(0).getId().getScheme(), is("sasl"));
            assertThat(aclProvider.getAclForPath(s).get(0).getId().getId(), is("principal"));
            assertThat(aclProvider.getAclForPath(s).get(0).getPerms(), is(ZooDefs.Perms.ALL));
          });

      assertThat(System.getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY), is("TestEntry"));
      assertThat(System.getProperty("zookeeper.authProvider.1"),
          is("org.apache.zookeeper.server.auth.SASLAuthenticationProvider"));

      Configuration config = Configuration.getConfiguration();
      assertThat(config.getAppConfigurationEntry("TestEntry").length, is(1));
      AppConfigurationEntry entry = config.getAppConfigurationEntry("TestEntry")[0];
      assertThat(entry.getOptions().get("keyTab"), is("keytabLoc"));
      assertThat(entry.getOptions().get("principal"), is("principal@some.host/SOME.REALM"));
      assertThat(entry.getOptions().get("useKeyTab"), is("true"));
      assertThat(entry.getOptions().get("storeKey"), is("true"));
      assertThat(entry.getOptions().get("useTicketCache"), is("false"));
      assertThat(entry.getOptions().get("refreshKrb5Config"), is("true"));
      if (System.getProperty("java.vendor").contains("IBM")){
        assertThat(entry.getLoginModuleName(), is("com.ibm.security.auth.module.Krb5LoginModule"));
      } else {
        assertThat(entry.getLoginModuleName(), is("com.sun.security.auth.module.Krb5LoginModule"));
      }
    } finally {
      Configuration.setConfiguration(origConf);
      System.setProperty("java.vendor", origVendor);
    }

    verifyDummyConnectionString();
    verifyDefaultZKFactory();
    verifyDefaultNamespace();
    verifyDefaultSessionTimeout();
    verifyDefaultConnectionTimeout();
    verifyDefaultRetryPolicy();
    verifyDefaultZKClientConfig();
  }

  private void verifyDummyConnectionString() {
    verify(cfBuilder).connectString("dummy");
  }

  private void verifyDefaultNamespace() {
    verify(cfBuilder).namespace(null);
  }

  private void verifyDefaultZKFactory() {
    verify(cfBuilder).zookeeperFactory(isA(ConfigurableZookeeperFactory.class));
  }

  private void verifyDefaultSessionTimeout() {
    verify(cfBuilder).sessionTimeoutMs(60000);
  }

  private void verifyDefaultConnectionTimeout() {
    verify(cfBuilder).connectionTimeoutMs(15000);
  }

  private void verifyDefaultRetryPolicy() {
    ArgumentCaptor<ExponentialBackoffRetry> retry = forClass(ExponentialBackoffRetry.class);
    verify(cfBuilder).retryPolicy(retry.capture());
    ExponentialBackoffRetry policy = retry.getValue();

    assertThat(policy.getBaseSleepTimeMs(), is(1000));
    assertThat(policy.getN(), is(3));
  }

  private void verifyDefaultAclProvider() {
    verify(cfBuilder).aclProvider(isA(DefaultACLProvider.class));
  }

  private void verifyDefaultZKClientConfig() {
    ArgumentCaptor<ZKClientConfig> clientConfCaptor = forClass(ZKClientConfig.class);
    verify(cfBuilder).zkClientConfig(clientConfCaptor.capture());
    ZKClientConfig conf = clientConfCaptor.getValue();

    assertThat(conf.getProperty(ZKClientConfig.SECURE_CLIENT), isEmptyOrFalse());
    try (ClientX509Util sslOpts = new ClientX509Util()) {
      assertThat(conf.getProperty(sslOpts.getSslKeystoreLocationProperty()), isEmpty());
      assertThat(conf.getProperty(sslOpts.getSslKeystorePasswdProperty()), isEmpty());
      assertThat(conf.getProperty(sslOpts.getSslTruststoreLocationProperty()), isEmpty());
      assertThat(conf.getProperty(sslOpts.getSslTruststorePasswdProperty()), isEmpty());
    }
  }

  private Matcher<String> isEmptyOrFalse() {
    return anyOf(isEmpty(), is("false"));
  }

  private Matcher<String> isEmpty() {
    return anyOf(new IsNull<>(), is(""));
  }

}
