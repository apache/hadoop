package org.apache.hadoop.security;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.security.sasl.Sasl;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION;
import static org.junit.Assert.assertEquals;

public class TestSaslPropertiesResolver {

  private static final SaslRpcServer.QualityOfProtection PRIVACY_QOP = SaslRpcServer.QualityOfProtection.PRIVACY;
  private static final SaslRpcServer.QualityOfProtection AUTHENTICATION_QOP = SaslRpcServer.QualityOfProtection.AUTHENTICATION;
  private static final InetAddress LOCALHOST = new InetSocketAddress("127.0.0.1", 1).getAddress();

  private SaslPropertiesResolver resolver;

  @Before
  public void setup() {
    Configuration conf = new Configuration();
    conf.set(HADOOP_RPC_PROTECTION, "privacy");
    resolver = new SaslPropertiesResolver();
    resolver.setConf(conf);
  }

  @Test
  public void testResolverDoesNotMutate() {
    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getDefaultProperties().get(Sasl.QOP));
    resolver.getDefaultProperties().put(Sasl.QOP, AUTHENTICATION_QOP.getSaslQop());
    // Even after changing the map returned by SaslPropertiesResolver, it does not change its future responses
    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getDefaultProperties().get(Sasl.QOP));

    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getClientProperties(LOCALHOST).get(Sasl.QOP));
    resolver.getDefaultProperties().put(Sasl.QOP, AUTHENTICATION_QOP.getSaslQop());
    // Even after changing the map returned by SaslPropertiesResolver, it does not change its future responses
    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getClientProperties(LOCALHOST).get(Sasl.QOP));

    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getServerProperties(LOCALHOST).get(Sasl.QOP));
    resolver.getDefaultProperties().put(Sasl.QOP, AUTHENTICATION_QOP.getSaslQop());
    // Even after changing the map returned by SaslPropertiesResolver, it does not change its future responses
    assertEquals(PRIVACY_QOP.getSaslQop(), resolver.getServerProperties(LOCALHOST).get(Sasl.QOP));
  }

}
