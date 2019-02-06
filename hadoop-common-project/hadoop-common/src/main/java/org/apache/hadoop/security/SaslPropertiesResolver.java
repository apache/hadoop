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

import java.net.InetAddress;
import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Provides SaslProperties to be used for a connection.
 * The default implementation is to read the values from configuration.
 * This class can be overridden to provide custom SaslProperties. 
 * The custom class can be specified via configuration.
 *
 */
public class SaslPropertiesResolver implements Configurable{
  private Map<String,String> properties;
  Configuration conf;

  /**
   * Returns an instance of SaslPropertiesResolver.
   * Looks up the configuration to see if there is custom class specified.
   * Constructs the instance by passing the configuration directly to the
   * constructor to achieve thread safety using final fields.
   * @param conf
   * @return SaslPropertiesResolver
   */
  public static SaslPropertiesResolver getInstance(Configuration conf) {
    Class<? extends SaslPropertiesResolver> clazz =
      conf.getClass(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
          SaslPropertiesResolver.class, SaslPropertiesResolver.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    properties = new TreeMap<String,String>();
    String[] qop = conf.getTrimmedStrings(
        CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION,
        QualityOfProtection.AUTHENTICATION.toString());
    for (int i=0; i < qop.length; i++) {
      qop[i] = QualityOfProtection.valueOf(
          StringUtils.toUpperCase(qop[i])).getSaslQop();
    }
    properties.put(Sasl.QOP, StringUtils.join(",", qop));
    properties.put(Sasl.SERVER_AUTH, "true");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * The default Sasl Properties read from the configuration
   * @return sasl Properties
   */
  public Map<String,String> getDefaultProperties() {
    return properties;
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a  client.
   * @param clientAddress client's address
   * @return the sasl properties to be used for the connection.
   */
  public Map<String, String> getServerProperties(InetAddress clientAddress){
    return properties;
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a  client.
   * @param clientAddress  client's address
   * @param ingressPort the port that the client is connecting
   * @return the sasl properties to be used for the connection.
   */
  public Map<String, String> getServerProperties(InetAddress clientAddress,
      int ingressPort){
    return getServerProperties(clientAddress);
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a server.
   * @param serverAddress server's address
   * @return the sasl properties to be used for the connection.
   */
  public Map<String, String> getClientProperties(InetAddress serverAddress){
    return properties;
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a server.
   * @param serverAddress server's address
   * @param ingressPort the port that is used to connect to server
   * @return the sasl properties to be used for the connection.
   */
  public Map<String, String> getClientProperties(InetAddress serverAddress,
      int ingressPort) {
    return getClientProperties(serverAddress);
  }

  /**
   * A util function to retrieve specific additional sasl property from config.
   * Used by subclasses to read sasl properties used by themselves.
   * @param conf the configuration
   * @param configKey the config key to look for
   * @param defaultQOP the default QOP if the key is missing
   * @return sasl property associated with the given key
   */
  static Map<String, String> getSaslProperties(Configuration conf,
      String configKey, QualityOfProtection defaultQOP) {
    Map<String, String> saslProps = new TreeMap<>();
    String[] qop = conf.getStrings(configKey, defaultQOP.toString());

    for (int i=0; i < qop.length; i++) {
      qop[i] = QualityOfProtection.valueOf(
          StringUtils.toUpperCase(qop[i])).getSaslQop();
    }

    saslProps.put(Sasl.QOP, StringUtils.join(",", qop));
    saslProps.put(Sasl.SERVER_AUTH, "true");

    return saslProps;
  }
}
