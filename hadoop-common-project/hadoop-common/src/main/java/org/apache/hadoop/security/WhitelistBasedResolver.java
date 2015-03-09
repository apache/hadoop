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
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;

import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.util.CombinedIPWhiteList;
import org.apache.hadoop.util.StringUtils;


/**
 * An implementation of the SaslPropertiesResolver.
 * Uses a white list of IPs.
 * If the connection's IP address is in the list of IP addresses, the salProperties
 * will be unchanged.
 * If the connection's IP is not in the list of IP addresses, then QOP for the
 * connection will be restricted to "hadoop.rpc.protection.non-whitelist"
 *
 * Uses 3 IPList implementations together to form an aggregate whitelist.
 * 1. ConstantIPList - to check against a set of hardcoded IPs
 * 2. Fixed IP List - to check against a list of IP addresses which are specified externally, but
 * will not change over runtime.
 * 3. Variable IP List - to check against a list of IP addresses which are specified externally and
 * could change during runtime.
 * A connection IP address will checked against these 3 IP Lists in the order specified above.
 * Once a match is found , the IP address is determined to be in whitelist.
 *
 * The behavior can be configured using a bunch of configuration parameters.
 *
 */
public class WhitelistBasedResolver extends SaslPropertiesResolver {
  public static final Log LOG = LogFactory.getLog(WhitelistBasedResolver.class);

  private static final String FIXEDWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/fixedwhitelist";

  private static final String VARIABLEWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/whitelist";

  /**
   * Path to the file to containing subnets and ip addresses to form fixed whitelist.
   */
  public static final String HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE =
    "hadoop.security.sasl.fixedwhitelist.file";
  /**
   * Enables/Disables variable whitelist
   */
  public static final String HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE =
    "hadoop.security.sasl.variablewhitelist.enable";
  /**
   * Path to the file to containing subnets and ip addresses to form variable whitelist.
   */
  public static final String HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE =
    "hadoop.security.sasl.variablewhitelist.file";
  /**
   * time in seconds by which the variable whitelist file is checked for updates
   */
  public static final String HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS =
    "hadoop.security.sasl.variablewhitelist.cache.secs";

  /**
   * comma separated list containing alternate hadoop.rpc.protection values for
   * clients which are not in whitelist
   */
  public static final String HADOOP_RPC_PROTECTION_NON_WHITELIST =
    "hadoop.rpc.protection.non-whitelist";

  private CombinedIPWhiteList whiteList;

  private Map<String, String> saslProps;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String fixedFile = conf.get(HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE,
        FIXEDWHITELIST_DEFAULT_LOCATION);
    String variableFile = null;
    long expiryTime = 0;

    if (conf.getBoolean(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE, false)) {
      variableFile = conf.get(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE,
          VARIABLEWHITELIST_DEFAULT_LOCATION);
      expiryTime =
        conf.getLong(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS,3600) * 1000;
    }

    whiteList = new CombinedIPWhiteList(fixedFile,variableFile,expiryTime);

    this.saslProps = getSaslProperties(conf);
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a client.
   * @param clientAddress client's address
   * @return the sasl properties to be used for the connection.
   */
  @Override
  public Map<String, String> getServerProperties(InetAddress clientAddress) {
    if (clientAddress == null) {
      return saslProps;
    }
    return  whiteList.isIn(clientAddress.getHostAddress())?getDefaultProperties():saslProps;
  }

  public Map<String, String> getServerProperties(String clientAddress) throws UnknownHostException {
    if (clientAddress == null) {
      return saslProps;
    }
    return getServerProperties(InetAddress.getByName(clientAddress));
  }

  static Map<String, String> getSaslProperties(Configuration conf) {
    Map<String, String> saslProps =new TreeMap<String, String>();
    String[] qop = conf.getStrings(HADOOP_RPC_PROTECTION_NON_WHITELIST,
        QualityOfProtection.PRIVACY.toString());

    for (int i=0; i < qop.length; i++) {
      qop[i] = QualityOfProtection.valueOf(
          StringUtils.toUpperCase(qop[i])).getSaslQop();
    }

    saslProps.put(Sasl.QOP, StringUtils.join(",", qop));
    saslProps.put(Sasl.SERVER_AUTH, "true");

    return saslProps;
  }
}
