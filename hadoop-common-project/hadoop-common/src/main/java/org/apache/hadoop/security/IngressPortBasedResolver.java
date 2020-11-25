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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of SaslPropertiesResolver. Used on server side,
 * returns SASL properties based on the port the client is connecting
 * to. This should be used along with server side enabling multiple ports
 * TODO: when NN multiple listener is enabled, automatically use this
 * resolver without having to set in config.
 *
 * For configuration, for example if server runs on two ports 9000 and 9001,
 * and we want to specify 9000 to use auth-conf and 9001 to use auth.
 *
 * We need to set the following configuration properties:
 * ingress.port.sasl.configured.ports=9000,9001
 * ingress.port.sasl.prop.9000=privacy
 * ingress.port.sasl.prop.9001=authentication
 *
 * One note is that, if there is misconfiguration that a port, say, 9002 is
 * given in ingress.port.sasl.configured.ports, but it's sasl prop is not
 * set, a default of QOP of privacy (auth-conf) will be used. In addition,
 * if a port is not given even in ingress.port.sasl.configured.ports, but
 * is being checked in getServerProperties(), the default SASL prop will
 * be returned. Both of these two cases are considered misconfiguration.
 */
public class IngressPortBasedResolver extends SaslPropertiesResolver {

  public static final Logger LOG =
      LoggerFactory.getLogger(IngressPortBasedResolver.class.getName());

  static final String INGRESS_PORT_SASL_PROP_PREFIX = "ingress.port.sasl.prop";

  static final String INGRESS_PORT_SASL_CONFIGURED_PORTS =
      "ingress.port.sasl.configured.ports";

  // no need to concurrent map, because after setConf() it never change,
  // only for read.
  private HashMap<Integer, Map<String, String>> portPropMapping;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    portPropMapping = new HashMap<>();
    Collection<String> portStrings =
        conf.getTrimmedStringCollection(INGRESS_PORT_SASL_CONFIGURED_PORTS);
    for (String portString : portStrings) {
      int port = Integer.parseInt(portString);
      String configKey = INGRESS_PORT_SASL_PROP_PREFIX + "." + portString;
      Map<String, String> props = getSaslProperties(conf, configKey,
          SaslRpcServer.QualityOfProtection.PRIVACY);
      portPropMapping.put(port, props);
    }
    LOG.debug("Configured with port to QOP mapping as:" + portPropMapping);
  }

  /**
   * Identify the Sasl Properties to be used for a connection with a client.
   * @param clientAddress client's address
   * @param ingressPort the port that the client is connecting
   * @return the sasl properties to be used for the connection.
   */
  @Override
  @VisibleForTesting
  public Map<String, String> getServerProperties(InetAddress clientAddress,
      int ingressPort) {
    LOG.debug("Resolving SASL properties for " + clientAddress + " "
        + ingressPort);
    if (!portPropMapping.containsKey(ingressPort)) {
      LOG.warn("An un-configured port is being requested " + ingressPort
          + " using default");
      return getDefaultProperties();
    }
    return portPropMapping.get(ingressPort);
  }
}
