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

package org.apache.hadoop.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * DNSDomainNameResolver wraps up the default DNS service for forward/reverse
 * DNS lookup. It also provides a function to resolve a host name to all of
 * fully qualified domain names belonging to the IPs from this host name
 */
public class DNSDomainNameResolver implements DomainNameResolver {

  private final static Logger LOG =
      LoggerFactory.getLogger(DNSDomainNameResolver.class.getName());

  @Override
  public InetAddress[] getAllByDomainName(String domainName)
      throws UnknownHostException {
    return InetAddress.getAllByName(domainName);
  }

  @Override
  public String getHostnameByIP(InetAddress address) {
    String host = address.getCanonicalHostName();
    if (host != null && host.length() != 0
        && host.charAt(host.length()-1) == '.') {
      host = host.substring(0, host.length()-1);
    }
    // Protect against the Java behaviour of returning the IP address as a string from a cache
    // instead of performing a reverse lookup.
    if (host != null && host.equals(address.getHostAddress())) {
      LOG.debug("IP address returned for FQDN detected: {}", address.getHostAddress());
      try {
        return DNS.reverseDns(address, null);
      } catch (NamingException lookupFailure) {
        LOG.warn("Failed to perform reverse lookup: {}", address);
      }
    }
    return host;
  }

  @Override
  public String[] getAllResolvedHostnameByDomainName(
      String domainName, boolean useFQDN) throws UnknownHostException {
    InetAddress[] addresses = getAllByDomainName(domainName);
    String[] hosts = new String[addresses.length];
    if (useFQDN) {
      for (int i = 0; i < addresses.length; i++) {
        hosts[i] = getHostnameByIP(addresses[i]);
      }
    } else {
      for (int i = 0; i < addresses.length; i++) {
        hosts[i] = addresses[i].getHostAddress();
      }
    }

    return hosts;
  }
}
