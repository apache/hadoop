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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This interface provides methods for the failover proxy to get IP addresses
 * of the associated servers (NameNodes, RBF routers etc). Implementations will
 * use their own service discovery mechanism, DNS, Zookeeper etc
 */
public interface DomainNameResolver {
  /**
   * Takes one domain name and returns its IP addresses based on the actual
   * service discovery methods.
   *
   * @param domainName
   * @return all IP addresses
   * @throws UnknownHostException
   */
  InetAddress[] getAllByDomainName(String domainName)
      throws UnknownHostException;

  /**
   * Reverse lookup an IP address and get the fully qualified domain name(fqdn).
   *
   * @param address
   * @return fully qualified domain name
   */
  String getHostnameByIP(InetAddress address);

  /**
   * This function combines getAllByDomainName and getHostnameByIP, for a single
   * domain name, it will first do a forward lookup to get all of IP addresses,
   * then for each IP address, it will do a reverse lookup to get the fqdn.
   * This function is necessary in secure environment since Kerberos uses fqdn
   * in the service principal instead of IP.
   *
   * @param domainName
   * @return all fully qualified domain names belonging to the IPs resolved from
   * the input domainName
   * @throws UnknownHostException
   */
   String[] getAllResolvedHostnameByDomainName(
       String domainName, boolean useFQDN) throws UnknownHostException;
}
