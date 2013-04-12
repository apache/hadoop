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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.SecurityUtil.QualifiedHostResolver;

/**
 * provides a dummy dns search resolver with a configurable search path
 * and host mapping
 */
public class NetUtilsTestResolver extends QualifiedHostResolver {
  Map<String, InetAddress> resolvedHosts = new HashMap<String, InetAddress>();
  List<String> hostSearches = new LinkedList<String>();

  public static NetUtilsTestResolver install() {
    NetUtilsTestResolver resolver = new NetUtilsTestResolver();
    resolver.setSearchDomains("a.b", "b", "c");
    resolver.addResolvedHost("host.a.b.", "1.1.1.1");
    resolver.addResolvedHost("b-host.b.", "2.2.2.2");
    resolver.addResolvedHost("simple.", "3.3.3.3");    
    SecurityUtil.hostResolver = resolver;
    return resolver;
  }

  public void addResolvedHost(String host, String ip) {
    InetAddress addr;
    try {
      addr = InetAddress.getByName(ip);
      addr = InetAddress.getByAddress(host, addr.getAddress());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("not an ip:"+ip);
    }
    resolvedHosts.put(host, addr);
  }

  @Override
  public InetAddress getInetAddressByName(String host) throws UnknownHostException {
    hostSearches.add(host);
    if (!resolvedHosts.containsKey(host)) {
      throw new UnknownHostException(host);
    }
    return resolvedHosts.get(host);
  }

  @Override
  public InetAddress getByExactName(String host) {
    return super.getByExactName(host);
  }
  
  @Override
  public InetAddress getByNameWithSearch(String host) {
    return super.getByNameWithSearch(host);
  }
  
  public String[] getHostSearches() {
    return hostSearches.toArray(new String[0]);
  }

  public void reset() {
    hostSearches.clear();
  }
}
