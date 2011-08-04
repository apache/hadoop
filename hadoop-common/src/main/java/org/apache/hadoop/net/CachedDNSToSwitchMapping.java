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
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in 
 * a cache. The following calls to a resolved network location
 * will get its location from the cache. 
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachedDNSToSwitchMapping implements DNSToSwitchMapping {
  private Map<String, String> cache = new ConcurrentHashMap<String, String>();
  protected DNSToSwitchMapping rawMapping;
  
  public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;
  }
  

  /**
   * Returns the hosts from 'names' that have not been cached previously
   */
  private List<String> getUncachedHosts(List<String> names) {
    // find out all names without cached resolved location
    List<String> unCachedHosts = new ArrayList<String>(names.size());
    for (String name : names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name);
      } 
    }
    return unCachedHosts;
  }

  /**
   * Caches the resolved hosts
   */
  private void cacheResolvedHosts(List<String> uncachedHosts, 
      List<String> resolvedHosts) {
    // Cache the result
    if (resolvedHosts != null) {
      for (int i=0; i<uncachedHosts.size(); i++) {
        cache.put(uncachedHosts.get(i), resolvedHosts.get(i));
      }
    }
  }

  /**
   * Returns the cached resolution of the list of hostnames/addresses.
   * Returns null if any of the names are not currently in the cache
   */
  private List<String> getCachedHosts(List<String> names) {
    List<String> result = new ArrayList<String>(names.size());
    // Construct the result
    for (String name : names) {
      String networkLocation = cache.get(name);
      if (networkLocation != null) {
        result.add(networkLocation);
      } else {
        return null;
      }
    }
    return result;
  }

  /**
   * Resolves host names and adds them to the cache.
   * Unlike the 'resolve" method, this won't hide UnknownHostExceptions
   * 
   * @param names to resolve
   * @return List of resolved names
   * @throws UnknownHostException if any hosts cannot be resolved
   */  
  public List<String> resolveValidHosts(List<String> names) 
    throws UnknownHostException {
    if (names.isEmpty()) {
      return new ArrayList<String>();
    }
    List<String> addresses = new ArrayList<String>(names.size());
    for (String name : names) {
      addresses.add(InetAddress.getByName(name).getHostAddress());
    }

    List<String> uncachedHosts = this.getUncachedHosts(names);

    // Resolve the uncached hosts
    List<String> resolvedHosts = rawMapping.resolveValidHosts(uncachedHosts);
    this.cacheResolvedHosts(uncachedHosts, resolvedHosts);
    return this.getCachedHosts(addresses);
  }

  public List<String> resolve(List<String> names) {
    // normalize all input names to be in the form of IP addresses
    names = NetUtils.normalizeHostNames(names);

    List <String> result = new ArrayList<String>(names.size());
    if (names.isEmpty()) {
      return result;
    }

    List<String> uncachedHosts = this.getUncachedHosts(names);

    // Resolve the uncached hosts
    List<String> resolvedHosts = rawMapping.resolve(uncachedHosts);
    this.cacheResolvedHosts(uncachedHosts, resolvedHosts);
    return this.getCachedHosts(names);

  }
}
