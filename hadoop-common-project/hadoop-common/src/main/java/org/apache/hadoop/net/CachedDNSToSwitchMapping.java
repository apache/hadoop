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

import java.util.ArrayList;
import java.util.HashMap;
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
public class CachedDNSToSwitchMapping extends AbstractDNSToSwitchMapping {
  private Map<String, String> cache = new ConcurrentHashMap<String, String>();

  /**
   * The uncached mapping
   */
  protected final DNSToSwitchMapping rawMapping;

  /**
   * cache a raw DNS mapping
   * @param rawMapping the raw mapping to cache
   */
  public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;
  }

  /**
   * @param names a list of hostnames to probe for being cached
   * @return the hosts from 'names' that have not been cached previously
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
   * Caches the resolved host:rack mappings. The two list
   * parameters must be of equal size.
   *
   * @param uncachedHosts a list of hosts that were uncached
   * @param resolvedHosts a list of resolved host entries where the element
   * at index(i) is the resolved value for the entry in uncachedHosts[i]
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
   * @param names a list of hostnames to look up (can be be empty)
   * @return the cached resolution of the list of hostnames/addresses.
   *  or null if any of the names are not currently in the cache
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

  @Override
  public List<String> resolve(List<String> names) {
    // normalize all input names to be in the form of IP addresses
    names = NetUtils.normalizeHostNames(names);

    List <String> result = new ArrayList<String>(names.size());
    if (names.isEmpty()) {
      return result;
    }

    List<String> uncachedHosts = getUncachedHosts(names);

    // Resolve the uncached hosts
    List<String> resolvedHosts = rawMapping.resolve(uncachedHosts);
    //cache them
    cacheResolvedHosts(uncachedHosts, resolvedHosts);
    //now look up the entire list in the cache
    return getCachedHosts(names);

  }

  /**
   * Get the (host x switch) map.
   * @return a copy of the cached map of hosts to rack
   */
  @Override
  public Map<String, String> getSwitchMap() {
    Map<String, String > switchMap = new HashMap<String, String>(cache);
    return switchMap;
  }


  @Override
  public String toString() {
    return "cached switch mapping relaying to " + rawMapping;
  }

  /**
   * Delegate the switch topology query to the raw mapping, via
   * {@link AbstractDNSToSwitchMapping#isMappingSingleSwitch(DNSToSwitchMapping)}
   * @return true iff the raw mapper is considered single-switch.
   */
  @Override
  public boolean isSingleSwitch() {
    return isMappingSingleSwitch(rawMapping);
  }
  
  @Override
  public void reloadCachedMappings() {
    cache.clear();
  }

  @Override
  public void reloadCachedMappings(List<String> names) {
    for (String name : names) {
      cache.remove(name);
    }
  }
}
