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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;

/**
 * Class that caches the netgroups and inverts group-to-user map
 * to user-to-group map, primarily intended for use with
 * netgroups (as returned by getent netgrgoup) which only returns
 * group to user mapping.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetgroupCache {
  private final static ConcurrentHashMap<String, Long> GROUPS_LOOKUP =
      new ConcurrentHashMap<>();
  private final static ConcurrentHashMap<String, Set<String>>
      USER_TO_NETGROUPS_MAP = new ConcurrentHashMap<>();

  /**
   * Get netgroups for a given user
   *
   * @param user get groups for this user
   * @param groups put groups into this List
   */
  public static void getNetgroups(final String user,
      Collection<String> groups) {
    Set<String> userGroups = USER_TO_NETGROUPS_MAP.get(user);
    // ConcurrentHashMap does not allow null values;
    // So null value check can be used to check if the key exists
    if (userGroups != null) {
      groups.addAll(userGroups);
    }
  }

  /**
   * Get the list of cached netgroups
   *
   * @return set of cached groups
   */
  public static Set<String> getNetgroupNames() {
    return getGroups().stream().collect(Collectors.toSet());
  }

  private static Set<String> getGroups() {
    return GROUPS_LOOKUP.keySet();
  }

  @VisibleForTesting
  protected static Collection<String> getCachedGroupsForTesting() {
    Set<String> cachedGroups = new LinkedHashSet<>();
    USER_TO_NETGROUPS_MAP.values().forEach(cachedGroups::addAll);
    return cachedGroups;
  }

  /**
   * Returns true if a given netgroup is cached
   *
   * @param group check if this group is cached
   * @return true if group is cached, false otherwise
   */
  public static boolean isCached(String group) {
    return GROUPS_LOOKUP.containsKey(group)
        && GROUPS_LOOKUP.get(group) > 0;
  }

  /**
   * Clear the cache
   */
  public static void clear() {
    USER_TO_NETGROUPS_MAP.clear();
    GROUPS_LOOKUP.replaceAll((k, v) -> 0L);
  }

  protected static void clearDataForTesting() {
    clear();
    GROUPS_LOOKUP.clear();
  }

  private static void cacheGroupInternal(String group) {
    if (!isCached(group)) {
      GROUPS_LOOKUP.put(group, Time.monotonicNow());
    }
  }
  /**
   * Add group to cache
   *
   * @param group name of the group to add to cache
   * @param users set of users for a given group.
   */
  public static void add(String group, Collection<String> users) {
    cacheGroupInternal(group);
    for (String user : users) {
      USER_TO_NETGROUPS_MAP.computeIfAbsent(user,
          groupName -> (ConcurrentHashMap.newKeySet())).add(group);
    }
  }

  public static void refreshCacheCB(NetgroupCacheListAdder bulkAdder)
      throws IOException {
    NetgroupCacheFaultInjector.get().checkPointResettingBeforeClearing();
    clear();
    bulkAdder.addGroupToCacheInBulk(getGroups());
  }

  static class NetgroupCacheListAdder {
    boolean isCacheableKey(String entryKey, boolean forceRefresh) {
      if (entryKey.length() > 0) {
        if (entryKey.charAt(0) == '@') { // unix group, not caching
          return (forceRefresh || !isCached(entryKey));
        }
      }
      return false;
    }

    Collection<String> getValuesForEntryKey(String entryKey)
        throws IOException {
      throw new UnsupportedOperationException(
          "getValuesForEntryKey is not implemented");
    }

    void addGroupToCacheInBulkInternal(Collection<String> groups)
        throws IOException {
      addGroupToCacheInBulkInternal(groups, false);
    }

    void addGroupToCacheInBulkInternal(Collection<String> groups,
        boolean refresh) throws IOException {
      for (String group : groups) {
        if (isCacheableKey(group, refresh)) {
          NetgroupCache.add(group, getValuesForEntryKey(group));
        }
      }
    }

    void addGroupToCacheInBulk(Collection<String> groups) throws IOException {
      addGroupToCacheInBulkInternal(groups);
      NetgroupCacheFaultInjector.get().checkPointPostAddingBulkGroup(groups);
    }

    void refreshCachedGroups() throws IOException {
      refreshCacheCB(this);
    }
  }
}
