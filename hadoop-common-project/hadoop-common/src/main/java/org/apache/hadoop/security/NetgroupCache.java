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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
  /**
   * a static lookup of all the groups that have been fetched.
   * Note that a group with empty users won't be added.
   */
  private final static ConcurrentHashMap<String, Long> GROUPS_LOOKUP =
      new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, Set<String>> userToNetgroupsMap =
    new ConcurrentHashMap<String, Set<String>>();

  /**
   * Get netgroups for a given user
   *
   * @param user get groups for this user
   * @param groups put groups into this List
   */
  public static void getNetgroups(final String user,
      List<String> groups) {
    Set<String> userGroups = userToNetgroupsMap.get(user);
    //ConcurrentHashMap does not allow null values; 
    //So null value check can be used to check if the key exists
    if (userGroups != null) {
      groups.addAll(userGroups);
    }
  }

  /**
   * Get netgroups for a given user
   *
   * @param user get groups for this user
   * @param groups add user's netgroups into this set of groups
   */
  public static void getNetgroups(final String user,
      Set<String> groups) {
    Set<String> userGroups = userToNetgroupsMap.get(user);
    //ConcurrentHashMap does not allow null values;
    //So null value check can be used to check if the key exists
    if (userGroups != null) {
      groups.addAll(userGroups);
    }
  }

  /**
   * Get the list of cached netgroups
   *
   * @return list of cached groups
   */
  public static List<String> getNetgroupNames() {
    return Collections.unmodifiableList(
        new ArrayList<>(getGroups()));
  }

  private static Set<String> getGroups() {
    return GROUPS_LOOKUP.keySet();
  }

  /**
   * Returns true if a given netgroup is cached
   *
   * @param group check if this group is cached
   * @return true if group is cached, false otherwise
   */
  public static boolean isCached(String group) {
    return GROUPS_LOOKUP.containsKey(group);
  }

  /**
   * Clear the cache
   */
  public static void clear() {
    userToNetgroupsMap.clear();
  }

  /**
   * Refresh the cached entries.
   * This is done in two steps: clearing the map, then regenerating the values
   * for each key.
   *
   * @param groupProvider the implementation that retrieves the users of the
   *                      group. This could be a JNI.
   * @throws IOException exception thrown getting users of a specific group.
   */
  public static void refreshCacheCB(NetgroupCacheProvider groupProvider)
      throws IOException {
    NetgroupCacheFaultInjector.get().checkPointResettingBeforeClearing();
    clear();
    for (String groupName : GROUPS_LOOKUP.keySet()) {
      add(groupName, groupProvider);
    }
  }

  /**
   * clears the group-users mapping and clear the lookup.
   */
  @VisibleForTesting
  protected static void clearDataForTesting() {
    clear();
    GROUPS_LOOKUP.clear();
  }

  /**
   * Add group to cache.
   *
   * @param group name of the group to add to cache
   * @param users list of users for a given group
   */
  public static void add(String group, List<String> users) {
    //TODO this method should be only visible for testing.
    addInternal(group, users);
  }

  /**
   * Add a group to cache using the provider implementation.
   * Note that the group will not be added to the cache if an exception is
   * thrown by the group provider implementation.
   *
   * @param group the netgroup value.
   * @param groupProvider the implementation that fetches the group users.
   * @throws IOException exception thrown fetching the users.
   */
  public static void add(String group,
      NetgroupCacheProvider groupProvider) throws IOException {
    List<String> groupUsers = groupProvider.getUsersForNetgroup(group);
    addInternal(group, groupUsers);
  }

  /**
   * Add group to cache.
   *
   * @param group name of the group to add to cache
   * @param users list of users for a given group
   */
  private static void addInternal(String group, List<String> users) {
    //TODO is it supposed to cache empty groups?
    for (String user : users) {
      userToNetgroupsMap.computeIfAbsent(user,
          userName -> ConcurrentHashMap.newKeySet()).add(group);
    }
    GROUPS_LOOKUP.put(group, Time.monotonicNow());
    NetgroupCacheFaultInjector.get().checkPointPostAddingGroup(group);
  }

  /**
   * An interface required to provide the netgroup cache with the key/value
   * pairs.
   */
  public interface NetgroupCacheProvider {
    default List<String> getUsersForNetgroup(String netgroup)
        throws IOException {
      return Collections.emptyList();
    }

    default boolean isCacheableGroup(String groupName) {
      return (groupName.length() > 0
          && groupName.charAt(0) == '@' // unix group, not caching
          && !isCached(groupName));
    }
  }
}
