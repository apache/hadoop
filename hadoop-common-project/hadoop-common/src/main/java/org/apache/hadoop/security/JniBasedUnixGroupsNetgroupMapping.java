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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.security.NetgroupCache.NetgroupCacheListAdder;
import org.apache.hadoop.util.NativeCodeLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider} 
 * that invokes libC calls to get the group.
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class JniBasedUnixGroupsNetgroupMapping
  extends JniBasedUnixGroupsMapping {
  
  private static final Logger LOG = LoggerFactory.getLogger(
    JniBasedUnixGroupsNetgroupMapping.class);

  native String[] getUsersForNetgroupJNI(String group);

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new RuntimeException("Bailing out since native library couldn't " +
        "be loaded");
    }
    LOG.debug("Using JniBasedUnixGroupsNetgroupMapping for Netgroup resolution");
  }

  private JniUnixGroupsNetgroupCacheAdder groupBulkAdder;

  public JniBasedUnixGroupsNetgroupMapping() {
    super();
    init();
  }

  /**
   * Gets unix groups and netgroups for the user.
   *
   * It gets all unix groups as returned by id -Gn but it
   * only returns netgroups that are used in ACLs (there is
   * no way to get all netgroups for a given user, see
   * documentation for getent netgroup)
   */
  @Override
  public List<String> getGroups(String user) throws IOException {
    // parent gets unix groups
    List<String> groups = new LinkedList<>(super.getGroups(user));
    NetgroupCache.getNetgroups(user, groups);
    return groups;
  }

  @Override
  public Set<String> getGroupsSet(String user) throws IOException {
    // parent gets unix groups
    Set<String> groups = new LinkedHashSet<>(super.getGroupsSet(user));
    NetgroupCache.getNetgroups(user, groups);
    return groups;
  }

  /**
   * Refresh the netgroup cache
   */
  @Override
  public void cacheGroupsRefresh() throws IOException {
    groupBulkAdder.refreshCachedGroups();
  }

  /**
   * Add a group to cache, only netgroups are cached
   *
   * @param groups list of group names to add to cache
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    groupBulkAdder.addGroupToCacheInBulk(groups);
  }

  /**
   * Calls JNI function to get users for a netgroup, since C functions
   * are not reentrant we need to make this synchronized (see
   * documentation for setnetgrent, getnetgrent and endnetgrent)
   *
   * @param netgroup return users for this netgroup
   * @return list of users for a given netgroup
   */
  private synchronized Set<String> getUsersForNetgroup(String netgroup) {
    String[] users = null;
    try {
      // JNI code does not expect '@' at the beginning of the group name
      users = getUsersForNetgroupJNI(netgroup.substring(1));
    } catch (Exception e) {
      LOG.debug("Error getting users for netgroup {}", netgroup, e);
      LOG.info("Error getting users for netgroup {}: {}", netgroup,
          e.getMessage());
    }
    if (users != null && users.length != 0) {
      return new HashSet<>(Arrays.asList(users));
    }
    return Collections.emptySet();
  }

  protected void init() {
    groupBulkAdder = new JniUnixGroupsNetgroupCacheAdder();
  }

  class JniUnixGroupsNetgroupCacheAdder extends NetgroupCacheListAdder {
    @Override
    Collection<String> getValuesForEntryKey(String entryKey) {
      return getUsersForNetgroup(entryKey);
    }
  }
}
