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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit test to verify the behavior of Groups caching in the presence of a
 * lower level caching mechanism like the one in {@link NetgroupCache}.
 */
public class TestGroupsCachingOnNetgroups {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestGroupsCachingOnNetgroups.class);

  private static final int USERS_COUNT = 200;
  private static final int GROUPS_COUNT = 40;
  // add 50 users on each advance
  private static final int USERS_STEPS  = 50;
  private static final int GROUPS_STEPS = 10;
  private static final String GROUP_ID_FORMAT = "group-%03d";
  private static final String NETGROUP_ID_FORMAT = "@group-%03d";
  private static final String USER_ID_FORMAT = "user-%03d";
  private static ConcurrentHashMap<String, Set<String>> groupUserLookup;

  private Configuration conf;
  private List<String> usersList;
  private List<String> groupsList;
  private AtomicInteger usersBound;
  private AtomicInteger groupsBound;
  private Groups groupsStore;
  private List<String> currentGroupsList;
  private List<String> currentUsersList;

  private static boolean groupBelongsToNetgroup(String groupName) {
    return (groupName.length() > 0 && groupName.charAt(0) == '@');
  }

  private static void generateRawData(List<String> groupsList, int groupsCount,
      List<String> usersList, int usersCount) {
    for (int uCounter = 0, userID = usersList.size() + 1;
         uCounter < usersCount; uCounter++, userID++) {
      usersList.add(String.format(USER_ID_FORMAT, userID));
    }
    for (int gCounter = 0, groupID = usersList.size() + 1;
         gCounter < groupsCount; gCounter++, groupID++) {
      String groupNamePrefix =
          gCounter % 3 == 0 ? GROUP_ID_FORMAT : NETGROUP_ID_FORMAT;
      groupsList.add(String.format(groupNamePrefix, groupID));
    }
  }

  private static Set<String> getGroupsAddedToLookup(
      Map<String, Set<String>> groupToUsersMap, boolean isNetGroup) {
    if (!isNetGroup) {
      return groupToUsersMap.keySet().stream()
          .filter(groupName -> !groupBelongsToNetgroup(groupName))
          .collect(Collectors.toSet());
    }
    return groupToUsersMap.keySet().stream()
        .filter(TestGroupsCachingOnNetgroups::groupBelongsToNetgroup)
        .collect(Collectors.toSet());
  }

  private static void assignUsersToGroups(List<String> groupsList,
      List<String> usersList, Map<String, Set<String>> groupToUsersMap) {
    for (String groupName :  groupsList) {
      Set<String> users = getRandomElements(usersList);
      groupToUsersMap.computeIfAbsent(groupName,
          k -> ConcurrentHashMap.newKeySet()).addAll(users);
    }
  }

  /**
   * Pick items randomly from a list of String.
   *
   * @param originalList list to select items from.
   * @return a random selected set with size at least larger than half
   *         the original set.
   */
  private static Set<String> getRandomElements(List<String> originalList) {
    int cutOffElements =
        Math.abs(ThreadLocalRandom.current().nextInt())
            % (originalList.size() >> 1);
    int numberOfElements = originalList.size() - cutOffElements;
    Set<String> randomElements = ConcurrentHashMap.newKeySet();
    for (int i = 0; i < numberOfElements; i++) {
      int rIndex =
          Math.abs(ThreadLocalRandom.current().nextInt()) % originalList.size();
      randomElements.add(originalList.get(rIndex));
    }
    return randomElements;
  }

  private static Collection<String> getUserGroupsFromLookUp(
      Map<String, Set<String>> lookupMap, String user, boolean isNetgroup) {
    Set<String> result = new LinkedHashSet<>();
    for (Map.Entry<String, Set<String>> lookupEntry : lookupMap.entrySet()) {
      if (isNetgroup ^ groupBelongsToNetgroup(lookupEntry.getKey())) {
        continue;
      }
      if (lookupEntry.getValue().contains(user)) {
        result.add(lookupEntry.getKey());
      }
    }
    return result;
  }

  private static Collection<String> getAllGroupsUserFromLookUp(
      Map<String, Set<String>> lookupMap, Collection<String> groupList) {
    Set<String> result = new LinkedHashSet<>();
    for (String groupName : groupList) {
      Collection<String> assignedUsers = lookupMap.get(groupName);
      if (assignedUsers.size() > 0) {
        result.addAll(lookupMap.get(groupName));
      }
    }
    return result;
  }

  @Before
  public void setup() throws IOException {
    groupUserLookup = new ConcurrentHashMap<>();
    usersList = new ArrayList<>();
    groupsList = new ArrayList<>();
    usersBound = new AtomicInteger(USERS_STEPS);
    groupsBound = new AtomicInteger(GROUPS_STEPS);
    conf = new Configuration();
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        GroupMappingWithNetgroups.class,
        GroupMappingWithNetgroupCachingImpl.class);
    generateRawData(groupsList, GROUPS_COUNT, usersList, USERS_COUNT);
    // generate group-user  assignment for first 50 users
    assignUsersToGroups(groupsList.subList(0, GROUPS_STEPS),
        usersList.subList(0, usersBound.get()),
        groupUserLookup);
    setCurrentLists();
    groupsStore = Groups.getUserToGroupsMappingService(conf);
  }

  @After
  public void tearDown() throws IOException {
    // clear the netgroupCache first to clear all the entries.
    NetgroupCache.clear();
    // call refresh to clear the higher level of cache in groupStore.
    groupsStore.refresh();
    groupUserLookup.clear();
    usersList.clear();
    groupsList.clear();
    usersBound.set(0);
    groupsBound.set(0);
  }

  /**
   * Verify the behavior of retrieving groups of users in the presence of
   * two-level caching where the lower level is {@link NetgroupCache}.
   *
   * @throws Exception when failure.
   */
  @Test
  public void testNetgroupCaching() throws Exception {
    // add groups to the first bulk of groups
    groupsStore.cacheGroupsAdd(currentGroupsList);

    // by now the groups should be added
    Collection<String> addedNetgroups =
        getGroupsAddedToLookup(groupUserLookup, true);
    Assert.assertTrue(verifyNetgroupCaching(addedNetgroups, groupUserLookup));
    int prevUBound = usersBound.getAndAdd(USERS_STEPS);
    setCurrentLists();
    assignUsersToGroups(currentGroupsList,
        usersList.subList(prevUBound, usersBound.get()), groupUserLookup);
    groupsStore.cacheGroupsAdd(currentGroupsList);
    // the groups won't refresh the netgroups groups with the new but it should
    // return the non-netgroups.
    // netgroupcache should not have the last user
    String lastUser = currentUsersList.get(currentUsersList.size() - 1);
    Assert.assertTrue(
        verifyUserInformation(lastUser, false,
            () -> {
              Set<String> lastUsersInNetgroups = new LinkedHashSet<>();
              NetgroupCache.getNetgroups(lastUser, lastUsersInNetgroups);
              return lastUsersInNetgroups.isEmpty();
            }));
    Collection<String> lastUsersGroups = groupsStore.getGroupsSet(lastUser);
    Assert.assertEquals(
        getUserGroupsFromLookUp(groupUserLookup, lastUser, false),
        lastUsersGroups);
    Set<String> lastUsersInNetgroups = new LinkedHashSet<>();
    NetgroupCache.getNetgroups(lastUser, lastUsersInNetgroups);
    Assert.assertTrue(lastUsersInNetgroups.isEmpty());
    // call refresh
    groupsStore.refresh();
    // all checks should pass by now.
    Assert.assertTrue(
        verifyUserInformation(lastUser, true,
            () -> {
              Set<String> updatedUsersInNetGroups = new LinkedHashSet<>();
              NetgroupCache.getNetgroups(lastUser, updatedUsersInNetGroups);
              return !updatedUsersInNetGroups.isEmpty();
            }));
    Assert.assertTrue(verifyNetgroupCaching(addedNetgroups, groupUserLookup));
    // verify Groups return correct values too.
    Assert.assertTrue(verifyGroupStore(addedNetgroups, groupUserLookup, true));

    // querying for the next 50 users will cause them to be placed into
    // the negativeCache even when the Netgroup is being added.
    prevUBound = usersBound.getAndAdd(USERS_STEPS);
    setCurrentLists();
    for (String userName : currentUsersList.subList(prevUBound,
        usersBound.get())) {
      intercept(IOException.class,
          () -> groupsStore.getGroupsSet(userName));
    }
    // assign mapping including two of the cached groups.
    groupsBound.getAndAdd(GROUPS_STEPS);
    setCurrentLists();
    assignUsersToGroups(currentGroupsList,
        usersList.subList(prevUBound, usersBound.get()), groupUserLookup);
    // the following call won't actually reflect updates for previously cached
    // groups. A user may not seen by the system until a manual refresh.
    groupsStore.cacheGroupsAdd(currentGroupsList);
    for (String userName: usersList.subList(prevUBound, usersBound.get())) {
      Collection<String> cachedNGrpsForUser = new HashSet<>();
      NetgroupCache.getNetgroups(userName, cachedNGrpsForUser);
      Collection<String> usersGrpFromLookup =
          getUserGroupsFromLookUp(groupUserLookup, userName, true);
      Assert.assertTrue(usersGrpFromLookup.containsAll(cachedNGrpsForUser));
      // Group store would still cause an exception
      intercept(IOException.class,
          () -> groupsStore.getGroupsSet(userName));
    }
  }

  private void setCurrentLists() {
    currentUsersList = usersList.subList(0, usersBound.get());
    currentGroupsList = groupsList.subList(0, groupsBound.get());
  }

  private boolean verifyUserInformation(String userName,
      boolean includeNetGroups,
      Supplier<Boolean> extraChecker) throws IOException {
    boolean result;
    Collection<String> lastUsersGroups = groupsStore.getGroupsSet(userName);
    Collection<String> referenceGroups =
        getUserGroupsFromLookUp(groupUserLookup, userName, false);
    if (includeNetGroups) {
      referenceGroups.addAll(
          getUserGroupsFromLookUp(groupUserLookup, userName, true));
    }
    result = (referenceGroups.size() == lastUsersGroups.size());
    result = result && lastUsersGroups.containsAll(referenceGroups)
        && referenceGroups.containsAll(lastUsersGroups);
    result = result && extraChecker.get();

    return result;
  }

  private boolean verifyGroupStore(Collection<String> groups,
      Map<String, Set<String>> lookupMap, boolean isNetGroup)
      throws IOException {
    Collection<String> allUsers = getAllGroupsUserFromLookUp(lookupMap, groups);
    for (String userName : allUsers) {
      Collection<String> groupStoreResult = groupsStore.getGroupsSet(userName);
      Collection<String> referenceResult =
          getUserGroupsFromLookUp(lookupMap, userName, isNetGroup);
      // user has to have all the values but not vice versa.
      // user could be assigned to non netgroup.
      if (!groupStoreResult.containsAll(referenceResult)) {
        return false;
      }
    }
    return true;
  }

  private boolean verifyNetgroupCaching(Collection<String> groups,
      Map<String, Set<String>> lookupMap) {
    boolean result = true;
    // verify groups are the same
    Collection<String> cachedNetgroups = NetgroupCache.getNetgroupNames();
    if (cachedNetgroups.size() != groups.size()
        || !cachedNetgroups.containsAll(groups)
        || !groups.containsAll(cachedNetgroups)) {
      return false;
    }

    // verify users are assigned correctly to the cache
    for (String groupName : groups) {
      Collection<String> referenceUsers = lookupMap.get(groupName);
      // verify users are cached correctly
      for (String user : referenceUsers) {
        Set<String> cachedUserNetgroups = new LinkedHashSet<>();
        NetgroupCache.getNetgroups(user, cachedUserNetgroups);
        result = cachedUserNetgroups.contains(groupName);
        if (result) {
          // verify that the assignment is correct for each group in the set
          for (String cacheUserNetgroup : cachedUserNetgroups) {
            result = lookupMap.get(cacheUserNetgroup) != null
                && lookupMap.get(cacheUserNetgroup).contains(user);
            if (!result) {
              break;
            }
          }
        }
        if (!result) {
          break;
        }
      }
    }
    return result;
  }

  public static class GroupMappingWithNetgroups
      extends GroupMappingWithNetgroupCachingImpl {
    private Map<String, Set<String>> groupUsersMap;

    GroupMappingWithNetgroups() {
      super();
      setGroupUsersMap(groupUserLookup);
    }

    public void setGroupUsersMap(Map<String, Set<String>> groupLookup) {
      groupUsersMap = groupLookup;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      List<String> groups = NetgroupCache.getNetgroupNames();
      NetgroupCache.clear();
      cacheGroupsAdd(groups);
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      for (String group: groups) {
        if (group.length() > 0) {
          if (group.charAt(0) == '@' && !NetgroupCache.isCached(group)) {
            NetgroupCache.add(group, getUsersForNetgroup(group));
          }
        }
      }
    }

    @Override
    public Set<String> getGroupsSet(String user) throws IOException {
      // parent get unix groups
      // create a new set because parent class may return an immutable set.
      Set<String> groups = new LinkedHashSet<>(super.getGroupsSet(user));
      // append netgroups.
      NetgroupCache.getNetgroups(user, groups);
      return groups;
    }

    public List<String> getUsersForNetgroup(String netgroup) {
      return new ArrayList<>(groupUsersMap.get(netgroup));
    }
  }

  /**
   * A class implementation to be used as mock to retrieve the non-Netgroup
   * values.
   */
  public static class GroupMappingWithNetgroupCachingImpl
      implements GroupMappingServiceProvider {
    @Override
    public List<String> getGroups(String user) throws IOException {
      return Arrays.asList(getGroupsInternal(user));
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      // does nothing in this provider of user to groups mapping
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      // does nothing in this provider of user to groups mapping
    }

    @Override
    public Set<String> getGroupsSet(String user) throws IOException {
      String[] groups = getGroupsInternal(user);
      Set<String> result = new LinkedHashSet<>(groups.length);
      CollectionUtils.addAll(result, groups);
      return result;
    }

    private String[] getGroupsInternal(String user) throws IOException {
      String[] groups = new String[0];
      try {
        groups = getGroupsForUser(user);
      } catch (Exception e) {
        LOG.debug("Error getting groups for {}", user, e);
        LOG.info("Error getting groups for {}: {}", user, e.getMessage());
      }
      return groups;
    }

    private String[] getGroupsForUser(String user) throws IOException {
      List<String> results = new ArrayList<>();
      Set<String> systemGroups = getGroupsAddedToLookup(groupUserLookup, false);
      for (String systemGroup : systemGroups) {
        if (groupUserLookup.get(systemGroup).contains(user)) {
          results.add(systemGroup);
        }
      }
      return results.toArray(new String[0]);
    }
  }
}
