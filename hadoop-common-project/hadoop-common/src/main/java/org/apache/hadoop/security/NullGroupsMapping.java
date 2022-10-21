/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This class provides groups mapping for {@link UserGroupInformation} when the
 * user group information will not be used.
 */
public class NullGroupsMapping implements GroupMappingServiceProvider {
  /**
   * Nothing is returned, so nothing is cached.
   * @param groups ignored
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) {
  }

  /**
   * Get all various group memberships of a given user.
   * Returns EMPTY set in case of non-existing user
   *
   * @param user User's name
   * @return set of group memberships of user
   * @throws IOException raised on errors performing I/O.
   */
  @Override
  public Set<String> getGroupsSet(String user) throws IOException {
    return Collections.emptySet();
  }

  /**
   * Returns an empty list.
   * @param user ignored
   * @return an empty list
   */
  @Override
  public List<String> getGroups(String user) {
    return Collections.emptyList();
  }

  /**
   * Nothing is returned, so nothing is cached.
   */
  @Override
  public void cacheGroupsRefresh() {
  }
}
