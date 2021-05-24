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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider} 
 * that invokes libC calls to get the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class JniBasedUnixGroupsMapping implements GroupMappingServiceProvider {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(JniBasedUnixGroupsMapping.class);

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new RuntimeException("Bailing out since native library couldn't " +
        "be loaded");
    }
    anchorNative();
    LOG.debug("Using JniBasedUnixGroupsMapping for Group resolution");
  }

  /**
   * Set up our JNI resources.
   *
   * @throws                 RuntimeException if setup fails.
   */
  native static void anchorNative();

  /**
   * Get the set of groups associated with a user.
   *
   * @param username           The user name
   *
   * @return                   The set of groups associated with a user.
   */
  native static String[] getGroupsForUser(String username);

  /**
   * Log an error message about a group.  Used from JNI.
   */
  static private void logError(int groupId, String error) {
    LOG.error("error looking up the name of group " + groupId + ": " + error);
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    String[] groups = new String[0];
    try {
      groups = getGroupsForUser(user);
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error getting groups for " + user, e);
      } else {
        LOG.info("Error getting groups for " + user + ": " + e.getMessage());
      }
    }
    return Arrays.asList(groups);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
}
