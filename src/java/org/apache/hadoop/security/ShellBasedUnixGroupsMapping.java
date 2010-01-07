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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} which
 * exec's the <code>groups</code> shell command to fetch the {@link Group}
 * memberships of a given {@link User}.
 */
public class ShellBasedUnixGroupsMapping implements GroupMappingServiceProvider {
  Map<String, List<String>> userGroups = 
    new ConcurrentHashMap<String, List<String>>();
  
  private static final Log LOG = LogFactory.getLog(ShellBasedUnixGroupsMapping.class);
  
  @Override
  public List<String> getGroups(String user) throws IOException {
    List<String> groups = userGroups.get(user);
    if (groups == null) {
      groups = getUnixGroups(user);
      userGroups.put(user, groups);
    }
    return groups;
  }

  /** 
   * Get the current user's group list from Unix by running the command 'groups'
   * NOTE. For non-existing user it will return EMPTY list
   * @param user user name
   * @return the groups list that the <code>user</code> belongs to
   * @throws IOException if encounter any error when running the command
   */
  private static List<String> getUnixGroups(final String user) throws IOException {
    String result = "";
    try {
      result = Shell.execCommand(Shell.getGROUPS_FOR_USER_COMMAND(user));
    } catch (ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("got exception trying to get groups for user " + user, e);
    }
    
    StringTokenizer tokenizer = new StringTokenizer(result);
    List<String> groups = new LinkedList<String>();
    while (tokenizer.hasMoreTokens()) {
      groups.add(tokenizer.nextToken());
    }
    return groups;
  }
}
