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
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} 
 * that exec's a shell command to fetch the group
 * memberships of a given user.
 */
public class ShellBasedUnixGroupsMapping implements GroupMappingServiceProvider {
  
  private static final Log LOG = LogFactory.getLog(ShellBasedUnixGroupsMapping.class);
  
  @Override
  public List<String> getGroups(String user) throws IOException {
    return getUserGroups(user);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Get the current user's group list from Unix by running the command 'groups'
   * NOTE. For non-existing user it will return EMPTY list
   * @param user user name
   * @return the groups list that the <code>user</code> belongs to
   * @throws IOException if encounter any error when running the command
   */
  private static List<String> getUserGroups(final String user) throws IOException {
    List<String> groups = new LinkedList<String>();
    if (Shell.WINDOWS) {
      String result = Shell.execCommand(Shell.getGroupsForUserCommand(user));
      String[] lines = result.split("\\r\\n");
        String line = lines[0];
        if (!line.startsWith("User name")) {
          throw new IOException(
              "Command result did not start with \"User name\"");
        }
        String[] splits = line.substring(9).split("\\s");
        if (splits.length == 0 || !splits[splits.length-1].equals(user)) {
          throw new IOException("Bad user name returned");
        }
        for (int i=1; i<lines.length; ++i) {
          line = lines[i];
          // not handling global group memberships now
          // it might be better to handle them via a specific domain controller
          // plugin
          if (line.startsWith("Local Group Memberships")) {
            splits = line.substring(23).split("\\s");
            for (String group : splits) {
              if (group.length() > 0) {
                if (group.charAt(0) == '*') {
                  group = group.substring(1);
                }
                groups.add(group);
              }
            }
          }
        }
    }
    else {
      String result = "";
      try {
        result = Shell.execCommand(Shell.getGroupsForUserCommand(user));
      } catch (ExitCodeException e) {
        // if we didn't get the group - just return empty list;
        LOG.warn("got exception trying to get groups for user " + user, e);
      }
      StringTokenizer tokenizer = new StringTokenizer(result);
      
      while (tokenizer.hasMoreTokens()) {
        groups.add(tokenizer.nextToken());
      }
    }
    
    return groups;
  }
}
