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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} 
 * that exec's the <code>groups</code> shell command to fetch the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ShellBasedUnixGroupsMapping extends Configured
  implements GroupMappingServiceProvider {

  @VisibleForTesting
  protected static final Logger LOG =
      LoggerFactory.getLogger(ShellBasedUnixGroupsMapping.class);

  private long timeout = CommonConfigurationKeys.
      HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT;
  private static final List<String> EMPTY_GROUPS = new LinkedList<>();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      timeout = conf.getTimeDuration(
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_KEY,
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUP_SHELL_COMMAND_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
    }
  }

  @SuppressWarnings("serial")
  private static class PartialGroupNameException extends IOException {
    public PartialGroupNameException(String message) {
      super(message);
    }

    public PartialGroupNameException(String message, Throwable err) {
      super(message, err);
    }

    @Override
    public String toString() {
      final StringBuilder sb =
          new StringBuilder("PartialGroupNameException ");
      sb.append(super.getMessage());
      return sb.toString();
    }
  }
  /**
   * Returns list of groups for a user
   *
   * @param userName get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public List<String> getGroups(String userName) throws IOException {
    return getUnixGroups(userName);
  }

  /**
   * Caches groups, no need to do that for this provider
   */
  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /**
   * Create a ShellCommandExecutor object using the user's name.
   *
   * @param userName user's name
   * @return a ShellCommandExecutor object
   */
  protected ShellCommandExecutor createGroupExecutor(String userName) {
    return new ShellCommandExecutor(
        getGroupsForUserCommand(userName), null, null, timeout);
  }

  /**
   * Returns just the shell command to be used to fetch a user's groups list.
   * This is mainly separate to make some tests easier.
   * @param userName The username that needs to be passed into the command built
   * @return An appropriate shell command with arguments
   */
  protected String[] getGroupsForUserCommand(String userName) {
    return Shell.getGroupsForUserCommand(userName);
  }

  /**
   * Create a ShellCommandExecutor object for fetch a user's group id list.
   *
   * @param userName the user's name
   * @return a ShellCommandExecutor object
   */
  protected ShellCommandExecutor createGroupIDExecutor(String userName) {
    return new ShellCommandExecutor(
        getGroupsIDForUserCommand(userName), null, null, timeout);
  }

  /**
   * Returns just the shell command to be used to fetch a user's group IDs list.
   * This is mainly separate to make some tests easier.
   * @param userName The username that needs to be passed into the command built
   * @return An appropriate shell command with arguments
   */
  protected String[] getGroupsIDForUserCommand(String userName) {
    return Shell.getGroupsIDForUserCommand(userName);
  }

  /**
   * Check if the executor had a timeout and logs the event.
   * @param executor to check
   * @param user user to log
   * @return true if timeout has occurred
   */
  private boolean handleExecutorTimeout(
      ShellCommandExecutor executor,
      String user) {
    // If its a shell executor timeout, indicate so in the message
    // but treat the result as empty instead of throwing it up,
    // similar to how partial resolution failures are handled above
    if (executor.isTimedOut()) {
      LOG.warn(
          "Unable to return groups for user '{}' as shell group lookup " +
              "command '{}' ran longer than the configured timeout limit of " +
              "{} seconds.",
          user,
          Joiner.on(' ').join(executor.getExecString()),
          timeout
      );
      return true;
    }
    return false;
  }

  /**
   * Get the current user's group list from Unix by running the command 'groups'
   * NOTE. For non-existing user it will return EMPTY list.
   *
   * @param user get groups for this user
   * @return the groups list that the <code>user</code> belongs to. The primary
   *         group is returned first.
   * @throws IOException if encounter any error when running the command
   */
  private List<String> getUnixGroups(String user) throws IOException {
    ShellCommandExecutor executor = createGroupExecutor(user);

    List<String> groups;
    try {
      executor.execute();
      groups = resolveFullGroupNames(executor.getOutput());
    } catch (ExitCodeException e) {
      if (handleExecutorTimeout(executor, user)) {
        return EMPTY_GROUPS;
      } else {
        try {
          groups = resolvePartialGroupNames(user, e.getMessage(),
              executor.getOutput());
        } catch (PartialGroupNameException pge) {
          LOG.warn("unable to return groups for user {}", user, pge);
          return EMPTY_GROUPS;
        }
      }
    } catch (IOException ioe) {
      if (handleExecutorTimeout(executor, user)) {
        return EMPTY_GROUPS;
      } else {
        // If its not an executor timeout, we should let the caller handle it
        throw ioe;
      }
    }

    // remove duplicated primary group
    if (!Shell.WINDOWS) {
      for (int i = 1; i < groups.size(); i++) {
        if (groups.get(i).equals(groups.get(0))) {
          groups.remove(i);
          break;
        }
      }
    }

    return groups;
  }

  /**
   * Attempt to parse group names given that some names are not resolvable.
   * Use the group id list to identify those that are not resolved.
   *
   * @param groupNames a string representing a list of group names
   * @param groupIDs a string representing a list of group ids
   * @return a linked list of group names
   * @throws PartialGroupNameException
   */
  private List<String> parsePartialGroupNames(String groupNames,
      String groupIDs) throws PartialGroupNameException {
    StringTokenizer nameTokenizer =
        new StringTokenizer(groupNames, Shell.TOKEN_SEPARATOR_REGEX);
    StringTokenizer idTokenizer =
        new StringTokenizer(groupIDs, Shell.TOKEN_SEPARATOR_REGEX);
    List<String> groups = new LinkedList<String>();
    while (nameTokenizer.hasMoreTokens()) {
      // check for unresolvable group names.
      if (!idTokenizer.hasMoreTokens()) {
        throw new PartialGroupNameException("Number of group names and ids do"
        + " not match. group name =" + groupNames + ", group id = " + groupIDs);
      }
      String groupName = nameTokenizer.nextToken();
      String groupID = idTokenizer.nextToken();
      if (!StringUtils.isNumeric(groupName) ||
          !groupName.equals(groupID)) {
        // if the group name is non-numeric, it is resolved.
        // if the group name is numeric, but is not the same as group id,
        // regard it as a group name.
        // if unfortunately, some group names are not resolvable, and
        // the group name is the same as the group id, regard it as not
        // resolved.
        groups.add(groupName);
      }
    }
    return groups;
  }

  /**
   * Attempt to partially resolve group names.
   *
   * @param userName the user's name
   * @param errMessage error message from the shell command
   * @param groupNames the incomplete list of group names
   * @return a list of resolved group names
   * @throws PartialGroupNameException if the resolution fails or times out
   */
  private List<String> resolvePartialGroupNames(String userName,
      String errMessage, String groupNames) throws PartialGroupNameException {
    // Exception may indicate that some group names are not resolvable.
    // Shell-based implementation should tolerate unresolvable groups names,
    // and return resolvable ones, similar to what JNI-based implementation
    // does.
    if (Shell.WINDOWS) {
      throw new PartialGroupNameException("Does not support partial group"
      + " name resolution on Windows. " + errMessage);
    }
    if (groupNames.isEmpty()) {
      throw new PartialGroupNameException("The user name '" + userName
          + "' is not found. " + errMessage);
    } else {
      LOG.warn("Some group names for '{}' are not resolvable. {}",
          userName, errMessage);
      // attempt to partially resolve group names
      ShellCommandExecutor partialResolver = createGroupIDExecutor(userName);
      try {
        partialResolver.execute();
        return parsePartialGroupNames(
            groupNames, partialResolver.getOutput());
      } catch (ExitCodeException ece) {
        // If exception is thrown trying to get group id list,
        // something is terribly wrong, so give up.
        throw new PartialGroupNameException(
            "failed to get group id list for user '" + userName + "'", ece);
      } catch (IOException ioe) {
        String message =
            "Can't execute the shell command to " +
            "get the list of group id for user '" + userName + "'";
        if (partialResolver.isTimedOut()) {
          message +=
              " because of the command taking longer than " +
              "the configured timeout: " + timeout + " seconds";
        }
        throw new PartialGroupNameException(message, ioe);
      }
    }
  }

  /**
   * Split group names into a linked list.
   *
   * @param groupNames a string representing the user's group names
   * @return a linked list of group names
   */
  @VisibleForTesting
  protected List<String> resolveFullGroupNames(String groupNames) {
    StringTokenizer tokenizer =
        new StringTokenizer(groupNames, Shell.TOKEN_SEPARATOR_REGEX);
    List<String> groups = new LinkedList<String>();
    while (tokenizer.hasMoreTokens()) {
      groups.add(tokenizer.nextToken());
    }

    return groups;
  }
}
