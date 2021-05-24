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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class that helps in checking permissions in Router-based federation.
 */
public class RouterPermissionChecker extends FSPermissionChecker {
  static final Logger LOG =
      LoggerFactory.getLogger(RouterPermissionChecker.class);

  /** Mount table default permission. */
  public static final short MOUNT_TABLE_PERMISSION_DEFAULT = 00755;

  /** Name of the super user. */
  private final String superUser;
  /** Name of the super group. */
  private final String superGroup;

  public RouterPermissionChecker(String user, String group,
      UserGroupInformation callerUgi) {
    super(user, group, callerUgi, null);
    this.superUser = user;
    this.superGroup = group;
  }

  public RouterPermissionChecker(String user, String group)
      throws IOException {
    super(user, group, UserGroupInformation.getCurrentUser(), null);
    this.superUser = user;
    this.superGroup = group;
  }

  /**
   * Whether a mount table entry can be accessed by the current context.
   *
   * @param mountTable
   *          MountTable being accessed
   * @param access
   *          type of action being performed on the mount table entry
   * @throws AccessControlException
   *           if mount table cannot be accessed
   */
  public void checkPermission(MountTable mountTable, FsAction access)
      throws AccessControlException {
    if (isSuperUser()) {
      return;
    }

    FsPermission mode = mountTable.getMode();
    if (getUser().equals(mountTable.getOwnerName())
        && mode.getUserAction().implies(access)) {
      return;
    }

    if (isMemberOfGroup(mountTable.getGroupName())
        && mode.getGroupAction().implies(access)) {
      return;
    }

    if (!getUser().equals(mountTable.getOwnerName())
        && !isMemberOfGroup(mountTable.getGroupName())
        && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
        "Permission denied while accessing mount table "
            + mountTable.getSourcePath()
            + ": user " + getUser() + " does not have " + access.toString()
            + " permissions.");
  }

  /**
   * Check the superuser privileges of the current RPC caller. This method is
   * based on Datanode#checkSuperuserPrivilege().
   * @throws AccessControlException If the user is not authorized.
   */
  @Override
  public void checkSuperuserPrivilege() throws  AccessControlException {

    // Try to get the ugi in the RPC call.
    UserGroupInformation ugi = null;
    try {
      ugi = NameNode.getRemoteUser();
    } catch (IOException e) {
      // Ignore as we catch it afterwards
    }
    if (ugi == null) {
      LOG.error("Cannot get the remote user name");
      throw new AccessControlException("Cannot get the remote user name");
    }

    // Is this by the Router user itself?
    if (ugi.getShortUserName().equals(superUser)) {
      return;
    }

    // Is the user a member of the super group?
    List<String> groups = Arrays.asList(ugi.getGroupNames());
    if (groups.contains(superGroup)) {
      return;
    }

    // Not a superuser
    throw new AccessControlException(
        ugi.getUserName() + " is not a super user");
  }
}
