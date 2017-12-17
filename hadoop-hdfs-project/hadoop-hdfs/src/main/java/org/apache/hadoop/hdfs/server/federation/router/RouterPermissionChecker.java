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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Class that helps in checking permissions in Router-based federation.
 */
public class RouterPermissionChecker extends FSPermissionChecker {
  static final Log LOG = LogFactory.getLog(RouterPermissionChecker.class);

  /** Mount table default permission. */
  public static final short MOUNT_TABLE_PERMISSION_DEFAULT = 00755;

  public RouterPermissionChecker(String routerOwner, String supergroup,
      UserGroupInformation callerUgi) {
    super(routerOwner, supergroup, callerUgi, null);
  }

  /**
   * Whether a mount table entry can be accessed by the current context.
   *
   * @param mountTable
   *          MountTable being accessed
   * @param access
   *          type of action being performed on the cache pool
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
}
