/*
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
 *
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * The AbfsPermission for AbfsClient.
 */
public class AbfsPermission extends FsPermission {
  private static final int STICKY_BIT_OCTAL_VALUE = 01000;
  private final boolean aclBit;

  public AbfsPermission(Short aShort, boolean aclBitStatus) {
    super(aShort);
    this.aclBit = aclBitStatus;
  }

  public AbfsPermission(FsAction u, FsAction g, FsAction o) {
    super(u, g, o, false);
    this.aclBit = false;
  }

  /**
   * Returns true if there is also an ACL (access control list).
   *
   * @return boolean true if there is also an ACL (access control list).
   * @deprecated Get acl bit from the {@link org.apache.hadoop.fs.FileStatus}
   * object.
   */
  public boolean getAclBit() {
    return aclBit;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FsPermission) {
      FsPermission that = (FsPermission) obj;
      return this.getUserAction() == that.getUserAction()
          && this.getGroupAction() == that.getGroupAction()
          && this.getOtherAction() == that.getOtherAction()
          && this.getStickyBit() == that.getStickyBit();
    }
    return false;
  }

  /**
   * Create a AbfsPermission from a abfs symbolic permission string
   * @param abfsSymbolicPermission e.g. "rw-rw-rw-+" / "rw-rw-rw-"
   * @return a permission object for the provided string representation
   */
  public static AbfsPermission valueOf(final String abfsSymbolicPermission) {
    if (abfsSymbolicPermission == null) {
      return null;
    }

    final boolean isExtendedAcl = abfsSymbolicPermission.charAt(abfsSymbolicPermission.length() - 1) == '+';

    final String abfsRawSymbolicPermission = isExtendedAcl ? abfsSymbolicPermission.substring(0, abfsSymbolicPermission.length() - 1)
        : abfsSymbolicPermission;

    int n = 0;
    for (int i = 0; i < abfsRawSymbolicPermission.length(); i++) {
      n = n << 1;
      char c = abfsRawSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }

    // Add sticky bit value if set
    if (abfsRawSymbolicPermission.charAt(abfsRawSymbolicPermission.length() - 1) == 't'
        || abfsRawSymbolicPermission.charAt(abfsRawSymbolicPermission.length() - 1) == 'T') {
      n += STICKY_BIT_OCTAL_VALUE;
    }

    return new AbfsPermission((short) n, isExtendedAcl);
  }

  /**
   * Check whether abfs symbolic permission string is a extended Acl
   * @param abfsSymbolicPermission e.g. "rw-rw-rw-+" / "rw-rw-rw-"
   * @return true if the permission string indicates the existence of an
   * extended ACL; otherwise false.
   */
  public static boolean isExtendedAcl(final String abfsSymbolicPermission) {
    if (abfsSymbolicPermission == null) {
      return false;
    }

    return abfsSymbolicPermission.charAt(abfsSymbolicPermission.length() - 1) == '+';
  }

  @Override
  public int hashCode() {
    return toShort();
  }
}