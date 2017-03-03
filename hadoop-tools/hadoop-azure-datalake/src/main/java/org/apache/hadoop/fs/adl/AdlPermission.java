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

package org.apache.hadoop.fs.adl;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Hadoop shell command -getfacl does not invoke getAclStatus if FsPermission
 * from getFileStatus has not set ACL bit to true. By default getAclBit returns
 * false.
 *
 * Provision to make additional call to invoke getAclStatus would be redundant
 * when adls is running as additional FS. To avoid this redundancy, provided
 * configuration to return true/false on getAclBit.
 */
class AdlPermission extends FsPermission {
  private final boolean aclBit;

  AdlPermission(boolean aclBitStatus, Short aShort) {
    super(aShort);
    this.aclBit = aclBitStatus;
  }

  /**
   * Returns true if "adl.feature.support.acl.bit" configuration is set to
   * true.
   *
   * If configuration is not set then default value is true.
   *
   * @return If configuration is not set then default value is true.
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

  @Override
  public int hashCode() {
    return toShort();
  }
}
