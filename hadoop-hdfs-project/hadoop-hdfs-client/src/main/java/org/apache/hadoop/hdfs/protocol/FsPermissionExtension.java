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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * HDFS permission subclass used to indicate an ACL is present and/or that the
 * underlying file/dir is encrypted. The ACL/encrypted bits are not visible
 * directly to users of {@link FsPermission} serialization.  This is
 * done for backwards compatibility in case any existing clients assume the
 * value of FsPermission is in a particular range.
 */

/**
 * @deprecated ACLs, encryption, and erasure coding are managed on FileStatus.
 */
@Deprecated
@InterfaceAudience.Private
public class FsPermissionExtension extends FsPermission {
  private static final long serialVersionUID = 0x13c298a4;

  private final static short ACL_BIT = 1 << 12;
  private final static short ENCRYPTED_BIT = 1 << 13;
  private final static short ERASURE_CODED_BIT = 1 << 14;
  private final boolean aclBit;
  private final boolean encryptedBit;
  private final boolean erasureCodedBit;

  /**
   * Constructs a new FsPermissionExtension based on the given FsPermission.
   *
   * @param perm FsPermission containing permission bits
   */
  public FsPermissionExtension(FsPermission perm, boolean hasAcl,
      boolean isEncrypted, boolean isErasureCoded) {
    super(perm.toShort());
    aclBit = hasAcl;
    encryptedBit = isEncrypted;
    erasureCodedBit = isErasureCoded;
  }

  /**
   * Creates a new FsPermissionExtension by calling the base class constructor.
   *
   * @param perm short containing permission bits
   */
  public FsPermissionExtension(short perm) {
    super(perm);
    aclBit = (perm & ACL_BIT) != 0;
    encryptedBit = (perm & ENCRYPTED_BIT) != 0;
    erasureCodedBit = (perm & ERASURE_CODED_BIT) != 0;
  }

  @Override
  public short toExtendedShort() {
    return (short)(toShort()
        | (aclBit ? ACL_BIT : 0)
        | (encryptedBit ? ENCRYPTED_BIT : 0)
        | (erasureCodedBit ? ERASURE_CODED_BIT : 0));
  }

  @Override
  public boolean getAclBit() {
    return aclBit;
  }

  @Override
  public boolean getEncryptedBit() {
    return encryptedBit;
  }

  @Override
  public boolean getErasureCodedBit() {
    return erasureCodedBit;
  }

  @Override
  public boolean equals(Object o) {
    // This intentionally delegates to the base class.  This is only overridden
    // to suppress a FindBugs warning.
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // This intentionally delegates to the base class.  This is only overridden
    // to suppress a FindBugs warning.
    return super.hashCode();
  }
}
