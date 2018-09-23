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
package org.apache.hadoop.fs.azurebfs.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

import static org.junit.Assert.assertEquals;

/**
 * Helper methods useful for writing ACL tests.
 */
public final class AclTestHelpers {

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param permission FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  public static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
                                  FsAction permission) {
    return new AclEntry.Builder()
        .setScope(scope)
        .setType(type)
        .setPermission(permission)
        .build();
  }

  /**
   * Create a new AclEntry with scope, type, name and permission.
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param name String optional ACL entry name
   * @param permission FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  public static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
                                  String name, FsAction permission) {
    return new AclEntry.Builder()
        .setScope(scope)
        .setType(type)
        .setName(name)
        .setPermission(permission)
        .build();
  }

  /**
   * Create a new AclEntry with scope, type and name (no permission).
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @param name String optional ACL entry name
   * @return AclEntry new AclEntry
   */
  public static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
                                  String name) {
    return new AclEntry.Builder()
        .setScope(scope)
        .setType(type)
        .setName(name)
        .build();
  }

  /**
   * Create a new AclEntry with scope and type (no name or permission).
   *
   * @param scope AclEntryScope scope of the ACL entry
   * @param type AclEntryType ACL entry type
   * @return AclEntry new AclEntry
   */
  public static AclEntry aclEntry(AclEntryScope scope, AclEntryType type) {
    return new AclEntry.Builder()
        .setScope(scope)
        .setType(type)
        .build();
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of a specific path.
   *
   * @param fs FileSystem to use for check
   * @param pathToCheck Path inode to check
   * @param perm short expected permission bits
   * @throws IOException thrown if there is an I/O error
   */
  public static void assertPermission(FileSystem fs, Path pathToCheck,
                                      short perm) throws IOException {
    assertEquals(perm, fs.getFileStatus(pathToCheck).getPermission().toShort());
  }

  private AclTestHelpers() {
    // Not called.
  }
}