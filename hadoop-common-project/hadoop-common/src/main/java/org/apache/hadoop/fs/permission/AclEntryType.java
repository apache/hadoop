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
package org.apache.hadoop.fs.permission;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Specifies the type of an ACL entry.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum AclEntryType {
  /**
   * An ACL entry applied to a specific user.  These ACL entries can be unnamed,
   * which applies to the file owner, or named, which applies to the specific
   * named user.
   */
  USER,

  /**
   * An ACL entry applied to a specific group.  These ACL entries can be
   * unnamed, which applies to the file's group, or named, which applies to the
   * specific named group.
   */
  GROUP,

  /**
   * An ACL mask entry.  Mask entries are unnamed.  During permission checks,
   * the mask entry interacts with all ACL entries that are members of the group
   * class.  This consists of all named user entries, the unnamed group entry,
   * and all named group entries.  For each such entry, any permissions that are
   * absent from the mask entry are removed from the effective permissions used
   * during the permission check.
   */
  MASK,

  /**
   * An ACL entry that applies to all other users that were not covered by one
   * of the more specific ACL entry types.
   */
  OTHER;
}
