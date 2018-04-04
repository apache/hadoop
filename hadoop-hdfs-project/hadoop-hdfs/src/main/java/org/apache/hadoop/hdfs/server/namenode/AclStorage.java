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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.ScopedAclEntries;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.util.ReferenceCountMap;

/**
 * AclStorage contains utility methods that define how ACL data is stored in the
 * namespace.
 *
 * If an inode has an ACL, then the ACL bit is set in the inode's
 * {@link FsPermission} and the inode also contains an {@link AclFeature}.  For
 * the access ACL, the owner and other entries are identical to the owner and
 * other bits stored in FsPermission, so we reuse those.  The access mask entry
 * is stored into the group permission bits of FsPermission.  This is consistent
 * with other file systems' implementations of ACLs and eliminates the need for
 * special handling in various parts of the codebase.  For example, if a user
 * calls chmod to change group permission bits on a file with an ACL, then the
 * expected behavior is to change the ACL's mask entry.  By saving the mask entry
 * into the group permission bits, chmod continues to work correctly without
 * special handling.  All remaining access entries (named users and named groups)
 * are stored as explicit {@link AclEntry} instances in a list inside the
 * AclFeature.  Additionally, all default entries are stored in the AclFeature.
 *
 * The methods in this class encapsulate these rules for reading or writing the
 * ACL entries to the appropriate location.
 *
 * The methods in this class assume that input ACL entry lists have already been
 * validated and sorted according to the rules enforced by
 * {@link AclTransformation}.
 */
@InterfaceAudience.Private
public final class AclStorage {

  private final static ReferenceCountMap<AclFeature> UNIQUE_ACL_FEATURES =
      new ReferenceCountMap<AclFeature>();

  /**
   * If a default ACL is defined on a parent directory, then copies that default
   * ACL to a newly created child file or directory.
   *
   * @param child INode newly created child
   */
  public static boolean copyINodeDefaultAcl(INode child) {
    INodeDirectory parent = child.getParent();
    AclFeature parentAclFeature = parent.getAclFeature();
    if (parentAclFeature == null || !(child.isFile() || child.isDirectory())) {
      return false;
    }

    // Split parent's entries into access vs. default.
    List<AclEntry> featureEntries = getEntriesFromAclFeature(parent
        .getAclFeature());
    ScopedAclEntries scopedEntries = new ScopedAclEntries(featureEntries);
    List<AclEntry> parentDefaultEntries = scopedEntries.getDefaultEntries();

    // The parent may have an access ACL but no default ACL.  If so, exit.
    if (parentDefaultEntries.isEmpty()) {
      return false;
    }

    // Pre-allocate list size for access entries to copy from parent.
    List<AclEntry> accessEntries = Lists.newArrayListWithCapacity(
      parentDefaultEntries.size());

    FsPermission childPerm = child.getFsPermission();

    // Copy each default ACL entry from parent to new child's access ACL.
    boolean parentDefaultIsMinimal = AclUtil.isMinimalAcl(parentDefaultEntries);
    for (AclEntry entry: parentDefaultEntries) {
      AclEntryType type = entry.getType();
      String name = entry.getName();
      AclEntry.Builder builder = new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(type)
        .setName(name);

      // The child's initial permission bits are treated as the mode parameter,
      // which can filter copied permission values for owner, mask and other.
      final FsAction permission;
      if (type == AclEntryType.USER && name == null) {
        permission = entry.getPermission().and(childPerm.getUserAction());
      } else if (type == AclEntryType.GROUP && parentDefaultIsMinimal) {
        // This only happens if the default ACL is a minimal ACL: exactly 3
        // entries corresponding to owner, group and other.  In this case,
        // filter the group permissions.
        permission = entry.getPermission().and(childPerm.getGroupAction());
      } else if (type == AclEntryType.MASK) {
        // Group bits from mode parameter filter permission of mask entry.
        permission = entry.getPermission().and(childPerm.getGroupAction());
      } else if (type == AclEntryType.OTHER) {
        permission = entry.getPermission().and(childPerm.getOtherAction());
      } else {
        permission = entry.getPermission();
      }

      builder.setPermission(permission);
      accessEntries.add(builder.build());
    }

    // A new directory also receives a copy of the parent's default ACL.
    List<AclEntry> defaultEntries = child.isDirectory() ? parentDefaultEntries :
      Collections.<AclEntry>emptyList();

    final FsPermission newPerm;
    if (!AclUtil.isMinimalAcl(accessEntries) || !defaultEntries.isEmpty()) {
      // Save the new ACL to the child.
      child.addAclFeature(createAclFeature(accessEntries, defaultEntries));
      newPerm = createFsPermissionForExtendedAcl(accessEntries, childPerm);
    } else {
      // The child is receiving a minimal ACL.
      newPerm = createFsPermissionForMinimalAcl(accessEntries, childPerm);
    }

    child.setPermission(newPerm);
    return true;
  }

  /**
   * Reads the existing extended ACL entries of an inode.  This method returns
   * only the extended ACL entries stored in the AclFeature.  If the inode does
   * not have an ACL, then this method returns an empty list.  This method
   * supports querying by snapshot ID.
   *
   * @param inode INode to read
   * @param snapshotId int ID of snapshot to read
   * @return List<AclEntry> containing extended inode ACL entries
   */
  public static List<AclEntry> readINodeAcl(INode inode, int snapshotId) {
    AclFeature f = inode.getAclFeature(snapshotId);
    return getEntriesFromAclFeature(f);
  }

  /**
   * Reads the existing extended ACL entries of an INodeAttribute object.
   *
   * @param inodeAttr INode to read
   * @return List<AclEntry> containing extended inode ACL entries
   */
  public static List<AclEntry> readINodeAcl(INodeAttributes inodeAttr) {
    AclFeature f = inodeAttr.getAclFeature();
    return getEntriesFromAclFeature(f);
  }

  /**
   * Build list of AclEntries from the AclFeature
   * @param aclFeature AclFeature
   * @return List of entries
   */
  @VisibleForTesting
  static ImmutableList<AclEntry> getEntriesFromAclFeature(AclFeature aclFeature) {
    if (aclFeature == null) {
      return ImmutableList.<AclEntry> of();
    }
    ImmutableList.Builder<AclEntry> b = new ImmutableList.Builder<AclEntry>();
    for (int pos = 0, entry; pos < aclFeature.getEntriesSize(); pos++) {
      entry = aclFeature.getEntryAt(pos);
      b.add(AclEntryStatusFormat.toAclEntry(entry));
    }
    return b.build();
  }

  /**
   * Reads the existing ACL of an inode.  This method always returns the full
   * logical ACL of the inode after reading relevant data from the inode's
   * {@link FsPermission} and {@link AclFeature}.  Note that every inode
   * logically has an ACL, even if no ACL has been set explicitly.  If the inode
   * does not have an extended ACL, then the result is a minimal ACL consising of
   * exactly 3 entries that correspond to the owner, group and other permissions.
   * This method always reads the inode's current state and does not support
   * querying by snapshot ID.  This is because the method is intended to support
   * ACL modification APIs, which always apply a delta on top of current state.
   *
   * @param inode INode to read
   * @return List<AclEntry> containing all logical inode ACL entries
   */
  public static List<AclEntry> readINodeLogicalAcl(INode inode) {
    FsPermission perm = inode.getFsPermission();
    AclFeature f = inode.getAclFeature();
    if (f == null) {
      return AclUtil.getMinimalAcl(perm);
    }

    final List<AclEntry> existingAcl;
    // Split ACL entries stored in the feature into access vs. default.
    List<AclEntry> featureEntries = getEntriesFromAclFeature(f);
    ScopedAclEntries scoped = new ScopedAclEntries(featureEntries);
    List<AclEntry> accessEntries = scoped.getAccessEntries();
    List<AclEntry> defaultEntries = scoped.getDefaultEntries();

    // Pre-allocate list size for the explicit entries stored in the feature
    // plus the 3 implicit entries (owner, group and other) from the permission
    // bits.
    existingAcl = Lists.newArrayListWithCapacity(featureEntries.size() + 3);

    if (!accessEntries.isEmpty()) {
      // Add owner entry implied from user permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.USER).setPermission(perm.getUserAction())
          .build());

      // Next add all named user and group entries taken from the feature.
      existingAcl.addAll(accessEntries);

      // Add mask entry implied from group permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.MASK).setPermission(perm.getGroupAction())
          .build());

      // Add other entry implied from other permission bits.
      existingAcl.add(new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.OTHER).setPermission(perm.getOtherAction())
          .build());
    } else {
      // It's possible that there is a default ACL but no access ACL. In this
      // case, add the minimal access ACL implied by the permission bits.
      existingAcl.addAll(AclUtil.getMinimalAcl(perm));
    }

    // Add all default entries after the access entries.
    existingAcl.addAll(defaultEntries);

    // The above adds entries in the correct order, so no need to sort here.
    return existingAcl;
  }

  /**
   * Updates an inode with a new ACL.  This method takes a full logical ACL and
   * stores the entries to the inode's {@link FsPermission} and
   * {@link AclFeature}.
   *
   * @param inode INode to update
   * @param newAcl List<AclEntry> containing new ACL entries
   * @param snapshotId int latest snapshot ID of inode
   * @throws AclException if the ACL is invalid for the given inode
   * @throws QuotaExceededException if quota limit is exceeded
   */
  public static void updateINodeAcl(INode inode, List<AclEntry> newAcl,
      int snapshotId) throws AclException, QuotaExceededException {
    assert newAcl.size() >= 3;
    FsPermission perm = inode.getFsPermission();
    final FsPermission newPerm;
    if (!AclUtil.isMinimalAcl(newAcl)) {
      // This is an extended ACL.  Split entries into access vs. default.
      ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
      List<AclEntry> accessEntries = scoped.getAccessEntries();
      List<AclEntry> defaultEntries = scoped.getDefaultEntries();

      // Only directories may have a default ACL.
      if (!defaultEntries.isEmpty() && !inode.isDirectory()) {
        throw new AclException(
          "Invalid ACL: only directories may have a default ACL. "
            + "Path: " + inode.getFullPathName());
      }

      // Attach entries to the feature.
      if (inode.getAclFeature() != null) {
        inode.removeAclFeature(snapshotId);
      }
      inode.addAclFeature(createAclFeature(accessEntries, defaultEntries),
        snapshotId);
      newPerm = createFsPermissionForExtendedAcl(accessEntries, perm);
    } else {
      // This is a minimal ACL.  Remove the ACL feature if it previously had one.
      if (inode.getAclFeature() != null) {
        inode.removeAclFeature(snapshotId);
      }
      newPerm = createFsPermissionForMinimalAcl(newAcl, perm);
    }

    inode.setPermission(newPerm, snapshotId);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclStorage() {
  }

  /**
   * Creates an AclFeature from the given ACL entries.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param defaultEntries List<AclEntry> default ACL entries
   * @return AclFeature containing the required ACL entries
   */
  private static AclFeature createAclFeature(List<AclEntry> accessEntries,
      List<AclEntry> defaultEntries) {
    // Pre-allocate list size for the explicit entries stored in the feature,
    // which is all entries minus the 3 entries implicitly stored in the
    // permission bits.
    List<AclEntry> featureEntries = Lists.newArrayListWithCapacity(
      (accessEntries.size() - 3) + defaultEntries.size());

    // For the access ACL, the feature only needs to hold the named user and
    // group entries.  For a correctly sorted ACL, these will be in a
    // predictable range.
    if (!AclUtil.isMinimalAcl(accessEntries)) {
      featureEntries.addAll(
        accessEntries.subList(1, accessEntries.size() - 2));
    }

    // Add all default entries to the feature.
    featureEntries.addAll(defaultEntries);
    return new AclFeature(AclEntryStatusFormat.toInt(featureEntries));
  }

  /**
   * Creates the new FsPermission for an inode that is receiving an extended
   * ACL, based on its access ACL entries.  For a correctly sorted ACL, the
   * first entry is the owner and the last 2 entries are the mask and other
   * entries respectively.  Also preserve sticky bit and toggle ACL bit on.
   * Note that this method intentionally copies the permissions of the mask
   * entry into the FsPermission group permissions.  This is consistent with the
   * POSIX ACLs model, which presents the mask as the permissions of the group
   * class.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param existingPerm FsPermission existing permissions
   * @return FsPermission new permissions
   */
  private static FsPermission createFsPermissionForExtendedAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(accessEntries.size() - 2).getPermission(),
      accessEntries.get(accessEntries.size() - 1).getPermission(),
      existingPerm.getStickyBit());
  }

  /**
   * Creates the new FsPermission for an inode that is receiving a minimal ACL,
   * based on its access ACL entries.  For a correctly sorted ACL, the owner,
   * group and other permissions are in order.  Also preserve sticky bit and
   * toggle ACL bit off.
   *
   * @param accessEntries List<AclEntry> access ACL entries
   * @param existingPerm FsPermission existing permissions
   * @return FsPermission new permissions
   */
  private static FsPermission createFsPermissionForMinimalAcl(
      List<AclEntry> accessEntries, FsPermission existingPerm) {
    return new FsPermission(accessEntries.get(0).getPermission(),
      accessEntries.get(1).getPermission(),
      accessEntries.get(2).getPermission(),
      existingPerm.getStickyBit());
  }

  @VisibleForTesting
  public static ReferenceCountMap<AclFeature> getUniqueAclFeatures() {
    return UNIQUE_ACL_FEATURES;
  }

  /**
   * Add reference for the said AclFeature
   * 
   * @param aclFeature
   * @return Referenced AclFeature
   */
  public static AclFeature addAclFeature(AclFeature aclFeature) {
    return UNIQUE_ACL_FEATURES.put(aclFeature);
  }

  /**
   * Remove reference to the AclFeature
   * 
   * @param aclFeature
   */
  public static void removeAclFeature(AclFeature aclFeature) {
    UNIQUE_ACL_FEATURES.remove(aclFeature);
  }
}
