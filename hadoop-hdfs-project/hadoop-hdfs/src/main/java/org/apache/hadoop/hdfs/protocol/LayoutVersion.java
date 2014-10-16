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

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class tracks changes in the layout version of HDFS.
 * 
 * Layout version is changed for following reasons:
 * <ol>
 * <li>The layout of how namenode or datanode stores information 
 * on disk changes.</li>
 * <li>A new operation code is added to the editlog.</li>
 * <li>Modification such as format of a record, content of a record 
 * in editlog or fsimage.</li>
 * </ol>
 * <br>
 * <b>How to update layout version:<br></b>
 * When a change requires new layout version, please add an entry into
 * {@link Feature} with a short enum name, new layout version and description
 * of the change. Please see {@link Feature} for further details.
 * <br>
 */
@InterfaceAudience.Private
public class LayoutVersion {
  /**
   * Version in which HDFS-2991 was fixed. This bug caused OP_ADD to
   * sometimes be skipped for append() calls. If we see such a case when
   * loading the edits, but the version is known to have that bug, we
   * workaround the issue. Otherwise we should consider it a corruption
   * and bail.
   */
  public static final int BUGFIX_HDFS_2991_VERSION = -40;

  /**
   * The interface to be implemented by NameNode and DataNode layout features 
   */
  public interface LayoutFeature {
    public FeatureInfo getInfo();
  }

  /**
   * Enums for features that change the layout version before rolling
   * upgrade is supported.
   * <br><br>
   * To add a new layout version:
   * <ul>
   * <li>Define a new enum constant with a short enum name, the new layout version 
   * and description of the added feature.</li>
   * <li>When adding a layout version with an ancestor that is not same as
   * its immediate predecessor, use the constructor where a specific ancestor
   * can be passed.
   * </li>
   * </ul>
   */
  public static enum Feature implements LayoutFeature {
    NAMESPACE_QUOTA(-16, "Support for namespace quotas"),
    FILE_ACCESS_TIME(-17, "Support for access time on files"),
    DISKSPACE_QUOTA(-18, "Support for disk space quotas"),
    STICKY_BIT(-19, "Support for sticky bits"),
    APPEND_RBW_DIR(-20, "Datanode has \"rbw\" subdirectory for append"),
    ATOMIC_RENAME(-21, "Support for atomic rename"),
    CONCAT(-22, "Support for concat operation"),
    SYMLINKS(-23, "Support for symbolic links"),
    DELEGATION_TOKEN(-24, "Support for delegation tokens for security"),
    FSIMAGE_COMPRESSION(-25, "Support for fsimage compression"),
    FSIMAGE_CHECKSUM(-26, "Support checksum for fsimage"),
    REMOVE_REL13_DISK_LAYOUT_SUPPORT(-27, "Remove support for 0.13 disk layout"),
    EDITS_CHESKUM(-28, "Support checksum for editlog"),
    UNUSED(-29, "Skipped version"),
    FSIMAGE_NAME_OPTIMIZATION(-30, "Store only last part of path in fsimage"),
    RESERVED_REL20_203(-31, -19, "Reserved for release 0.20.203", true,
        DELEGATION_TOKEN),
    RESERVED_REL20_204(-32, -31, "Reserved for release 0.20.204", true),
    RESERVED_REL22(-33, -27, "Reserved for release 0.22", true),
    RESERVED_REL23(-34, -30, "Reserved for release 0.23", true),
    FEDERATION(-35, "Support for namenode federation"),
    LEASE_REASSIGNMENT(-36, "Support for persisting lease holder reassignment"),
    STORED_TXIDS(-37, "Transaction IDs are stored in edits log and image files"),
    TXID_BASED_LAYOUT(-38, "File names in NN Storage are based on transaction IDs"), 
    EDITLOG_OP_OPTIMIZATION(-39,
        "Use LongWritable and ShortWritable directly instead of ArrayWritable of UTF8"),
    OPTIMIZE_PERSIST_BLOCKS(-40,
        "Serialize block lists with delta-encoded variable length ints, " +
        "add OP_UPDATE_BLOCKS"),
    RESERVED_REL1_2_0(-41, -32, "Reserved for release 1.2.0", true, CONCAT),
    ADD_INODE_ID(-42, -40, "Assign a unique inode id for each inode", false),
    SNAPSHOT(-43, "Support for snapshot feature"),
    RESERVED_REL1_3_0(-44, -41, "Reserved for release 1.3.0", true,
    		ADD_INODE_ID, SNAPSHOT, FSIMAGE_NAME_OPTIMIZATION),
    OPTIMIZE_SNAPSHOT_INODES(-45, -43,
        "Reduce snapshot inode memory footprint", false),
    SEQUENTIAL_BLOCK_ID(-46, "Allocate block IDs sequentially and store " +
        "block IDs in the edits log and image files"),
    EDITLOG_SUPPORT_RETRYCACHE(-47, "Record ClientId and CallId in editlog to " 
        + "enable rebuilding retry cache in case of HA failover"),
    EDITLOG_ADD_BLOCK(-48, "Add new editlog that only records allocation of "
        + "the new block instead of the entire block list"),
    ADD_DATANODE_AND_STORAGE_UUIDS(-49, "Replace StorageID with DatanodeUuid."
        + " Use distinct StorageUuid per storage directory."),
    ADD_LAYOUT_FLAGS(-50, "Add support for layout flags."),
    CACHING(-51, "Support for cache pools and path-based caching"),
    // Hadoop 2.4.0
    PROTOBUF_FORMAT(-52, "Use protobuf to serialize FSImage"),
    EXTENDED_ACL(-53, "Extended ACL"),
    RESERVED_REL2_4_0(-54, -51, "Reserved for release 2.4.0", true,
        PROTOBUF_FORMAT, EXTENDED_ACL);

    private final FeatureInfo info;

    /**
     * Feature that is added at layout version {@code lv} - 1. 
     * @param lv new layout version with the addition of this feature
     * @param description description of the feature
     */
    Feature(final int lv, final String description) {
      this(lv, lv + 1, description, false);
    }

    /**
     * Feature that is added at layout version {@code ancestoryLV}.
     * @param lv new layout version with the addition of this feature
     * @param ancestorLV layout version from which the new lv is derived from.
     * @param description description of the feature
     * @param reserved true when this is a layout version reserved for previous
     *        version
     * @param features set of features that are to be enabled for this version
     */
    Feature(final int lv, final int ancestorLV, final String description,
        boolean reserved, Feature... features) {
      info = new FeatureInfo(lv, ancestorLV, description, reserved, features);
    }
    
    @Override
    public FeatureInfo getInfo() {
      return info;
    }
  }
  
  /** Feature information. */
  public static class FeatureInfo {
    private final int lv;
    private final int ancestorLV;
    private final String description;
    private final boolean reserved;
    private final LayoutFeature[] specialFeatures;

    public FeatureInfo(final int lv, final int ancestorLV, final String description,
        boolean reserved, LayoutFeature... specialFeatures) {
      this.lv = lv;
      this.ancestorLV = ancestorLV;
      this.description = description;
      this.reserved = reserved;
      this.specialFeatures = specialFeatures;
    }
    
    /** 
     * Accessor method for feature layout version 
     * @return int lv value
     */
    public int getLayoutVersion() {
      return lv;
    }

    /** 
     * Accessor method for feature ancestor layout version 
     * @return int ancestor LV value
     */
    public int getAncestorLayoutVersion() {
      return ancestorLV;
    }

    /** 
     * Accessor method for feature description 
     * @return String feature description 
     */
    public String getDescription() {
      return description;
    }
    
    public boolean isReservedForOldRelease() {
      return reserved;
    }
    
    public LayoutFeature[] getSpecialFeatures() {
      return specialFeatures;
    }
  }

  static class LayoutFeatureComparator implements Comparator<LayoutFeature> {
    @Override
    public int compare(LayoutFeature arg0, LayoutFeature arg1) {
      return arg0.getInfo().getLayoutVersion()
          - arg1.getInfo().getLayoutVersion();
    }
  }
 
  public static void updateMap(Map<Integer, SortedSet<LayoutFeature>> map,
      LayoutFeature[] features) {
    // Go through all the enum constants and build a map of
    // LayoutVersion <-> Set of all supported features in that LayoutVersion
    for (LayoutFeature f : features) {
      final FeatureInfo info = f.getInfo();
      SortedSet<LayoutFeature> ancestorSet = map.get(info.getAncestorLayoutVersion());
      if (ancestorSet == null) {
        // Empty set
        ancestorSet = new TreeSet<LayoutFeature>(new LayoutFeatureComparator());
        map.put(info.getAncestorLayoutVersion(), ancestorSet);
      }
      SortedSet<LayoutFeature> featureSet = new TreeSet<LayoutFeature>(ancestorSet);
      if (info.getSpecialFeatures() != null) {
        for (LayoutFeature specialFeature : info.getSpecialFeatures()) {
          featureSet.add(specialFeature);
        }
      }
      featureSet.add(f);
      map.put(info.getLayoutVersion(), featureSet);
    }
  }
  
  /**
   * Gets formatted string that describes {@link LayoutVersion} information.
   */
  public String getString(Map<Integer, SortedSet<LayoutFeature>> map,
      LayoutFeature[] values) {
    final StringBuilder buf = new StringBuilder();
    buf.append("Feature List:\n");
    for (LayoutFeature f : values) {
      final FeatureInfo info = f.getInfo();
      buf.append(f).append(" introduced in layout version ")
          .append(info.getLayoutVersion()).append(" (")
          .append(info.getDescription()).append(")\n");
    }

    buf.append("\n\nLayoutVersion and supported features:\n");
    for (LayoutFeature f : values) {
      final FeatureInfo info = f.getInfo();
      buf.append(info.getLayoutVersion()).append(": ")
          .append(map.get(info.getLayoutVersion())).append("\n");
    }
    return buf.toString();
  }
  
  /**
   * Returns true if a given feature is supported in the given layout version
   * @param map layout feature map
   * @param f Feature
   * @param lv LayoutVersion
   * @return true if {@code f} is supported in layout version {@code lv}
   */
  public static boolean supports(Map<Integer, SortedSet<LayoutFeature>> map,
      final LayoutFeature f, final int lv) {
    final SortedSet<LayoutFeature> set =  map.get(lv);
    return set != null && set.contains(f);
  }
  
  /**
   * Get the current layout version
   */
  public static int getCurrentLayoutVersion(LayoutFeature[] features) {
    return getLastNonReservedFeature(features).getInfo().getLayoutVersion();
  }

  static LayoutFeature getLastNonReservedFeature(LayoutFeature[] features) {
    for (int i = features.length -1; i >= 0; i--) {
      final FeatureInfo info = features[i].getInfo();
      if (!info.isReservedForOldRelease()) {
        return features[i];
      }
    }
    throw new AssertionError("All layout versions are reserved.");
  }
}

