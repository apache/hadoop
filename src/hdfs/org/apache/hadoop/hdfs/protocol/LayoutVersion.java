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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

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
   * Enums for features that change the layout version.
   * <br><br>
   * To add a new layout version:
   * <ul>
   * <li>Define a new enum constant with a short enum name, the new layout version 
   * and description of the added feature.</li>
   * <li>When adding a layout version with an ancestor that is not same as
   * its immediate predecessor, use the constructor where a spacific ancestor
   * can be passed.
   * </li>
   * </ul>
   */
  public static enum Feature {
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
    // layout versions -35 - -40 are features not present on this branch
    RESERVED_REL1_2_0(-41, -32, "Reserved for release 1.2.0", true, CONCAT);
    
    final int lv;
    final int ancestorLV;
    final String description;
    final boolean reserved;
    final Feature[] specialFeatures;
    
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
     *          verions
     * @param features set of features that are to be enabled for this version
     */
    Feature(final int lv, final int ancestorLV, final String description,
        boolean reserved, Feature... features) {
      this.lv = lv;
      this.ancestorLV = ancestorLV;
      this.description = description;
      this.reserved = reserved;
      specialFeatures = features;
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
  }
  
  // Build layout version and corresponding feature matrix
  static final Map<Integer, EnumSet<Feature>>map = 
    new HashMap<Integer, EnumSet<Feature>>();
  
  // Static initialization 
  static {
    initMap();
  }
  
  /**
   * Initialize the map of a layout version and EnumSet of {@link Feature}s 
   * supported.
   */
  private static void initMap() {
    // Go through all the enum constants and build a map of
    // LayoutVersion <-> EnumSet of all supported features in that LayoutVersion
    for (Feature f : Feature.values()) {
      EnumSet<Feature> ancestorSet = map.get(f.ancestorLV);
      if (ancestorSet == null) {
        ancestorSet = EnumSet.noneOf(Feature.class); // Empty enum set
        map.put(f.ancestorLV, ancestorSet);
      }
      EnumSet<Feature> featureSet = EnumSet.copyOf(ancestorSet);
      if (f.specialFeatures != null) {
        for (Feature specialFeature : f.specialFeatures) {
          featureSet.add(specialFeature);
        }
      }
      featureSet.add(f);
      map.put(f.lv, featureSet);
    }
  }
  
  /**
   * Gets formatted string that describes {@link LayoutVersion} information.
   */
  public static String getString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Feature List:\n");
    for (Feature f : Feature.values()) {
      buf.append(f).append(" introduced in layout version ")
          .append(f.lv).append(" (").
      append(f.description).append(")\n");
    }
    
    buf.append("\n\nLayoutVersion and supported features:\n");
    for (Feature f : Feature.values()) {
      buf.append(f.lv).append(": ").append(map.get(f.lv))
          .append("\n");
    }
    return buf.toString();
  }
  
  /**
   * Returns true if a given feature is supported in the given layout version
   * @param f Feature
   * @param lv LayoutVersion
   * @return true if {@code f} is supported in layout version {@code lv}
   */
  public static boolean supports(final Feature f, final int lv) {
    final EnumSet<Feature> set =  map.get(lv);
    return set != null && set.contains(f);
  }
  
  /**
   * Get the current layout version
   */
  public static int getCurrentLayoutVersion() {
    Feature[] values = Feature.values();
    for (int i = values.length -1; i >= 0; i--) {
      if (!values[i].isReservedForOldRelease()) {
        return values[i].lv;
      }
    }
    throw new AssertionError("All layout versions are reserved.");
  }
}
