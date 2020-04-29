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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.FeatureInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;


@InterfaceAudience.Private
public class NameNodeLayoutVersion { 
  /** Build layout version and corresponding feature matrix */
  public final static Map<Integer, SortedSet<LayoutFeature>> FEATURES
      = new HashMap<Integer, SortedSet<LayoutFeature>>();

  public static final int CURRENT_LAYOUT_VERSION
      = LayoutVersion.getCurrentLayoutVersion(Feature.values());
  public static final int MINIMUM_COMPATIBLE_LAYOUT_VERSION
      = LayoutVersion.getMinimumCompatibleLayoutVersion(Feature.values());

  static {
    LayoutVersion.updateMap(FEATURES, LayoutVersion.Feature.values());
    LayoutVersion.updateMap(FEATURES, NameNodeLayoutVersion.Feature.values());
  }
  
  public static SortedSet<LayoutFeature> getFeatures(int lv) {
    return FEATURES.get(lv);
  }

  public static boolean supports(final LayoutFeature f, final int lv) {
    return LayoutVersion.supports(FEATURES, f, lv);
  }

  /**
   * Enums for features that change the layout version.
   * <br><br>
   * To add a new layout version:
   * <ul>
   * <li>Define a new enum constant with a short enum name, the new layout version 
   * and description of the added feature.</li>
   * <li>When adding a layout version with an ancestor that is not same as
   * its immediate predecessor, use the constructor where a specific ancestor
   * can be passed.
   * </li>
   * <li>Specify a minimum compatible layout version.  The minimum compatible
   * layout version is the earliest prior version to which a downgrade is
   * possible after initiating rolling upgrade.  If the feature cannot satisfy
   * compatibility with any prior version, then set its minimum compatible
   * lqyout version to itself to indicate that downgrade is impossible.
   * Satisfying compatibility might require adding logic to the new feature to
   * reject operations or handle them differently while rolling upgrade is in
   * progress.  In general, it's possible to satisfy compatiblity for downgrade
   * if the new feature just involves adding new edit log ops.  Deeper
   * structural changes, such as changing the way we place files in the metadata
   * directories, might be incompatible.  Feature implementations should strive
   * for compatibility, because it's in the best interest of our users to
   * support downgrade.
   * </ul>
   */
  public enum Feature implements LayoutFeature {
    ROLLING_UPGRADE(-55, -53, -55, "Support rolling upgrade", false),
    EDITLOG_LENGTH(-56, -56, "Add length field to every edit log op"),
    XATTRS(-57, -57, "Extended attributes"),
    CREATE_OVERWRITE(-58, -58, "Use single editlog record for " +
      "creating file with overwrite"),
    XATTRS_NAMESPACE_EXT(-59, -59, "Increase number of xattr namespaces"),
    BLOCK_STORAGE_POLICY(-60, -60, "Block Storage policy"),
    TRUNCATE(-61, -61, "Truncate"),
    APPEND_NEW_BLOCK(-62, -61, "Support appending to new block"),
    QUOTA_BY_STORAGE_TYPE(-63, -61, "Support quota for specific storage types"),
    ERASURE_CODING(-64, -61, "Support erasure coding"),
    EXPANDED_STRING_TABLE(-65, -61, "Support expanded string table in fsimage");

    private final FeatureInfo info;

    /**
     * Feature that is added at layout version {@code lv} - 1. 
     * @param lv new layout version with the addition of this feature
     * @param minCompatLV minimium compatible layout version
     * @param description description of the feature
     */
    Feature(final int lv, int minCompatLV, final String description) {
      this(lv, lv + 1, minCompatLV, description, false);
    }

    /**
     * NameNode feature that is added at layout version {@code ancestoryLV}.
     * @param lv new layout version with the addition of this feature
     * @param ancestorLV layout version from which the new lv is derived from.
     * @param minCompatLV minimum compatible layout version
     * @param description description of the feature
     * @param reserved true when this is a layout version reserved for previous
     *        versions
     * @param features set of features that are to be enabled for this version
     */
    Feature(final int lv, final int ancestorLV, int minCompatLV,
        final String description, boolean reserved, Feature... features) {
      info = new FeatureInfo(lv, ancestorLV, minCompatLV, description, reserved,
          features);
    }
    
    @Override
    public FeatureInfo getInfo() {
      return info;
    }
  }
}
