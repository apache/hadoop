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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.FeatureInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;

@InterfaceAudience.Private
public class DataNodeLayoutVersion {  
  /** Build layout version and corresponding feature matrix */
  public final static Map<Integer, SortedSet<LayoutFeature>> FEATURES = 
    new HashMap<Integer, SortedSet<LayoutFeature>>();
  
  public static final int CURRENT_LAYOUT_VERSION
      = LayoutVersion.getCurrentLayoutVersion(Feature.values());

  static{
    LayoutVersion.updateMap(FEATURES, LayoutVersion.Feature.values());
    LayoutVersion.updateMap(FEATURES, DataNodeLayoutVersion.Feature.values());
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
   * </ul>
   */
  public static enum Feature implements LayoutFeature {
    FIRST_LAYOUT(-55, -53, "First datanode layout", false),
    BLOCKID_BASED_LAYOUT(-56,
        "The block ID of a finalized block uniquely determines its position " +
        "in the directory structure"),
    BLOCKID_BASED_LAYOUT_32_by_32(-57,
        "Identical to the block id based layout (-56) except it uses a smaller"
        + " directory structure (32x32)");
   
    private final FeatureInfo info;

    /**
     * DataNodeFeature that is added at layout version {@code lv} - 1. 
     * @param lv new layout version with the addition of this feature
     * @param description description of the feature
     */
    Feature(final int lv, final String description) {
      this(lv, lv + 1, description, false);
    }

    /**
     * DataNode feature that is added at layout version {@code ancestoryLV}.
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
}
