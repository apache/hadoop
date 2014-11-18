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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.SortedSet;

import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.FeatureInfo;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.junit.Test;

/**
 * Test for {@link LayoutVersion}
 */
public class TestLayoutVersion {
  public static final LayoutFeature LAST_NON_RESERVED_COMMON_FEATURE;
  public static final LayoutFeature LAST_COMMON_FEATURE;
  static {
    final Feature[] features = Feature.values();
    LAST_COMMON_FEATURE = features[features.length - 1];
    LAST_NON_RESERVED_COMMON_FEATURE = LayoutVersion.getLastNonReservedFeature(features);
  }
  
  /**
   * Tests to make sure a given layout version supports all the
   * features from the ancestor
   */
  @Test
  public void testFeaturesFromAncestorSupported() {
    for (LayoutFeature f : Feature.values()) {
      validateFeatureList(f);
    }
  }
  
  /**
   * Test to make sure 0.20.203 supports delegation token
   */
  @Test
  public void testRelease203() {
    assertTrue(NameNodeLayoutVersion.supports(LayoutVersion.Feature.DELEGATION_TOKEN, 
        Feature.RESERVED_REL20_203.getInfo().getLayoutVersion()));
  }
  
  /**
   * Test to make sure 0.20.204 supports delegation token
   */
  @Test
  public void testRelease204() {
    assertTrue(NameNodeLayoutVersion.supports(LayoutVersion.Feature.DELEGATION_TOKEN, 
        Feature.RESERVED_REL20_204.getInfo().getLayoutVersion()));
  }
  
  /**
   * Test to make sure release 1.2.0 support CONCAT
   */
  @Test
  public void testRelease1_2_0() {
    assertTrue(NameNodeLayoutVersion.supports(LayoutVersion.Feature.CONCAT, 
        Feature.RESERVED_REL1_2_0.getInfo().getLayoutVersion()));
  }
  
  /**
   * Test to make sure NameNode.Feature support previous features
   */
  @Test
  public void testNameNodeFeature() {
    final LayoutFeature first = NameNodeLayoutVersion.Feature.ROLLING_UPGRADE; 
    assertTrue(NameNodeLayoutVersion.supports(LAST_NON_RESERVED_COMMON_FEATURE,
        first.getInfo().getLayoutVersion()));
    assertEquals(LAST_COMMON_FEATURE.getInfo().getLayoutVersion() - 1,
        first.getInfo().getLayoutVersion());
  }
  
  /**
   * Test to make sure DataNode.Feature support previous features
   */
  @Test
  public void testDataNodeFeature() {
    final LayoutFeature first = DataNodeLayoutVersion.Feature.FIRST_LAYOUT; 
    assertTrue(DataNodeLayoutVersion.supports(LAST_NON_RESERVED_COMMON_FEATURE,
        first.getInfo().getLayoutVersion()));
    assertEquals(LAST_COMMON_FEATURE.getInfo().getLayoutVersion() - 1,
        first.getInfo().getLayoutVersion());
  }
  
  /**
   * Given feature {@code f}, ensures the layout version of that feature
   * supports all the features supported by it's ancestor.
   */
  private void validateFeatureList(LayoutFeature f) {
    final FeatureInfo info = f.getInfo();
    int lv = info.getLayoutVersion();
    int ancestorLV = info.getAncestorLayoutVersion();
    SortedSet<LayoutFeature> ancestorSet = NameNodeLayoutVersion.getFeatures(ancestorLV);
    assertNotNull(ancestorSet);
    for (LayoutFeature  feature : ancestorSet) {
      assertTrue("LV " + lv + " does nto support " + feature
          + " supported by the ancestor LV " + info.getAncestorLayoutVersion(),
          NameNodeLayoutVersion.supports(feature, lv));
    }
  }
  
  /**
   * When a LayoutVersion support SNAPSHOT, it must support
   * FSIMAGE_NAME_OPTIMIZATION.
   */
  @Test
  public void testSNAPSHOT() {
    for(Feature f : Feature.values()) {
      final int version = f.getInfo().getLayoutVersion();
      if (NameNodeLayoutVersion.supports(Feature.SNAPSHOT, version)) {
        assertTrue(NameNodeLayoutVersion.supports(
            Feature.FSIMAGE_NAME_OPTIMIZATION, version));
      }
    }
  }
}
