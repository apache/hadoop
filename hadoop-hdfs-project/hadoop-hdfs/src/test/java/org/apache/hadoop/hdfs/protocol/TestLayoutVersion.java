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
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
   * Tests expected values for minimum compatible layout version in NameNode
   * features.  TRUNCATE, APPEND_NEW_BLOCK and QUOTA_BY_STORAGE_TYPE are all
   * features that launched in the same release.  TRUNCATE was added first, so
   * we expect all 3 features to have a minimum compatible layout version equal
   * to TRUNCATE's layout version.  All features older than that existed prior
   * to the concept of a minimum compatible layout version, so for each one, the
   * minimum compatible layout version must be equal to itself.
   */
  @Test
  public void testNameNodeFeatureMinimumCompatibleLayoutVersions() {
    int baseLV = NameNodeLayoutVersion.Feature.TRUNCATE.getInfo()
        .getLayoutVersion();
    EnumSet<NameNodeLayoutVersion.Feature> compatibleFeatures = EnumSet.of(
        NameNodeLayoutVersion.Feature.TRUNCATE,
        NameNodeLayoutVersion.Feature.APPEND_NEW_BLOCK,
        NameNodeLayoutVersion.Feature.QUOTA_BY_STORAGE_TYPE,
        NameNodeLayoutVersion.Feature.ERASURE_CODING,
        NameNodeLayoutVersion.Feature.EXPANDED_STRING_TABLE,
        NameNodeLayoutVersion.Feature.SNAPSHOT_MODIFICATION_TIME);
    for (LayoutFeature f : compatibleFeatures) {
      assertEquals(String.format("Expected minimum compatible layout version " +
          "%d for feature %s.", baseLV, f), baseLV,
          f.getInfo().getMinimumCompatibleLayoutVersion());
    }
    List<LayoutFeature> features = new ArrayList<>();
    features.addAll(EnumSet.allOf(LayoutVersion.Feature.class));
    features.addAll(EnumSet.allOf(NameNodeLayoutVersion.Feature.class));
    for (LayoutFeature f : features) {
      if (!compatibleFeatures.contains(f)) {
        assertEquals(String.format("Expected feature %s to have minimum " +
            "compatible layout version set to itself.", f),
            f.getInfo().getLayoutVersion(),
            f.getInfo().getMinimumCompatibleLayoutVersion());
      }
    }
  }

  /**
   * Tests that NameNode features are listed in order of minimum compatible
   * layout version.  It would be inconsistent to have features listed out of
   * order with respect to minimum compatible layout version, because it would
   * imply going back in time to change compatibility logic in a software release
   * that had already shipped.
   */
  @Test
  public void testNameNodeFeatureMinimumCompatibleLayoutVersionAscending() {
    LayoutFeature prevF = null;
    for (LayoutFeature f : EnumSet.allOf(NameNodeLayoutVersion.Feature.class)) {
      if (prevF != null) {
        assertTrue(String.format("Features %s and %s not listed in order of " +
            "minimum compatible layout version.", prevF, f),
            f.getInfo().getMinimumCompatibleLayoutVersion() <=
            prevF.getInfo().getMinimumCompatibleLayoutVersion());
      } else {
        prevF = f;
      }
    }
  }

  /**
   * Tests that attempting to add a new NameNode feature out of order with
   * respect to minimum compatible layout version will fail fast.
   */
  @Test(expected=AssertionError.class)
  public void testNameNodeFeatureMinimumCompatibleLayoutVersionOutOfOrder() {
    FeatureInfo ancestorF = LayoutVersion.Feature.RESERVED_REL2_4_0.getInfo();
    LayoutFeature f = mock(LayoutFeature.class);
    when(f.getInfo()).thenReturn(new FeatureInfo(
        ancestorF.getLayoutVersion() - 1, ancestorF.getLayoutVersion(),
        ancestorF.getMinimumCompatibleLayoutVersion() + 1, "Invalid feature.",
        false));
    Map<Integer, SortedSet<LayoutFeature>> features = new HashMap<>();
    LayoutVersion.updateMap(features, LayoutVersion.Feature.values());
    LayoutVersion.updateMap(features, new LayoutFeature[] { f });
  }

  /**
   * Asserts the current minimum compatible layout version of the software, if a
   * release were created from the codebase right now.  This test is meant to
   * make developers stop and reconsider if they introduce a change that requires
   * a new minimum compatible layout version.  This would make downgrade
   * impossible.
   */
  @Test
  public void testCurrentMinimumCompatibleLayoutVersion() {
    int expectedMinCompatLV = NameNodeLayoutVersion.Feature.TRUNCATE.getInfo()
        .getLayoutVersion();
    int actualMinCompatLV = LayoutVersion.getMinimumCompatibleLayoutVersion(
        NameNodeLayoutVersion.Feature.values());
    assertEquals("The minimum compatible layout version has changed.  " +
        "Downgrade to prior versions is no longer possible.  Please either " +
        "restore compatibility, or if the incompatibility is intentional, " +
        "then update this assertion.", expectedMinCompatLV, actualMinCompatLV);
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
