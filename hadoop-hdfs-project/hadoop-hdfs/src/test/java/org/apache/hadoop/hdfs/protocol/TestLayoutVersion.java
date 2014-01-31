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
    assertTrue(NameNodeLayoutVersion.supports(LayoutVersion.Feature.CACHING,
        NameNodeLayoutVersion.Feature.ROLLING_UPGRADE_MARKER.getInfo().getLayoutVersion()));
  }
  
  /**
   * Test to make sure DataNode.Feature support previous features
   */
  @Test
  public void testDataNodeFeature() {
    assertTrue(DataNodeLayoutVersion.supports(LayoutVersion.Feature.CACHING,
        DataNodeLayoutVersion.Feature.FIRST_LAYOUT.getInfo().getLayoutVersion()));
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
}
