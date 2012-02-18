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

import static org.junit.Assert.*;
import org.junit.Test;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;

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
    for (Feature f : Feature.values()) {
      validateFeatureList(f);
    }
  }
  
  /**
   * Test to make sure 0.20.203 supports delegation token
   */
  @Test
  public void testRelease203() {
    assertTrue(LayoutVersion.supports(Feature.DELEGATION_TOKEN, 
        Feature.RESERVED_REL20_203.lv));
  }
  
  /**
   * Test to make sure 0.20.204 supports delegation token
   */
  @Test
  public void testRelease204() {
    assertTrue(LayoutVersion.supports(Feature.DELEGATION_TOKEN, 
        Feature.RESERVED_REL20_204.lv));
  }
  
  /**
   * Given feature {@code f}, ensures the layout version of that feature
   * supports all the features supported by it's ancestor.
   */
  private void validateFeatureList(Feature f) {
    int lv = f.lv;
    int ancestorLV = f.ancestorLV;
    EnumSet<Feature> ancestorSet = LayoutVersion.map.get(ancestorLV);
    assertNotNull(ancestorSet);
    for (Feature  feature : ancestorSet) {
      assertTrue(LayoutVersion.supports(feature, lv));
    }
  }
}
