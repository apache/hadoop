/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils.DEFAULT_POLICY_KEY;
import static org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils.FEDERATION_POLICY_LABEL_TAG_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link FederationPolicyUtils}.
 */
public class TestFederationPolicyUtils {

  @Test
  public void testGetWeightedRandom() {
    int i;
    float[] weights =
        new float[] {0, 0.1f, 0.2f, 0.2f, -0.1f, 0.1f, 0.2f, 0.1f, 0.1f};
    float[] expectedWeights =
        new float[] {0, 0.1f, 0.2f, 0.2f, 0, 0.1f, 0.2f, 0.1f, 0.1f};
    int[] result = new int[weights.length];

    ArrayList<Float> weightsList = new ArrayList<>();
    for (float weight : weights) {
      weightsList.add(weight);
    }

    int n = 10000000;
    for (i = 0; i < n; i++) {
      int sample = FederationPolicyUtils.getWeightedRandom(weightsList);
      result[sample]++;
    }
    for (i = 0; i < weights.length; i++) {
      double actualWeight = (float) result[i] / n;
      System.out.println(i + " " + actualWeight);
      Assert.assertTrue(
          "Index " + i + " Actual weight: " + actualWeight
              + " expected weight: " + expectedWeights[i],
          Math.abs(actualWeight - expectedWeights[i]) < 0.01);
    }
  }

  @Test
  public void testGetApplicationPolicyTag() {
    Assert.assertEquals(DEFAULT_POLICY_KEY, FederationPolicyUtils.getApplicationPolicyTag(null));
    ApplicationSubmissionContext context = mock(ApplicationSubmissionContext.class);
    when(context.getApplicationTags()).thenReturn(new HashSet<>());
    Assert.assertEquals(DEFAULT_POLICY_KEY, FederationPolicyUtils.getApplicationPolicyTag(context));
    when(context.getApplicationTags()).thenReturn(
        Sets.newHashSet(FEDERATION_POLICY_LABEL_TAG_PREFIX + ":LABEL1"));
    Assert.assertEquals("LABEL1", FederationPolicyUtils.getApplicationPolicyTag(context));
  }
}
