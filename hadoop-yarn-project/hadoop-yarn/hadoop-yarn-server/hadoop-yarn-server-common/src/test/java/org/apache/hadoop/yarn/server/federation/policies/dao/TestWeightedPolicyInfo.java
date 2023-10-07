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

package org.apache.hadoop.yarn.server.federation.policies.dao;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.nio.ByteBuffer;

public class TestWeightedPolicyInfo {

  @Test
  public void testParsePoliciesFromJson() throws Exception {
    URL url =
        Thread.currentThread().getContextClassLoader().getResource("weighted_policy_info_1.json");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'federation_weights_1.json' dummy file in classpath");
    }
    File file = new File(url.getPath());
    int size = (int) file.length();
    byte[] contents = new byte[size];
    IOUtils.readFully(new FileInputStream(file), contents);
    ByteBuffer byteBuffer = ByteBuffer.wrap(contents);
    WeightedPolicyInfo policyInfo = WeightedPolicyInfo.fromByteBuffer(byteBuffer);
    Assertions.assertEquals(1.0F, policyInfo.getHeadroomAlpha());
    // Assert DEFAULT weights
    SubClusterIdInfo cluster1 = new SubClusterIdInfo("SC-1");
    SubClusterIdInfo cluster2 = new SubClusterIdInfo("SC-2");
    Assertions.assertEquals(2, policyInfo.getRouterPolicyWeights().size());
    Assertions.assertEquals(2, policyInfo.getAMRMPolicyWeights().size());
    Assertions.assertEquals(0.6F, policyInfo.getRouterPolicyWeights().get(cluster1));
    Assertions.assertEquals(0.4F, policyInfo.getRouterPolicyWeights().get(cluster2));
    Assertions.assertEquals(0.3F, policyInfo.getAMRMPolicyWeights().get(cluster1));
    Assertions.assertEquals(0.7F, policyInfo.getAMRMPolicyWeights().get(cluster2));

    // Assert weights of LABEL1 and LABEL2
    Assertions.assertEquals(3, policyInfo.getRouterPolicyWeightsMap().size());
    Assertions.assertEquals(3, policyInfo.getAmrmPolicyWeightsMap().size());
    Assertions.assertEquals(1F, policyInfo.getRouterPolicyWeights("LABEL1").get(cluster1));
    Assertions.assertEquals(0F, policyInfo.getRouterPolicyWeights("LABEL1").get(cluster2));
    Assertions.assertEquals(1F, policyInfo.getAMRMPolicyWeights("LABEL1").get(cluster1));
    Assertions.assertEquals(0F, policyInfo.getAMRMPolicyWeights("LABEL1").get(cluster2));
    Assertions.assertEquals(0F, policyInfo.getRouterPolicyWeights("LABEL2").get(cluster1));
    Assertions.assertEquals(1F, policyInfo.getRouterPolicyWeights("LABEL2").get(cluster2));
    Assertions.assertEquals(0F, policyInfo.getAMRMPolicyWeights("LABEL2").get(cluster1));
    Assertions.assertEquals(1F, policyInfo.getAMRMPolicyWeights("LABEL2").get(cluster2));
  }

  @Test
  public void testParsePoliciesFromJsonWithoutLabledWeights() throws Exception {
    URL url =
        Thread.currentThread().getContextClassLoader().getResource("weighted_policy_info_2.json");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'federation_weights_1.json' dummy file in classpath");
    }
    File file = new File(url.getPath());
    int size = (int) file.length();
    byte[] contents = new byte[size];
    IOUtils.readFully(new FileInputStream(file), contents);
    ByteBuffer byteBuffer = ByteBuffer.wrap(contents);
    WeightedPolicyInfo policyInfo = WeightedPolicyInfo.fromByteBuffer(byteBuffer);
    Assertions.assertEquals(1.0F, policyInfo.getHeadroomAlpha());
    // Assert DEFAULT weights
    SubClusterIdInfo cluster1 = new SubClusterIdInfo("SC-1");
    SubClusterIdInfo cluster2 = new SubClusterIdInfo("SC-2");
    Assertions.assertEquals(2, policyInfo.getRouterPolicyWeights().size());
    Assertions.assertEquals(2, policyInfo.getAMRMPolicyWeights().size());
    Assertions.assertEquals(0.1F, policyInfo.getRouterPolicyWeights().get(cluster1));
    Assertions.assertEquals(0.9F, policyInfo.getRouterPolicyWeights().get(cluster2));
    Assertions.assertEquals(0.2F, policyInfo.getAMRMPolicyWeights().get(cluster1));
    Assertions.assertEquals(0.8F, policyInfo.getAMRMPolicyWeights().get(cluster2));
    Assertions.assertEquals(1, policyInfo.getRouterPolicyWeightsMap().size());
    Assertions.assertEquals(1, policyInfo.getAmrmPolicyWeightsMap().size());
  }
}
