/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.server.namenode.ECTopologyVerifierResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for verifying whether the cluster setup can support
 * all enabled EC policies.
 *
 * Scenarios when the verification fails:
 * 1. not enough data nodes compared to EC policy's highest data+parity number
 * 2. not enough racks to satisfy BlockPlacementPolicyRackFaultTolerant
 */
@InterfaceAudience.Private
public final class ECTopologyVerifier {

  public static final Logger LOG =
      LoggerFactory.getLogger(ECTopologyVerifier.class);

  private ECTopologyVerifier() {}

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   *
   * @param report list of data node descriptors for all data nodes
   * @param policies all system and user defined erasure coding policies
   * @return the status of the verification
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final DatanodeInfo[] report, final ErasureCodingPolicyInfo[] policies) {
    final int numOfRacks = getNumberOfRacks(report);
    return getECTopologyVerifierResult(policies, numOfRacks, report.length);
  }

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   *
   * @param policies all system and user defined erasure coding policies
   * @param numOfRacks number of racks
   * @param numOfDataNodes number of data nodes
   * @return the status of the verification
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final ErasureCodingPolicyInfo[] policies, final int numOfRacks,
      final int numOfDataNodes) {
    int minDN = 0;
    int minRack = 0;
    for (ErasureCodingPolicyInfo policy: policies) {
      if (policy.isEnabled()) {
        final int policyDN =
            policy.getPolicy().getNumDataUnits() + policy.getPolicy()
                .getNumParityUnits();
        minDN = Math.max(minDN, policyDN);
        final int policyRack = (int) Math.ceil(
            policyDN / (double) policy.getPolicy().getNumParityUnits());
        minRack = Math.max(minRack, policyRack);
      }
    }
    if (minDN == 0 || minRack == 0) {
      String resultMessage = "No erasure coding policy is enabled.";
      LOG.trace(resultMessage);
      return new ECTopologyVerifierResult(true, resultMessage);
    }
    return verifyECWithTopology(minDN, minRack, numOfRacks, numOfDataNodes);
  }

  private static ECTopologyVerifierResult verifyECWithTopology(
      final int minDN, final int minRack,
      final int numOfRacks, final int numOfDataNodes) {
    String resultMessage;
    if (numOfDataNodes < minDN) {
      resultMessage = "The number of DataNodes (" + numOfDataNodes
          + ") is less than the minimum required number of DataNodes ("
          + minDN + ") for enabled erasure coding policy.";
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }

    if (numOfRacks < minRack) {
      resultMessage = "The number of racks (" + numOfRacks
          + ") is less than the minimum required number of racks ("
          + minRack + ") for enabled erasure coding policy.";
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }
    return new ECTopologyVerifierResult(true,
        "The cluster setup can support all enabled EC policies");
  }

  private static int getNumberOfRacks(DatanodeInfo[] report) {
    final Map<String, Integer> racks = new HashMap<>();
    for (DatanodeInfo dni : report) {
      Integer count = racks.get(dni.getNetworkLocation());
      if (count == null) {
        count = 0;
      }
      racks.put(dni.getNetworkLocation(), count + 1);
    }
    return racks.size();
  }
}
