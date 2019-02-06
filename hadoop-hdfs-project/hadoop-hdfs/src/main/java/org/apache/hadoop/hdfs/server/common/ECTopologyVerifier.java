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
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.namenode.ECTopologyVerifierResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
   * Verifies whether the cluster setup can support the given EC policies.
   *
   * @param report list of data node descriptors for all data nodes
   * @param policies erasure coding policies to verify
   * @return the status of the verification
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final DatanodeInfo[] report, final ErasureCodingPolicy... policies) {
    final int numOfRacks = getNumberOfRacks(report);
    return getECTopologyVerifierResult(numOfRacks, report.length, policies);
  }

  /**
   * Verifies whether the cluster setup can support all enabled EC policies.
   *
   * @param policies erasure coding policies to verify
   * @param numOfRacks number of racks
   * @param numOfDataNodes number of data nodes
   * @return the status of the verification
   */
  public static ECTopologyVerifierResult getECTopologyVerifierResult(
      final int numOfRacks, final int numOfDataNodes,
      final ErasureCodingPolicy... policies) {
    int minDN = 0;
    int minRack = 0;
    for (ErasureCodingPolicy policy: policies) {
      final int policyDN =
          policy.getNumDataUnits() + policy
              .getNumParityUnits();
      minDN = Math.max(minDN, policyDN);
      final int policyRack = (int) Math.ceil(
          policyDN / (double) policy.getNumParityUnits());
      minRack = Math.max(minRack, policyRack);
    }
    if (minDN == 0 || minRack == 0) {
      String resultMessage = "No erasure coding policy is given.";
      LOG.trace(resultMessage);
      return new ECTopologyVerifierResult(true, resultMessage);
    }
    return verifyECWithTopology(minDN, minRack, numOfRacks, numOfDataNodes,
        getReadablePolicies(policies));
  }

  private static ECTopologyVerifierResult verifyECWithTopology(
      final int minDN, final int minRack,
      final int numOfRacks, final int numOfDataNodes, String readablePolicies) {
    String resultMessage;
    if (numOfDataNodes < minDN) {
      resultMessage = String.format("The number of DataNodes (%d) is less " +
          "than the minimum required number of DataNodes (%d) for the " +
          "erasure coding policies: %s", numOfDataNodes, minDN,
          readablePolicies);
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }

    if (numOfRacks < minRack) {
      resultMessage = String.format("The number of racks (%d) is less than " +
          "the minimum required number of racks (%d) for the erasure " +
          "coding policies: %s", numOfRacks, minRack, readablePolicies);
      LOG.debug(resultMessage);
      return new ECTopologyVerifierResult(false, resultMessage);
    }
    return new ECTopologyVerifierResult(true,
        String.format("The cluster setup can support EC policies: %s",
            readablePolicies));
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

  private static String getReadablePolicies(
      final ErasureCodingPolicy... policies) {
    return Arrays.asList(policies)
            .stream()
            .map(policyInfo -> policyInfo.getName())
            .collect(Collectors.joining(", "));
  }
}
