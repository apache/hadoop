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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Queue weights for representing Federation.
 */
@Private
@Unstable
public abstract class FederationQueueWeight {

  /**
   * The FederationQueueWeight object consists of three parts:
   * routerWeight, amrmWeight, and headRoomAlpha.
   *
   * @param routerWeight Weight for routing applications to different subclusters.
   * We will route the application to different subclusters based on the configured weights.
   * Assuming we have two subclusters, SC-1 and SC-2,
   * with a weight of 0.7 for SC-1 and 0.3 for SC-2,
   * the application will be allocated in such a way
   * that 70% of the applications will be assigned to SC-1 and 30% to SC-2.
   *
   * @param amrmWeight Weight for resource request from ApplicationMaster (AM) to
   * different subclusters' Resource Manager (RM).
   * Assuming we have two subclusters, SC-1 and SC-2,
   * with a weight of 0.6 for SC-1 and 0.4 for SC-2,
   * When AM requesting resources,
   * 60% of the requests will be made to the Resource Manager (RM) of SC-1
   * and 40% to the RM of SC-2.
   *
   * @param headRoomAlpha
   * used by policies that balance weight-based and load-based considerations in their decisions.
   * For policies that use this parameter,
   * values close to 1 indicate that most of the decision
   * should be based on currently observed headroom from various sub-clusters,
   * values close to zero, indicate that the decision should be
   * mostly based on weights and practically ignore current load.
   *
   * @return FederationQueueWeight
   */
  @Private
  @Unstable
  public static FederationQueueWeight newInstance(String routerWeight,
      String amrmWeight, String headRoomAlpha) {
    FederationQueueWeight federationQueueWeight = Records.newRecord(FederationQueueWeight.class);
    federationQueueWeight.setRouterWeight(routerWeight);
    federationQueueWeight.setAmrmWeight(amrmWeight);
    federationQueueWeight.setHeadRoomAlpha(headRoomAlpha);
    return federationQueueWeight;
  }

  @Private
  @Unstable
  public static FederationQueueWeight newInstance(String routerWeight,
      String amrmWeight, String headRoomAlpha, String queue, String policyManagerClassName) {
    FederationQueueWeight federationQueueWeight = Records.newRecord(FederationQueueWeight.class);
    federationQueueWeight.setRouterWeight(routerWeight);
    federationQueueWeight.setAmrmWeight(amrmWeight);
    federationQueueWeight.setHeadRoomAlpha(headRoomAlpha);
    federationQueueWeight.setQueue(queue);
    federationQueueWeight.setPolicyManagerClassName(policyManagerClassName);
    return federationQueueWeight;
  }

  @Public
  @Unstable
  public abstract String getRouterWeight();

  @Public
  @Unstable
  public abstract void setRouterWeight(String routerWeight);

  @Public
  @Unstable
  public abstract String getAmrmWeight();

  @Public
  @Unstable
  public abstract void setAmrmWeight(String amrmWeight);

  @Public
  @Unstable
  public abstract String getHeadRoomAlpha();

  @Public
  @Unstable
  public abstract void setHeadRoomAlpha(String headRoomAlpha);

  private static final String COMMA = ",";
  private static final String COLON = ":";

  /**
   * Check if the subCluster Queue Weight Ratio are valid.
   *
   * This method can be used to validate RouterPolicyWeight and AMRMPolicyWeight.
   *
   * @param subClusterWeight the weight ratios of subClusters.
   * @throws YarnException exceptions from yarn servers.
   */
  public static void checkSubClusterQueueWeightRatioValid(String subClusterWeight)
      throws YarnException {
    // The subClusterWeight cannot be empty.
    if (StringUtils.isBlank(subClusterWeight)) {
      throw new YarnException("subClusterWeight can't be empty!");
    }

    // SC-1:0.7,SC-2:0.3 -> [SC-1:0.7,SC-2:0.3]
    String[] subClusterWeights = subClusterWeight.split(COMMA);
    Map<String, Double> subClusterWeightMap = new LinkedHashMap<>();
    for (String subClusterWeightItem : subClusterWeights) {
      // SC-1:0.7 -> [SC-1,0.7]
      // We require that the parsing result is not empty and must have a length of 2.
      String[] subClusterWeightItems = subClusterWeightItem.split(COLON);
      if (subClusterWeightItems == null || subClusterWeightItems.length != 2) {
        throw new YarnException("The subClusterWeight cannot be empty," +
            " and the subClusterWeight size must be 2. (eg.SC-1,0.2)");
      }
      subClusterWeightMap.put(subClusterWeightItems[0], Double.valueOf(subClusterWeightItems[1]));
    }

    // The sum of weight ratios for subClusters must be equal to 1.
    double sum = subClusterWeightMap.values().stream().mapToDouble(Double::doubleValue).sum();
    boolean isValid = Math.abs(sum - 1.0) < 1e-6; // Comparing with a tolerance of 1e-6

    if (!isValid) {
      throw new YarnException("The sum of ratios for all subClusters must be equal to 1.");
    }
  }

  /**
   * Check if HeadRoomAlpha is a number and is between 0 and 1.
   *
   * @param headRoomAlpha headroomalpha.
   * @throws YarnException exceptions from yarn servers.
   */
  public static void checkHeadRoomAlphaValid(String headRoomAlpha) throws YarnException {
    if (!isNumeric(headRoomAlpha)) {
      throw new YarnException("HeadRoomAlpha must be a number.");
    }

    double dHeadRoomAlpha = Double.parseDouble(headRoomAlpha);
    if (!(dHeadRoomAlpha >= 0 && dHeadRoomAlpha <= 1)) {
      throw new YarnException("HeadRoomAlpha must be between 0-1.");
    }
  }

  /**
   * Determines whether the given value is a number.
   *
   * @param value given value.
   * @return true, is a number, false, not a number.
   */
  protected static boolean isNumeric(String value) {
    return NumberUtils.isCreatable(value);
  }

  @Public
  @Unstable
  public abstract String getQueue();

  @Public
  @Unstable
  public abstract void setQueue(String queue);

  @Public
  @Unstable
  public abstract String getPolicyManagerClassName();

  @Public
  @Unstable
  public abstract void setPolicyManagerClassName(String policyManagerClassName);

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("FederationQueueWeight { ");
    builder.append("Queue: ").append(getQueue()).append(", ");
    builder.append("RouterWeight: ").append(getRouterWeight()).append(", ");
    builder.append("AmrmWeight: ").append(getAmrmWeight()).append(", ");
    builder.append("PolicyManagerClassName: ").append(getPolicyManagerClassName());
    builder.append(" }");
    return builder.toString();
  }
}
