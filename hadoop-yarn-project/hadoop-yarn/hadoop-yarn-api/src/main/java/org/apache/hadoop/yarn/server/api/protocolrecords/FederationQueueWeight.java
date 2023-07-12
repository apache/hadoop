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
   * @return If the validation is successful, return true. Otherwise, an exception will be thrown.
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
}
