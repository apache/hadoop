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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

/**
 * This is a DAO class for the configuration of parameters for federation
 * policies. This generalizes several possible configurations as two lists of
 * {@link SubClusterIdInfo} and corresponding weights as a {@link Float}. The
 * interpretation of the weight is left to the logic in the policy.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
@XmlRootElement(name = "federation-policy")
@XmlAccessorType(XmlAccessType.FIELD)
public class WeightedPolicyInfo {

  private static final ObjectWriter writer = new ObjectMapper()
      .writerFor(WeightedPolicyInfo.class).withDefaultPrettyPrinter();
  private static final ObjectReader reader =
      new ObjectMapper().readerFor(WeightedPolicyInfo.class);
  private Map<SubClusterIdInfo, Float> routerPolicyWeights = new HashMap<>();
  private Map<SubClusterIdInfo, Float> amrmPolicyWeights = new HashMap<>();
  private float headroomAlpha;

  public WeightedPolicyInfo() {
    // Jackson requires this
  }

  /**
   * Deserializes a {@link WeightedPolicyInfo} from a byte UTF-8 JSON
   * representation.
   *
   * @param bb the input byte representation.
   *
   * @return the {@link WeightedPolicyInfo} represented.
   *
   * @throws FederationPolicyInitializationException if a deserializaiton error
   *           occurs.
   */
  public static WeightedPolicyInfo fromByteBuffer(ByteBuffer bb)
      throws FederationPolicyInitializationException {

    if (reader == null || writer == null) {
      throw new FederationPolicyInitializationException(
          "ObjectWriter and ObjectReader are not initialized.");
    }

    try {
      final byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      String params = new String(bytes, StandardCharsets.UTF_8);
      return reader.readValue(params);
    } catch (IOException e) {
      throw new FederationPolicyInitializationException(e);
    }
  }

  /**
   * Getter of the router weights.
   *
   * @return the router weights.
   */
  public Map<SubClusterIdInfo, Float> getRouterPolicyWeights() {
    return routerPolicyWeights;
  }

  /**
   * Setter method for Router weights.
   *
   * @param policyWeights the router weights.
   */
  public void setRouterPolicyWeights(
      Map<SubClusterIdInfo, Float> policyWeights) {
    this.routerPolicyWeights = policyWeights;
  }

  /**
   * Getter for AMRMProxy weights.
   *
   * @return the AMRMProxy weights.
   */
  public Map<SubClusterIdInfo, Float> getAMRMPolicyWeights() {
    return amrmPolicyWeights;
  }

  /**
   * Setter method for ARMRMProxy weights.
   *
   * @param policyWeights the amrmproxy weights.
   */
  public void setAMRMPolicyWeights(Map<SubClusterIdInfo, Float> policyWeights) {
    this.amrmPolicyWeights = policyWeights;
  }

  /**
   * Converts the policy into a byte array representation in the input
   * {@link ByteBuffer}.
   *
   * @return byte array representation of this policy configuration.
   *
   * @throws FederationPolicyInitializationException if a serialization error
   *           occurs.
   */
  public ByteBuffer toByteBuffer()
      throws FederationPolicyInitializationException {
    if (reader == null || writer == null) {
      throw new FederationPolicyInitializationException(
          "ObjectWriter and ObjectReader are not initialized.");
    }
    try {
      String s = toJSONString();
      return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    } catch (JsonProcessingException e) {
      throw new FederationPolicyInitializationException(e);
    }
  }

  private String toJSONString() throws JsonProcessingException {
    return writer.writeValueAsString(this);
  }

  @Override
  public boolean equals(Object other) {

    if (other == null || !other.getClass().equals(this.getClass())) {
      return false;
    }

    WeightedPolicyInfo otherPolicy = (WeightedPolicyInfo) other;
    Map<SubClusterIdInfo, Float> otherAMRMWeights =
        otherPolicy.getAMRMPolicyWeights();
    Map<SubClusterIdInfo, Float> otherRouterWeights =
        otherPolicy.getRouterPolicyWeights();

    boolean amrmWeightsMatch =
        otherAMRMWeights != null && getAMRMPolicyWeights() != null
            && CollectionUtils.isEqualCollection(otherAMRMWeights.entrySet(),
                getAMRMPolicyWeights().entrySet());

    boolean routerWeightsMatch =
        otherRouterWeights != null && getRouterPolicyWeights() != null
            && CollectionUtils.isEqualCollection(otherRouterWeights.entrySet(),
                getRouterPolicyWeights().entrySet());

    return amrmWeightsMatch && routerWeightsMatch;
  }

  @Override
  public int hashCode() {
    return 31 * amrmPolicyWeights.hashCode() + routerPolicyWeights.hashCode();
  }

  /**
   * Return the parameter headroomAlpha, used by policies that balance
   * weight-based and load-based considerations in their decisions.
   *
   * For policies that use this parameter, values close to 1 indicate that most
   * of the decision should be based on currently observed headroom from various
   * sub-clusters, values close to zero, indicate that the decision should be
   * mostly based on weights and practically ignore current load.
   *
   * @return the value of headroomAlpha.
   */
  public float getHeadroomAlpha() {
    return headroomAlpha;
  }

  /**
   * Set the parameter headroomAlpha, used by policies that balance weight-based
   * and load-based considerations in their decisions.
   *
   * For policies that use this parameter, values close to 1 indicate that most
   * of the decision should be based on currently observed headroom from various
   * sub-clusters, values close to zero, indicate that the decision should be
   * mostly based on weights and practically ignore current load.
   *
   * @param headroomAlpha the value to use for balancing.
   */
  public void setHeadroomAlpha(float headroomAlpha) {
    this.headroomAlpha = headroomAlpha;
  }

  @Override
  public String toString() {
    try {
      return toJSONString();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return "Error serializing to string.";
    }
  }
}
