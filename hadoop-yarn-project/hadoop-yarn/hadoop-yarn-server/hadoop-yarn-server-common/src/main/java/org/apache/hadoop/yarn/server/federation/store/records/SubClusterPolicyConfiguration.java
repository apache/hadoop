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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;

// used in javadoc

/**
 * {@link SubClusterPolicyConfiguration} is a class that represents a
 * configuration of a policy. It contains a policy type (resolve to a class
 * name) and its params as an opaque {@link ByteBuffer}.
 *
 * Note: by design the params are an opaque ByteBuffer, this allows for enough
 * flexibility to evolve the policies without impacting the protocols to/from
 * the federation state store.
 */
@Private
@Unstable
public abstract class SubClusterPolicyConfiguration {

  @Private
  @Unstable
  public static SubClusterPolicyConfiguration newInstance(String policyType,
      ByteBuffer policyParams) {
    SubClusterPolicyConfiguration policy =
        Records.newRecord(SubClusterPolicyConfiguration.class);
    policy.setType(policyType);
    policy.setParams(policyParams);
    return policy;
  }

  /**
   * Get the type of the policy. This could be random, round-robin, load-based,
   * etc.
   *
   * @return the type of the policy
   */
  @Public
  @Unstable
  public abstract String getType();

  /**
   * Sets the type of the policy. This could be random, round-robin, load-based,
   * etc.
   *
   * @param policyType the type of the policy
   */
  @Private
  @Unstable
  public abstract void setType(String policyType);

  /**
   * Get the policy parameters. This affects how the policy behaves and an
   * example could be weight distribution of queues across multiple
   * sub-clusters.
   *
   * @return the byte array that contains the parameters
   */
  @Public
  @Unstable
  public abstract ByteBuffer getParams();

  /**
   * Set the policy parameters. This affects how the policy behaves and an
   * example could be weight distribution of queues across multiple
   * sub-clusters.
   *
   * @param policyParams byte array that describes the policy
   */
  @Private
  @Unstable
  public abstract void setParams(ByteBuffer policyParams);

  @Override
  public int hashCode() {
    return 31 * getParams().hashCode() + getType().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SubClusterPolicyConfiguration other = (SubClusterPolicyConfiguration) obj;
    if (!this.getType().equals(other.getType())) {
      return false;
    }
    if (!this.getParams().equals(other.getParams())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType());
    sb.append(" : ");
    sb.append(getParams());
    return sb.toString();
  }
}