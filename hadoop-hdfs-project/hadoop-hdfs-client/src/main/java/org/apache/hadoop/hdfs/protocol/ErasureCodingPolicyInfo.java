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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Serializable;

/**
 * HDFS internal presentation of a {@link ErasureCodingPolicy}. Also contains
 * additional information such as {@link ErasureCodingPolicyState}.
 */
@InterfaceAudience.Private
public class ErasureCodingPolicyInfo implements Serializable {

  private static final long serialVersionUID = 0x31;

  private final ErasureCodingPolicy policy;
  private ErasureCodingPolicyState state;

  public ErasureCodingPolicyInfo(final ErasureCodingPolicy thePolicy,
      final ErasureCodingPolicyState theState) {
    Preconditions.checkNotNull(thePolicy);
    Preconditions.checkNotNull(theState);
    policy = thePolicy;
    state = theState;
  }

  public ErasureCodingPolicyInfo(final ErasureCodingPolicy thePolicy) {
    this(thePolicy, ErasureCodingPolicyState.DISABLED);
  }

  public ErasureCodingPolicy getPolicy() {
    return policy;
  }

  public ErasureCodingPolicyState getState() {
    return state;
  }

  public void setState(final ErasureCodingPolicyState newState) {
    Preconditions.checkNotNull(newState, "New state should not be null.");
    state = newState;
  }

  public boolean isEnabled() {
    return (this.state == ErasureCodingPolicyState.ENABLED);
  }

  public boolean isDisabled() {
    return (this.state == ErasureCodingPolicyState.DISABLED);
  }

  public boolean isRemoved() {
    return (this.state == ErasureCodingPolicyState.REMOVED);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o.getClass() != getClass()) {
      return false;
    }
    ErasureCodingPolicyInfo rhs = (ErasureCodingPolicyInfo) o;
    return new EqualsBuilder()
        .append(policy, rhs.policy)
        .append(state, rhs.state)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(303855623, 582626729)
        .append(policy)
        .append(state)
        .toHashCode();
  }

  @Override
  public String toString() {
    return policy.toString() + ", State=" + state.toString();
  }
}