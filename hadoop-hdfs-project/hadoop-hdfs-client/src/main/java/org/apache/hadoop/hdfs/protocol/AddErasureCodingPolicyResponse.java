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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.HadoopIllegalArgumentException;

/**
 * A response of add an ErasureCoding policy.
 */
public class AddErasureCodingPolicyResponse {
  private boolean succeed;
  private ErasureCodingPolicy policy;
  private String errorMsg;

  public AddErasureCodingPolicyResponse(ErasureCodingPolicy policy) {
    this.policy = policy;
    this.succeed = true;
  }

  public AddErasureCodingPolicyResponse(ErasureCodingPolicy policy,
                                        String errorMsg) {
    this.policy = policy;
    this.errorMsg = errorMsg;
    this.succeed = false;
  }

  public AddErasureCodingPolicyResponse(ErasureCodingPolicy policy,
                                        HadoopIllegalArgumentException e) {
    this(policy, e.getMessage());
  }

  public boolean isSucceed() {
    return succeed;
  }

  public ErasureCodingPolicy getPolicy() {
    return policy;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  @Override
  public String toString() {
    if (isSucceed()) {
      return "Add ErasureCodingPolicy " + getPolicy().getName() + " succeed.";
    } else {
      return "Add ErasureCodingPolicy " + getPolicy().getName() + " failed and "
          + "error message is " + getErrorMsg();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AddErasureCodingPolicyResponse) {
      AddErasureCodingPolicyResponse other = (AddErasureCodingPolicyResponse) o;
      return new EqualsBuilder()
          .append(policy, other.policy)
          .append(succeed, other.succeed)
          .append(errorMsg, other.errorMsg)
          .isEquals();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(303855623, 582626729)
        .append(policy)
        .append(succeed)
        .append(errorMsg)
        .toHashCode();
  }
}
