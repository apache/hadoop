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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;

@Private
@Unstable
public abstract class RouterMasterKey {

  @Private
  @Unstable
  public static RouterMasterKey newInstance(Integer keyId, ByteBuffer keyBytes, Long expiryDate) {
    RouterMasterKey policy = Records.newRecord(RouterMasterKey.class);
    policy.setKeyId(keyId);
    policy.setKeyBytes(keyBytes);
    policy.setExpiryDate(expiryDate);
    return policy;
  }

  @Private
  @Unstable
  public static RouterMasterKey newInstance(RouterMasterKey masterKey) {
    RouterMasterKey routerMasterKey = Records.newRecord(RouterMasterKey.class);
    routerMasterKey.setKeyId(masterKey.getKeyId());
    routerMasterKey.setKeyBytes(masterKey.getKeyBytes());
    routerMasterKey.setExpiryDate(masterKey.getExpiryDate());
    return routerMasterKey;
  }

  /**
   * Get the keyId of the MasterKey.
   *
   * @return MasterKeyId.
   */
  @Public
  @Unstable
  public abstract Integer getKeyId();

  /**
   * Set the keyId of the MasterKey.
   *
   * @param keyId MasterKeyId.
   */
  @Private
  @Unstable
  public abstract void setKeyId(Integer keyId);

  /**
   * Get the keyBytes of the DelegationKey.
   *
   * @return KeyBytes of the DelegationKey.
   */
  @Public
  @Unstable
  public abstract ByteBuffer getKeyBytes();

  /**
   * Set the keyBytes of the DelegationKey.
   *
   * @param keyBytes KeyBytes of the DelegationKey.
   */
  @Private
  @Unstable
  public abstract void setKeyBytes(ByteBuffer keyBytes);

  /**
   * Get the ExpiryDate of the DelegationKey.
   *
   * @return ExpiryDate of the DelegationKey.
   */
  @Private
  @Unstable
  public abstract Long getExpiryDate();

  /**
   * Set the expiryDate of the DelegationKey.
   *
   * @param expiryDate expiryDate of the DelegationKey.
   */
  @Private
  @Unstable
  public abstract void setExpiryDate(Long expiryDate);

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(this.getExpiryDate().longValue())
        .append(this.getKeyId().intValue())
        .append(getKeyBytes().array())
        .hashCode();
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (obj instanceof RouterMasterKey) {
      RouterMasterKey other = (RouterMasterKey) obj;
      return new EqualsBuilder()
          .append(this.getKeyId().intValue(), other.getKeyId().intValue())
          .append(this.getExpiryDate().longValue(), other.getExpiryDate().longValue())
          .append(this.getKeyBytes().array(), other.getKeyBytes())
          .isEquals();
    }

    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RouterMasterKey: [")
        .append("KeyId: ").append(getKeyId()).append(", ")
        .append("ExpiryDate: ").append(getExpiryDate()).append(", ")
        .append("KeyBytes: ").append(getKeyBytes()).append(", ")
        .append("]");
    return sb.toString();
  }
}
