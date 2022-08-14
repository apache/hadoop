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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;
import java.util.Arrays;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class RouterMasterKey {

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterMasterKey newInstance(Integer keyId, ByteBuffer keyBytes, Long expiryDate) {
    RouterMasterKey policy = Records.newRecord(RouterMasterKey.class);
    policy.setKeyId(keyId);
    policy.setKeyBytes(keyBytes);
    policy.setExpiryDate(expiryDate);
    return policy;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
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
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract Integer getKeyId();

  /**
   * Set the keyId of the MasterKey.
   *
   * @param keyId MasterKeyId.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setKeyId(Integer keyId);

  /**
   * Get the keyBytes of the DelegationKey.
   *
   * @return KeyBytes of the DelegationKey.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract ByteBuffer getKeyBytes();

  /**
   * Set the keyBytes of the DelegationKey.
   *
   * @param keyBytes KeyBytes of the DelegationKey.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setKeyBytes(ByteBuffer keyBytes);

  /**
   * Get the ExpiryDate of the DelegationKey.
   *
   * @return ExpiryDate of the DelegationKey.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract Long getExpiryDate();

  /**
   * Set the expiryDate of the DelegationKey.
   *
   * @param expiryDate expiryDate of the DelegationKey.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setExpiryDate(Long expiryDate);

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (getExpiryDate() ^ (getExpiryDate() >>> 32));
    result = prime * result + Arrays.hashCode(getKeyBytes().array());
    result = prime * result + getKeyId();
    return result;
  }

  @Override
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      RouterMasterKey r = (RouterMasterKey) right;
      return getKeyId() == r.getKeyId() && getExpiryDate() == r.getExpiryDate() &&
          Arrays.equals(getKeyBytes().array(), r.getKeyBytes().array());
    }
  }
}
