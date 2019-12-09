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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code UpdateContainerError} is used by the Scheduler to notify the
 * ApplicationMaster of an UpdateContainerRequest it cannot satisfy due to
 * an error in the request. It includes the update request as well as
 * a reason for why the request was not satisfiable.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class UpdateContainerError {

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static UpdateContainerError newInstance(String reason,
      UpdateContainerRequest updateContainerRequest) {
    UpdateContainerError error = Records.newRecord(UpdateContainerError.class);
    error.setReason(reason);
    error.setUpdateContainerRequest(updateContainerRequest);
    return error;
  }

  /**
   * Get reason why the update request was not satisfiable.
   * @return Reason
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract String getReason();

  /**
   * Set reason why the update request was not satisfiable.
   * @param reason Reason
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setReason(String reason);

  /**
   * Get current container version.
   * @return Current container Version.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract int getCurrentContainerVersion();

  /**
   * Set current container version.
   * @param currentVersion Current container version.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setCurrentContainerVersion(int currentVersion);

  /**
   * Get the {@code UpdateContainerRequest} that was not satisfiable.
   * @return UpdateContainerRequest
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract UpdateContainerRequest getUpdateContainerRequest();

  /**
   * Set the {@code UpdateContainerRequest} that was not satisfiable.
   * @param updateContainerRequest Update Container Request
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setUpdateContainerRequest(
      UpdateContainerRequest updateContainerRequest);

  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    String reason = getReason();
    UpdateContainerRequest updateReq = getUpdateContainerRequest();
    result = prime * result + ((reason == null) ? 0 : reason.hashCode());
    result = prime * result + ((updateReq == null) ? 0 : updateReq.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "UpdateContainerError{reason=" + getReason() + ", "
        + "currentVersion=" + getCurrentContainerVersion() + ", "
        + "req=" + getUpdateContainerRequest() + "}";
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
    UpdateContainerError other = (UpdateContainerError) obj;
    String reason = getReason();
    if (reason == null) {
      if (other.getReason() != null) {
        return false;
      }
    } else if (!reason.equals(other.getReason())) {
      return false;
    }
    UpdateContainerRequest req = getUpdateContainerRequest();
    if (req == null) {
      if (other.getUpdateContainerRequest() != null) {
        return false;
      }
    } else if (!req.equals(other.getUpdateContainerRequest())) {
      return false;
    }
    return getCurrentContainerVersion() == other.getCurrentContainerVersion();
  }
}
