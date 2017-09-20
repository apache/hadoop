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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * Response class for getting the details for a particular resource profile.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class GetResourceProfileResponse {

  public static GetResourceProfileResponse newInstance() {
    return Records.newRecord(GetResourceProfileResponse.class);
  }

  /**
   * Get the resources that will be allocated if the profile was used.
   *
   * @return the resources that will be allocated if the profile was used.
   */
  public abstract Resource getResource();

  /**
   * Set the resources that will be allocated if the profile is used.
   *
   * @param r Set the resources that will be allocated if the profile is used.
   */
  public abstract void setResource(Resource r);

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || !(other instanceof GetResourceProfileResponse)) {
      return false;
    }
    return this.getResource()
        .equals(((GetResourceProfileResponse) other).getResource());
  }

  @Override
  public int hashCode() {
    return getResource().hashCode();
  }
}
