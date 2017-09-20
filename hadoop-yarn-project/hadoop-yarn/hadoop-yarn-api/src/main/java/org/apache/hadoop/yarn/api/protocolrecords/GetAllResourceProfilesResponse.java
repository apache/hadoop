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

import java.util.Map;

/**
 * Response class for getting all the resource profiles from the RM.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class GetAllResourceProfilesResponse {

  public static GetAllResourceProfilesResponse newInstance() {
    return Records.newRecord(GetAllResourceProfilesResponse.class);
  }

  public abstract void setResourceProfiles(Map<String, Resource> profiles);

  public abstract Map<String, Resource> getResourceProfiles();

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || !(other instanceof GetAllResourceProfilesResponse)) {
      return false;
    }
    return ((GetAllResourceProfilesResponse) other).getResourceProfiles()
        .equals(this.getResourceProfiles());
  }

  @Override
  public int hashCode() {
    return this.getResourceProfiles().hashCode();
  }

}
