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
 * Class to capture capability requirements when using resource profiles. The
 * ProfileCapability is meant to be used as part of the ResourceRequest. A
 * profile capability has two pieces - the resource profile name and the
 * overrides. The resource profile specifies the name of the resource profile
 * to be used and the capability override is the overrides desired on specific
 * resource types. For example, you could use the "minimum" profile and set the
 * memory in the capability override to 4096M. This implies that you wish for
 * the resources specified in the "minimum" profile but with 4096M memory. The
 * conversion from the ProfileCapability to the Resource class with the actual
 * resource requirements will be done by the ResourceManager, which has the
 * actual profile to Resource mapping.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class ProfileCapability {

  public static ProfileCapability newInstance(String profile,
      Resource override) {
    ProfileCapability obj = Records.newRecord(ProfileCapability.class);
    obj.setProfileName(profile);
    obj.setProfileCapabilityOverride(override);
    return obj;
  }

  public abstract String getProfileName();

  public abstract Resource getProfileCapabilityOverride();

  public abstract void setProfileName(String profileName);

  public abstract void setProfileCapabilityOverride(Resource r);

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || !(other instanceof ProfileCapability)) {
      return false;
    }
    return ((ProfileCapability) other).getProfileName()
        .equals(this.getProfileName()) && ((ProfileCapability) other)
        .getProfileCapabilityOverride()
        .equals(this.getProfileCapabilityOverride());
  }

  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    String name = getProfileName();
    Resource override = getProfileCapabilityOverride();
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((override == null) ? 0 : override.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "{ profile: " + this.getProfileName() + ", capabilityOverride: "
        + this.getProfileCapabilityOverride() + " }";
  }
}
