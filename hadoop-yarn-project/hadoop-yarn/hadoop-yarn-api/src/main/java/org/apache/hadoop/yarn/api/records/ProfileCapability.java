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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Class to capture capability requirements when using resource profiles. The
 * ProfileCapability is meant to be used as part of the ResourceRequest. A
 * profile capability has two pieces - the resource profile name and the
 * overrides. The resource profile specifies the name of the resource profile
 * to be used and the capability override is the overrides desired on specific
 * resource types.
 *
 * For example, if you have a resource profile "small" that maps to
 * {@literal <4096M, 2 cores, 1 gpu>} and you set the capability override to
 * {@literal <8192M, 0 cores, 0 gpu>}, then the actual resource allocation on
 * the ResourceManager will be {@literal <8192M, 2 cores, 1 gpu>}.
 *
 * Note that the conversion from the ProfileCapability to the Resource class
 * with the actual resource requirements will be done by the ResourceManager,
 * which has the actual profile to Resource mapping.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class ProfileCapability {

  public static final String DEFAULT_PROFILE = "default";

  public static ProfileCapability newInstance(Resource override) {
    return newInstance(DEFAULT_PROFILE, override);
  }

  public static ProfileCapability newInstance(String profile) {
    Preconditions
        .checkArgument(profile != null, "The profile name cannot be null");
    ProfileCapability obj = Records.newRecord(ProfileCapability.class);
    obj.setProfileName(profile);
    obj.setProfileCapabilityOverride(Resource.newInstance(0, 0));
    return obj;
  }

  public static ProfileCapability newInstance(String profile,
      Resource override) {
    Preconditions
        .checkArgument(profile != null, "The profile name cannot be null");
    ProfileCapability obj = Records.newRecord(ProfileCapability.class);
    obj.setProfileName(profile);
    obj.setProfileCapabilityOverride(override);
    return obj;
  }

  /**
   * Get the profile name.
   * @return the profile name
   */
  public abstract String getProfileName();

  /**
   * Get the profile capability override.
   * @return Resource object containing the override.
   */
  public abstract Resource getProfileCapabilityOverride();

  /**
   * Set the resource profile name.
   * @param profileName the resource profile name
   */
  public abstract void setProfileName(String profileName);

  /**
   * Set the capability override to override specific resource types on the
   * resource profile.
   *
   * For example, if you have a resource profile "small" that maps to
   * {@literal <4096M, 2 cores, 1 gpu>} and you set the capability override to
   * {@literal <8192M, 0 cores, 0 gpu>}, then the actual resource allocation on
   * the ResourceManager will be {@literal <8192M, 2 cores, 1 gpu>}.
   *
   * Note that the conversion from the ProfileCapability to the Resource class
   * with the actual resource requirements will be done by the ResourceManager,
   * which has the actual profile to Resource mapping.
   *
   * @param r Resource object containing the capability override
   */
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

  /**
   * Get a representation of the capability as a Resource object.
   * @param capability the capability we wish to convert
   * @param resourceProfilesMap map of profile name to Resource object
   * @return Resource object representing the capability
   */
  public static Resource toResource(ProfileCapability capability,
      Map<String, Resource> resourceProfilesMap) {
    Preconditions
        .checkArgument(capability != null, "Capability cannot be null");
    Preconditions.checkArgument(resourceProfilesMap != null,
        "Resource profiles map cannot be null");
    Resource none = Resource.newInstance(0, 0);
    Resource resource = Resource.newInstance(0, 0);
    String profileName = capability.getProfileName();
    if (null == profileName || profileName.isEmpty()) {
      profileName = DEFAULT_PROFILE;
    }
    if (resourceProfilesMap.containsKey(profileName)) {
      resource = Resource.newInstance(resourceProfilesMap.get(profileName));
    }
    if (capability.getProfileCapabilityOverride() != null &&
        !capability.getProfileCapabilityOverride().equals(none)) {
      for (ResourceInformation entry : capability
          .getProfileCapabilityOverride().getResources()) {
        if (entry != null && entry.getValue() > 0) {
          resource.setResourceInformation(entry.getName(), entry);
        }
      }
    }
    return resource;
  }
}
