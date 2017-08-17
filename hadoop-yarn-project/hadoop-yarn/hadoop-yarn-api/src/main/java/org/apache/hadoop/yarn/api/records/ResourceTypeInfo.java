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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.util.Records;

/**
 * Class to encapsulate information about a ResourceType - the name of the
 * resource, the units(milli, micro, etc), the type(countable).
 */
public abstract class ResourceTypeInfo implements Comparable<ResourceTypeInfo> {

  /**
   * Get the name for the resource.
   *
   * @return resource name
   */
  public abstract String getName();

  /**
   * Set the name for the resource.
   *
   * @param rName
   *          name for the resource
   */
  public abstract void setName(String rName);

  /**
   * Get units for the resource.
   *
   * @return units for the resource
   */
  public abstract String getDefaultUnit();

  /**
   * Set the units for the resource.
   *
   * @param rUnits
   *          units for the resource
   */
  public abstract void setDefaultUnit(String rUnits);

  /**
   * Get the resource type.
   *
   * @return the resource type
   */
  public abstract ResourceTypes getResourceType();

  /**
   * Set the resource type.
   *
   * @param type
   *          the resource type
   */
  public abstract void setResourceType(ResourceTypes type);

  /**
   * Create a new instance of ResourceTypeInfo from another object.
   *
   * @param other
   *          the object from which the new object should be created
   * @return the new ResourceTypeInfo object
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static ResourceTypeInfo newInstance(ResourceTypeInfo other) {
    ResourceTypeInfo resourceType = Records.newRecord(ResourceTypeInfo.class);
    copy(other, resourceType);
    return resourceType;
  }

  /**
   * Create a new instance of ResourceTypeInfo from name, units and type.
   *
   * @param name name of resource type
   * @param units units of resource type
   * @param type such as countable, etc.
   * @return the new ResourceTypeInfo object
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static ResourceTypeInfo newInstance(String name, String units,
      ResourceTypes type) {
    ResourceTypeInfo resourceType = Records.newRecord(ResourceTypeInfo.class);
    resourceType.setName(name);
    resourceType.setResourceType(type);
    resourceType.setDefaultUnit(units);
    return resourceType;
  }

  /**
   * Create a new instance of ResourceTypeInfo from name, units
   *
   * @param name name of resource type
   * @param units units of resource type
   * @return the new ResourceTypeInfo object
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static ResourceTypeInfo newInstance(String name, String units) {
    return ResourceTypeInfo.newInstance(name, units, ResourceTypes.COUNTABLE);
  }

  /**
   * Create a new instance of ResourceTypeInfo from name
   *
   * @param name name of resource type
   * @return the new ResourceTypeInfo object
   */
  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static ResourceTypeInfo newInstance(String name) {
    return ResourceTypeInfo.newInstance(name, "");
  }

  /**
   * Copies the content of the source ResourceTypeInfo object to the
   * destination object, overwriting all properties of the destination object.
   *
   * @param src
   *          Source ResourceTypeInfo object
   * @param dst
   *          Destination ResourceTypeInfo object
   */

  public static void copy(ResourceTypeInfo src, ResourceTypeInfo dst) {
    dst.setName(src.getName());
    dst.setResourceType(src.getResourceType());
    dst.setDefaultUnit(src.getDefaultUnit());
  }

  @Override
  public String toString() {
    return "name: " + this.getName() + ", units: " + this.getDefaultUnit()
        + ", type: " + getResourceType();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof ResourceTypeInfo)) {
      return false;
    }
    ResourceTypeInfo r = (ResourceTypeInfo) obj;
    return this.getName().equals(r.getName())
        && this.getResourceType().equals(r.getResourceType())
        && this.getDefaultUnit().equals(r.getDefaultUnit());
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    int result = prime + getName().hashCode();
    result = prime * result + getResourceType().hashCode();
    return result;
  }

  @Override
  public int compareTo(ResourceTypeInfo other) {
    int diff = this.getName().compareTo(other.getName());
    if (diff == 0) {
      diff = this.getDefaultUnit().compareTo(other.getDefaultUnit());
      if (diff == 0) {
        diff = this.getResourceType().compareTo(other.getResourceType());
      }
    }
    return diff;
  }
}
