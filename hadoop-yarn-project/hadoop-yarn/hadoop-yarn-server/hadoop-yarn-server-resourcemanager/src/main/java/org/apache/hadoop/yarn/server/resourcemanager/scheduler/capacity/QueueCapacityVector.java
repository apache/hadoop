/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.ResourceInformation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Contains capacity values with calculation types associated for each
 * resource.
 */
public class QueueCapacityVector implements
    Iterable<QueueCapacityVector.QueueCapacityVectorEntry> {
  private final ResourceVector resource;
  private final Map<String, QueueCapacityType> capacityTypes
      = new HashMap<>();
  private final Map<QueueCapacityType, Set<String>> capacityTypePerResource
      = new HashMap<>();

  public QueueCapacityVector(ResourceVector resource) {
    this.resource = resource;
  }

  /**
   * Creates a new empty {@code QueueCapacityVector}.
   * @return empty capacity vector
   */
  public static QueueCapacityVector newInstance() {
    return new QueueCapacityVector(new ResourceVector());
  }

  public QueueCapacityVectorEntry getResource(String resourceName) {
    return new QueueCapacityVectorEntry(capacityTypes.get(resourceName),
        resourceName, resource.getValue(resourceName));
  }

  /**
   * Returns the number of resources defined for this vector.
   * @return number of resources
   */
  public int getResourceCount() {
    return capacityTypes.size();
  }

  /**
   * Set the value and capacity type of a resource.
   * @param resourceName name of the resource
   * @param value value of the resource
   * @param capacityType type of the resource
   */
  public void setResource(String resourceName, float value,
                          QueueCapacityType capacityType) {
    // Necessary due to backward compatibility (memory = memory-mb)
    String convertedResourceName = resourceName;
    if (resourceName.equals("memory")) {
      convertedResourceName = ResourceInformation.MEMORY_URI;
    }
    resource.setValue(convertedResourceName, value);
    storeResourceType(convertedResourceName, capacityType);
  }

  /**
   * A shorthand to retrieve the value stored for the memory resource.
   * @return value of memory resource
   */
  public float getMemory() {
    return resource.getValue(ResourceInformation.MEMORY_URI);
  }

  /**
   * Returns the name of all resources of
   * @param resourceType
   * @return
   */
  public Set<String> getResourceNamesByCapacityType(
      QueueCapacityType resourceType) {
    return capacityTypePerResource.get(resourceType);
  }

  @Override
  public Iterator<QueueCapacityVectorEntry> iterator() {
    return new Iterator<QueueCapacityVectorEntry>() {
      private final Iterator<Map.Entry<String, Float>> resources =
          resource.iterator();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return resources.hasNext() && capacityTypes.size() > i;
      }

      @Override
      public QueueCapacityVectorEntry next() {
        Map.Entry<String, Float> resourceInformation = resources.next();
        i++;
        return new QueueCapacityVectorEntry(
            capacityTypes.get(resourceInformation.getKey()),
            resourceInformation.getKey(), resourceInformation.getValue());
      }
    };
  }

  /**
   * Returns a set of all capacity type defined for this vector.
   * @return capacity types
   */
  public Set<QueueCapacityType> getDefinedCapacityTypes() {
    return capacityTypePerResource.keySet();
  }

  private void storeResourceType(
      String resourceName, QueueCapacityType resourceType) {
    if (!capacityTypePerResource.containsKey(resourceType)) {
      capacityTypePerResource.put(resourceType, new HashSet<>());
    }
    capacityTypePerResource.get(resourceType).add(resourceName);
    capacityTypes.put(resourceName, resourceType);
  }

  /**
   * Represents a capacity type associated with its syntax postfix.
   */
  public enum QueueCapacityType {
    PERCENTAGE("%"), ABSOLUTE(""), WEIGHT("w");
    private final String postfix;

    QueueCapacityType(String postfix) {
      this.postfix = postfix;
    }

    public String getPostfix() {
      return postfix;
    }
  }

  public static class QueueCapacityVectorEntry {
    private final QueueCapacityType vectorResourceType;
    private final float resourceValue;
    private final String resourceName;

    public QueueCapacityVectorEntry(QueueCapacityType vectorResourceType,
                                    String resourceName, float resourceValue) {
      this.vectorResourceType = vectorResourceType;
      this.resourceValue = resourceValue;
      this.resourceName = resourceName;
    }

    public QueueCapacityType getVectorResourceType() {
      return vectorResourceType;
    }

    public float getResourceValue() {
      return resourceValue;
    }

    public String getResourceName() {
      return resourceName;
    }
  }
}
