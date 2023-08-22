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

import java.util.Collections;
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
  private static final String START_PARENTHESES = "[";
  private static final String END_PARENTHESES = "]";
  private static final String RESOURCE_DELIMITER = ",";
  private static final String VALUE_DELIMITER = "=";

  private final ResourceVector resource;
  private final Map<String, ResourceUnitCapacityType> capacityTypes
      = new HashMap<>();
  private final Map<ResourceUnitCapacityType, Set<String>> capacityTypePerResource
      = new HashMap<>();

  public QueueCapacityVector() {
    this.resource = new ResourceVector();
  }

  private QueueCapacityVector(ResourceVector resource) {
    this.resource = resource;
  }

  /**
   * Creates a zero {@code QueueCapacityVector}. The resources are defined
   * in absolute capacity type by default.
   *
   * @return zero capacity vector
   */
  public static QueueCapacityVector newInstance() {
    QueueCapacityVector newCapacityVector =
        new QueueCapacityVector(ResourceVector.newInstance());
    for (Map.Entry<String, Double> resourceEntry : newCapacityVector.resource) {
      newCapacityVector.storeResourceType(resourceEntry.getKey(),
          ResourceUnitCapacityType.ABSOLUTE);
    }

    return newCapacityVector;
  }

  /**
   * Creates a uniform and homogeneous {@code QueueCapacityVector}.
   * The resources are defined in absolute capacity type by default.
   *
   * @param value value to be set for each resource
   * @param capacityType capacity type to be set for each resource
   * @return uniform capacity vector
   */
  public static QueueCapacityVector of(
      double value, ResourceUnitCapacityType capacityType) {
    QueueCapacityVector newCapacityVector =
        new QueueCapacityVector(ResourceVector.of(value));
    for (Map.Entry<String, Double> resourceEntry : newCapacityVector.resource) {
      newCapacityVector.storeResourceType(resourceEntry.getKey(), capacityType);
    }

    return newCapacityVector;
  }

  public QueueCapacityVectorEntry getResource(String resourceName) {
    return new QueueCapacityVectorEntry(capacityTypes.get(resourceName),
        resourceName, resource.getValue(resourceName));
  }

  /**
   * Returns the number of resources defined for this vector.
   *
   * @return number of resources
   */
  public int getResourceCount() {
    return capacityTypes.size();
  }

  /**
   * Set the value and capacity type of a resource.
   *
   * @param resourceName name of the resource
   * @param value        value of the resource
   * @param capacityType type of the resource
   */
  public void setResource(String resourceName, double value,
                          ResourceUnitCapacityType capacityType) {
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
   *
   * @return value of memory resource
   */
  public double getMemory() {
    return resource.getValue(ResourceInformation.MEMORY_URI);
  }

  public boolean isEmpty() {
    return resource.isEmpty() && capacityTypePerResource.isEmpty() && capacityTypes.isEmpty();
  }

  /**
   * Returns the name of all resources that are defined in the given capacity
   * type.
   *
   * @param capacityType the capacity type of the resources
   * @return all resource names for the given capacity type
   */
  public Set<String> getResourceNamesByCapacityType(
      ResourceUnitCapacityType capacityType) {
    return new HashSet<>(capacityTypePerResource.getOrDefault(capacityType,
        Collections.emptySet()));
  }

  /**
   * Checks whether a resource unit is defined as a specific type.
   *
   * @param resourceName resource unit name
   * @param capacityType capacity type
   * @return true, if resource unit is defined as the given type
   */
  public boolean isResourceOfType(
      String resourceName, ResourceUnitCapacityType capacityType) {
    return capacityTypes.containsKey(resourceName) &&
        capacityTypes.get(resourceName).equals(capacityType);
  }

  @Override
  public Iterator<QueueCapacityVectorEntry> iterator() {
    return new Iterator<QueueCapacityVectorEntry>() {
      private final Iterator<Map.Entry<String, Double>> resources =
          resource.iterator();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return resources.hasNext() && capacityTypes.size() > i;
      }

      @Override
      public QueueCapacityVectorEntry next() {
        Map.Entry<String, Double> resourceInformation = resources.next();
        i++;
        return new QueueCapacityVectorEntry(
            capacityTypes.get(resourceInformation.getKey()),
            resourceInformation.getKey(), resourceInformation.getValue());
      }
    };
  }

  /**
   * Returns a set of all capacity types defined for this vector.
   *
   * @return capacity types
   */
  public Set<ResourceUnitCapacityType> getDefinedCapacityTypes() {
    return capacityTypePerResource.keySet();
  }

  /**
   * Checks whether the vector is a mixed capacity vector (more than one capacity type is used,
   * therefore it is not uniform).
   * @return true, if the vector is mixed
   */
  public boolean isMixedCapacityVector() {
    return getDefinedCapacityTypes().size() > 1;
  }

  public Set<String> getResourceNames() {
    return resource.getResourceNames();
  }

  private void storeResourceType(
      String resourceName, ResourceUnitCapacityType resourceType) {
    if (capacityTypes.get(resourceName) != null
        && !capacityTypes.get(resourceName).equals(resourceType)) {
      capacityTypePerResource.get(capacityTypes.get(resourceName))
          .remove(resourceName);
      if (capacityTypePerResource.get(capacityTypes.get(resourceName)).isEmpty()) {
        capacityTypePerResource.remove(capacityTypes.get(resourceName));
      }
    }

    capacityTypePerResource.putIfAbsent(resourceType, new HashSet<>());
    capacityTypePerResource.get(resourceType).add(resourceName);
    capacityTypes.put(resourceName, resourceType);
  }

  @Override
  public String toString() {
    StringBuilder stringVector = new StringBuilder();
    stringVector.append(START_PARENTHESES);

    int resourceCount = 0;
    for (Map.Entry<String, Double> resourceEntry : resource) {
      resourceCount++;
      stringVector.append(resourceEntry.getKey())
          .append(VALUE_DELIMITER)
          .append(resourceEntry.getValue())
          .append(capacityTypes.get(resourceEntry.getKey()).postfix);
      if (resourceCount < capacityTypes.size()) {
        stringVector.append(RESOURCE_DELIMITER);
      }
    }

    stringVector.append(END_PARENTHESES);

    return stringVector.toString();
  }

  /**
   * Represents a capacity type associated with its syntax postfix.
   */
  public enum ResourceUnitCapacityType {
    PERCENTAGE("%"), ABSOLUTE(""), WEIGHT("w");
    private final String postfix;

    ResourceUnitCapacityType(String postfix) {
      this.postfix = postfix;
    }

    public String getPostfix() {
      return postfix;
    }
  }

  public static class QueueCapacityVectorEntry {
    private final ResourceUnitCapacityType vectorResourceType;
    private final double resourceValue;
    private final String resourceName;

    public QueueCapacityVectorEntry(ResourceUnitCapacityType vectorResourceType,
                                    String resourceName, double resourceValue) {
      this.vectorResourceType = vectorResourceType;
      this.resourceValue = resourceValue;
      this.resourceName = resourceName;
    }

    public ResourceUnitCapacityType getVectorResourceType() {
      return vectorResourceType;
    }

    public double getResourceValue() {
      return resourceValue;
    }

    public String getResourceName() {
      return resourceName;
    }
  }
}
