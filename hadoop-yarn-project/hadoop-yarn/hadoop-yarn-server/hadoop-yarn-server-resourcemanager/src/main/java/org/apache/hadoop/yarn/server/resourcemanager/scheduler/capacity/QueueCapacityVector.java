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
  private final Map<String, QueueCapacityType> capacityTypes
      = new HashMap<>();
  private final Map<QueueCapacityType, Set<String>> capacityTypePerResource
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
    for (Map.Entry<String, Float> resourceEntry : newCapacityVector.resource) {
      newCapacityVector.storeResourceType(resourceEntry.getKey(),
          QueueCapacityType.ABSOLUTE);
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
      float value, QueueCapacityType capacityType) {
    QueueCapacityVector newCapacityVector =
        new QueueCapacityVector(ResourceVector.of(value));
    for (Map.Entry<String, Float> resourceEntry : newCapacityVector.resource) {
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
   *
   * @return value of memory resource
   */
  public float getMemory() {
    return resource.getValue(ResourceInformation.MEMORY_URI);
  }

  /**
   * Returns the name of all resources that are defined in the given capacity
   * type.
   *
   * @param capacityType the capacity type of the resources
   * @return all resource names for the given capacity type
   */
  public Set<String> getResourceNamesByCapacityType(
      QueueCapacityType capacityType) {
    return capacityTypePerResource.getOrDefault(capacityType,
        Collections.emptySet());
  }

  public boolean isResourceOfType(
      String resourceName, QueueCapacityType capacityType) {
    return capacityTypes.containsKey(resourceName) &&
        capacityTypes.get(resourceName).equals(capacityType);
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
   *
   * @return capacity types
   */
  public Set<QueueCapacityType> getDefinedCapacityTypes() {
    return capacityTypePerResource.keySet();
  }

  private void storeResourceType(
      String resourceName, QueueCapacityType resourceType) {
    if (capacityTypes.get(resourceName) != null
        && !capacityTypes.get(resourceName).equals(resourceType)) {
      capacityTypePerResource.get(capacityTypes.get(resourceName))
          .remove(resourceName);
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
    for (Map.Entry<String, Float> resourceEntry : resource) {
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
