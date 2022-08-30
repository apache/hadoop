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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Represents a simple resource floating point value grouped by resource names.
 */
public class ResourceVector implements Iterable<Map.Entry<String, Double>> {
  private final Map<String, Double> resourcesByName = new HashMap<>();

  /**
   * Creates a new {@code ResourceVector} with all pre-defined resources set to
   * zero.
   * @return zero resource vector
   */
  public static ResourceVector newInstance() {
    ResourceVector zeroResourceVector = new ResourceVector();
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      zeroResourceVector.setValue(resource.getName(), 0);
    }

    return zeroResourceVector;
  }

  /**
   * Creates a new {@code ResourceVector} with all pre-defined resources set to
   * the same value.
   * @param value the value to set all resources to
   * @return uniform resource vector
   */
  public static ResourceVector of(double value) {
    ResourceVector emptyResourceVector = new ResourceVector();
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      emptyResourceVector.setValue(resource.getName(), value);
    }

    return emptyResourceVector;
  }

  /**
   * Creates a new {@code ResourceVector} with the values set in a
   * {@code Resource} object.
   * @param resource resource object the resource vector will be based on
   * @return uniform resource vector
   */
  public static ResourceVector of(Resource resource) {
    ResourceVector resourceVector = new ResourceVector();
    for (ResourceInformation resourceInformation : resource.getResources()) {
      resourceVector.setValue(resourceInformation.getName(),
          resourceInformation.getValue());
    }

    return resourceVector;
  }

  /**
   * Decrements values for each resource defined in the given resource vector.
   * @param otherResourceVector rhs resource vector of the subtraction
   */
  public void decrement(ResourceVector otherResourceVector) {
    for (Map.Entry<String, Double> resource : otherResourceVector) {
      setValue(resource.getKey(), getValue(resource.getKey()) - resource.getValue());
    }
  }

  /**
   * Decrements the given resource by the specified value.
   * @param resourceName name of the resource
   * @param value value to be subtracted from the resource's current value
   */
  public void decrement(String resourceName, double value) {
    setValue(resourceName, getValue(resourceName) - value);
  }

  /**
   * Increments the given resource by the specified value.
   * @param resourceName name of the resource
   * @param value value to be added to the resource's current value
   */
  public void increment(String resourceName, double value) {
    setValue(resourceName, getValue(resourceName) + value);
  }

  public double getValue(String resourceName) {
    return resourcesByName.get(resourceName);
  }

  public void setValue(String resourceName, double value) {
    resourcesByName.put(resourceName, value);
  }

  public boolean isEmpty() {
    return resourcesByName.isEmpty();
  }

  public Set<String> getResourceNames() {
    return resourcesByName.keySet();
  }

  @Override
  public Iterator<Map.Entry<String, Double>> iterator() {
    return resourcesByName.entrySet().iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return this.resourcesByName.equals(((ResourceVector) o).resourcesByName);
  }

  @Override
  public int hashCode() {
    return resourcesByName.hashCode();
  }
}
