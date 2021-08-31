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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
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
    Iterable<QueueCapacityVector.QueueResourceVectorEntry> {
  private final ResourceVector resource;
  private final Map<String, QueueVectorResourceType> resourceTypes
      = new HashMap<>();
  private final Set<QueueVectorResourceType>
      definedResourceTypes = new HashSet<>();

  public QueueCapacityVector(ResourceVector resource) {
    this.resource = resource;
  }

  public static QueueCapacityVector empty() {
    return new QueueCapacityVector(ResourceVector.empty());
  }

  public QueueResourceVectorEntry getResource(String resourceName) {
    return new QueueResourceVectorEntry(resourceTypes.get(resourceName),
        resourceName, resource.getValue(resourceName));
  }

  public void setResource(String resourceName, float value,
                          QueueVectorResourceType resourceType) {
    resource.setValue(resourceName, value);
    storeResourceType(resourceName, resourceType);
  }

  public float getMemory() {
    return resource.getValue(ResourceInformation.MEMORY_URI);
  }

  public Set<QueueVectorResourceType> getDefinedResourceTypes() {
    return definedResourceTypes;
  }

  @Override
  public Iterator<QueueResourceVectorEntry> iterator() {
    return new Iterator<QueueResourceVectorEntry>() {
      private final Iterator<Map.Entry<String, Float>> resources =
          resource.iterator();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return resources.hasNext() && resourceTypes.size() > i;
      }

      @Override
      public QueueResourceVectorEntry next() {
        Map.Entry<String, Float> resourceInformation = resources.next();
        i++;
        return new QueueResourceVectorEntry(
            resourceTypes.get(resourceInformation.getKey()),
            resourceInformation.getKey(), resourceInformation.getValue());
      }
    };
  }

  private void storeResourceType(String resourceName, QueueVectorResourceType resourceType) {
    definedResourceTypes.add(resourceType);
    resourceTypes.put(resourceName, resourceType);
  }

  /**
   * Represents a calculation type of a resource.
   */
  public enum QueueVectorResourceType {
    PERCENTAGE("%"), ABSOLUTE(""), WEIGHT("w");

    private static final Set<QueueVectorResourceType> FLOAT_TYPES =
        ImmutableSet.of(QueueVectorResourceType.PERCENTAGE,
            QueueVectorResourceType.WEIGHT);
    private final String postfix;

    QueueVectorResourceType(String postfix) {
      this.postfix = postfix;
    }

    public String getPostfix() {
      return postfix;
    }
  }

  public static class QueueResourceVectorEntry {
    private final QueueVectorResourceType vectorResourceType;
    private final float resourceValue;
    private final String resourceName;

    public QueueResourceVectorEntry(QueueVectorResourceType vectorResourceType,
                                    String resourceName, float resourceValue) {
      this.vectorResourceType = vectorResourceType;
      this.resourceValue = resourceValue;
      this.resourceName = resourceName;
    }

    public QueueVectorResourceType getVectorResourceType() {
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
