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
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Represents a simple resource floating point value storage
 * grouped by resource names.
 */
public class ResourceVector implements Iterable<Map.Entry<String, Float>> {
  private final Map<String, Float> resourcesByName = new HashMap<>();

  public static ResourceVector empty() {
    ResourceVector emptyResourceVector = new ResourceVector();
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      emptyResourceVector.setValue(resource.getName(), 0);
    }

    return emptyResourceVector;
  }

  public static ResourceVector uniform(float value) {
    ResourceVector emptyResourceVector = new ResourceVector();
    for (ResourceInformation resource : ResourceUtils.getResourceTypesArray()) {
      emptyResourceVector.setValue(resource.getName(), value);
    }

    return emptyResourceVector;
  }

  public float getValue(String resourceName) {
    return resourcesByName.getOrDefault(resourceName, 1f);
  }

  public void setValue(String resourceName, float value) {
    resourcesByName.put(resourceName, value);
  }

  @Override
  public Iterator<Map.Entry<String, Float>> iterator() {
    return resourcesByName.entrySet().iterator();
  }
}
