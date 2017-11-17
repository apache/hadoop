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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ResourceSizing} contains information for the size of a
 * {@link SchedulingRequest}, such as the number of requested allocations and
 * the resources for each allocation.
 */
@Public
@Unstable
public abstract class ResourceSizing {

  @Public
  @Unstable
  public static ResourceSizing newInstance(Resource resources) {
    return ResourceSizing.newInstance(1, resources);
  }

  @Public
  @Unstable
  public static ResourceSizing newInstance(int numAllocations, Resource resources) {
    ResourceSizing resourceSizing = Records.newRecord(ResourceSizing.class);
    resourceSizing.setNumAllocations(numAllocations);
    resourceSizing.setResources(resources);
    return resourceSizing;
  }

  @Public
  @Unstable
  public abstract int getNumAllocations();

  @Public
  @Unstable
  public abstract void setNumAllocations(int numAllocations);

  @Public
  @Unstable
  public abstract Resource getResources();

  @Public
  @Unstable
  public abstract void setResources(Resource resources);

  @Override
  public int hashCode() {
    int result = getResources().hashCode();
    result = 31 * result + getNumAllocations();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if(obj == null || getClass() != obj.getClass()) {
      return false;
    }

    ResourceSizing that = (ResourceSizing) obj;

    if(getNumAllocations() != that.getNumAllocations()) {
      return  false;
    }
    if(!getResources().equals(that.getResources())) {
      return false;
    }
    return true;
  }
}
