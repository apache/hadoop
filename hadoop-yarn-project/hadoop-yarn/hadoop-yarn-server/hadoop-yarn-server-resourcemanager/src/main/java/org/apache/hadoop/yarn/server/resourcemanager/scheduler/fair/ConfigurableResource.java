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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.Arrays;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

/**
 * A {@code ConfigurableResource} object represents an entity that is used to
 * configure resources, such as maximum resources of a queue. It can be
 * percentage of cluster resources or an absolute value.
 */
@Private
@Unstable
public class ConfigurableResource {
  private final Resource resource;
  private final double[] percentages;

  ConfigurableResource() {
    this(getOneHundredPercentArray());
  }

  ConfigurableResource(double[] percentages) {
    this.percentages = percentages.clone();
    this.resource = null;
  }

  /**
   * Creates a  {@link ConfigurableResource} instance with all resource values
   * initialized to {@code value}.
   * @param value the value to use for all resources
   */
  ConfigurableResource(long value) {
    this(ResourceUtils.createResourceWithSameValue(value));
  }

  public ConfigurableResource(Resource resource) {
    this.percentages = null;
    this.resource = resource;
  }

  private static double[] getOneHundredPercentArray() {
    double[] resourcePercentages =
        new double[ResourceUtils.getNumberOfCountableResourceTypes()];
    Arrays.fill(resourcePercentages, 1.0);

    return resourcePercentages;
  }

  /**
   * Get resource by multiplying the cluster resource and the percentage of
   * each resource respectively. Return the absolute resource if either
   * {@code percentages} or {@code clusterResource} is null.
   *
   * @param clusterResource the cluster resource
   * @return resource the resulting resource
   */
  public Resource getResource(Resource clusterResource) {
    if (percentages != null && clusterResource != null) {
      long memory = (long) (clusterResource.getMemorySize() * percentages[0]);
      int vcore = (int) (clusterResource.getVirtualCores() * percentages[1]);
      Resource res = Resource.newInstance(memory, vcore);
      ResourceInformation[] clusterInfo = clusterResource.getResources();

      for (int i = 2; i < clusterInfo.length; i++) {
        res.setResourceValue(i,
            (long)(clusterInfo[i].getValue() * percentages[i]));
      }

      return res;
    } else {
      return resource;
    }
  }

  /**
   * Get the absolute resource.
   *
   * @return the absolute resource
   */
  public Resource getResource() {
    return resource;
  }

  /**
   * Set the value of the wrapped resource if this object isn't setup to use
   * percentages. If this object is set to use percentages, this method has
   * no effect.
   *
   * @param name the name of the resource
   * @param value the value
   */
  void setValue(String name, long value) {
    if (resource != null) {
      resource.setResourceValue(name, value);
    }
  }

  /**
   * Set the percentage of the resource if this object is setup to use
   * percentages. If this object is set to use percentages, this method has
   * no effect.
   *
   * @param name the name of the resource
   * @param value the percentage
   */
  void setPercentage(String name, double value) {
    if (percentages != null) {
      Integer index = ResourceUtils.getResourceTypeIndex().get(name);

      if (index != null) {
        percentages[index] = value;
      } else {
        throw new ResourceNotFoundException("The requested resource, \""
            + name + "\", could not be found.");
      }
    }
  }

  public double[] getPercentages() {
    return percentages == null ? null :
      Arrays.copyOf(percentages, percentages.length);
  }
}
