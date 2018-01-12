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
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ValueRanges;

/**
 * A {@link ResourceCalculator} which uses the concept of  
 * <em>dominant resource</em> to compare multi-dimensional resources.
 *
 * Essentially the idea is that the in a multi-resource environment, 
 * the resource allocation should be determined by the dominant share 
 * of an entity (user or queue), which is the maximum share that the 
 * entity has been allocated of any resource. 
 * 
 * In a nutshell, it seeks to maximize the minimum dominant share across 
 * all entities. 
 * 
 * For example, if user A runs CPU-heavy tasks and user B runs
 * memory-heavy tasks, it attempts to equalize CPU share of user A 
 * with Memory-share of user B. 
 * 
 * In the single resource case, it reduces to max-min fairness for that resource.
 * 
 * See the Dominant Resource Fairness paper for more details:
 * www.cs.berkeley.edu/~matei/papers/2011/nsdi_drf.pdf
 */
@Private
@Unstable
public class DominantResourceCalculator extends ResourceCalculator {
  
  @Override
  public int compare(Resource clusterResource, Resource lhs, Resource rhs) {
    
    if (lhs.equals(rhs)) {
      return 0;
    }
    
    if (isInvalidDivisor(clusterResource)) {
      if ((lhs.getMemory() < rhs.getMemory() && lhs.getVirtualCores() > rhs
          .getVirtualCores())
          || (lhs.getMemory() > rhs.getMemory() && lhs.getVirtualCores() < rhs
              .getVirtualCores())) {
        return 0;
      } else if (lhs.getMemory() > rhs.getMemory()
          || lhs.getVirtualCores() > rhs.getVirtualCores()) {
        return 1;
      } else if (lhs.getMemory() < rhs.getMemory()
          || lhs.getVirtualCores() < rhs.getVirtualCores()) {
        return -1;
      }
    }

    float l = getResourceAsValue(clusterResource, lhs, true);
    float r = getResourceAsValue(clusterResource, rhs, true);
    
    if (l < r) {
      return -1;
    } else if (l > r) {
      return 1;
    } else {
      l = getResourceAsValue(clusterResource, lhs, false);
      r = getResourceAsValue(clusterResource, rhs, false);
      if (l < r) {
        return -1;
      } else if (l > r) {
        return 1;
      }
    }

    int diff = 0;
    ValueRanges lPorts = lhs.getPorts();
    ValueRanges rPorts = rhs.getPorts();
    if (lPorts == null) {
      diff = rPorts == null ? 0 : 1;
    } else if (rPorts == null) {
      diff = -1;
    } else {
      diff = lPorts.compareTo(rPorts);
    }
    
    return diff;
  }


  /**
   * Use 'dominant' for now since we only have 2 resources - gives us a slight
   * performance boost.
   * 
   * Once we add more resources, we'll need a more complicated (and slightly
   * less performant algorithm).
   */
  protected float getResourceAsValue(
      Resource clusterResource, Resource resource, boolean dominant) {
    // Just use 'dominant' resource
      float maxV =  Math.max(
                (float)resource.getMemory() / clusterResource.getMemory(), 
                (float)resource.getVirtualCores() / clusterResource.getVirtualCores()
                );
      float minV =  Math.min(
              (float)resource.getMemory() / clusterResource.getMemory(), 
              (float)resource.getVirtualCores() / clusterResource.getVirtualCores()
              ); 
      
      if(resource.getGPUs() != 0 && clusterResource.getGPUs() != 0) {
          maxV = Math.max(maxV, (float)resource.getGPUs()/clusterResource.getGPUs());
          minV = Math.min(minV, (float)resource.getGPUs()/clusterResource.getGPUs());
      }
      return (dominant) ? maxV:minV;
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {

    int num = Integer.MAX_VALUE;
    if (required.getPorts() != null && required.getPorts().getRangesCount() > 0) {
      // required ports resource, so we can not allocate more than one container
      num = 1;
    }
    num = Math.min(
        Math.min(
            available.getMemory() / required.getMemory(),
            available.getVirtualCores() / required.getVirtualCores()), num);

    if (required.getGPUs() != 0) {
      num = Math.min(num, available.getGPUs() / required.getGPUs());
    }
    return num;
  }

  @Override
  public float divide(Resource clusterResource, 
      Resource numerator, Resource denominator) {
    return 
        getResourceAsValue(clusterResource, numerator, true) / 
        getResourceAsValue(clusterResource, denominator, true);
  }
  
  @Override
  public boolean isInvalidDivisor(Resource r) {
    if (r.getMemory() == 0.0f || r.getVirtualCores() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
      float rate = Math.max(
        (float)a.getMemory()/b.getMemory(), 
        (float)a.getVirtualCores()/b.getVirtualCores()
        );
       if(b.getGPUs() != 0) {
           rate = Math.max(rate, (float)a.getGPUs() /b.getGPUs());
       }
       return rate;
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator),
        divideAndCeil(numerator.getVirtualCores(), denominator),
        divideAndCeil(numerator.getGPUs(), denominator),
        numerator.getGPUAttribute()
        );
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
      roundUp(
        Math.max(r.getMemory(), minimumResource.getMemory()),
        stepFactor.getMemory()),
      maximumResource.getMemory());
    int normalizedCores = Math.min(
      roundUp(
        Math.max(r.getVirtualCores(), minimumResource.getVirtualCores()),
        stepFactor.getVirtualCores()),
      maximumResource.getVirtualCores());
    int normalizedGPUs = Math.min(
      roundUp(
        Math.max(r.getGPUs(), minimumResource.getGPUs()),
        stepFactor.getGPUs()),
      maximumResource.getGPUs());

    return Resources.createResource(normalizedMemory,
      normalizedCores, normalizedGPUs, r.getGPUAttribute(), r.getPorts());
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory()), 
        roundUp(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundUp(r.getGPUs(), stepFactor.getGPUs()),
        r.getGPUAttribute(), r.getPorts()
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()),
        roundDown(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundDown(r.getGPUs(), stepFactor.getGPUs()),
        r.getGPUAttribute(),r.getPorts()
        );
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp(
            (int)Math.ceil(r.getMemory() * by), stepFactor.getMemory()),
        roundUp(
            (int)Math.ceil(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()),
        roundUp(
            (int)Math.ceil(r.getGPUs() * by),
            stepFactor.getGPUs()),
        r.getGPUAttribute(),
        r.getPorts()
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            ),
        roundDown(
            (int)(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()
            ),
        roundDown(
            (int)(r.getGPUs() * by),
            stepFactor.getGPUs()
            ),
        r.getGPUAttribute(),
        r.getPorts()
        );
  }

}
