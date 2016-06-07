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

@Private
@Unstable
public class DefaultResourceCalculator extends ResourceCalculator {
  
  @Override
  public int compare(Resource unused, Resource lhs, Resource rhs) {
    // Only consider memory
    return Long.compare(lhs.getMemorySize(), rhs.getMemorySize());
  }

  @Override
  public long computeAvailableContainers(Resource available, Resource required) {
    // Only consider memory
    return available.getMemorySize() / required.getMemorySize();
  }

  @Override
  public float divide(Resource unused, 
      Resource numerator, Resource denominator) {
    return ratio(numerator, denominator);
  }
  
  public boolean isInvalidDivisor(Resource r) {
    if (r.getMemorySize() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    return (float)a.getMemorySize() / b.getMemorySize();
  }

  @Override
  public Resource divideAndCeil(Resource numerator, long denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemorySize(), denominator));
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    long normalizedMemory = Math.min(
        roundUp(
            Math.max(r.getMemorySize(), minimumResource.getMemorySize()),
            stepFactor.getMemorySize()),
            maximumResource.getMemorySize());
    return Resources.createResource(normalizedMemory);
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemorySize(), stepFactor.getMemorySize())
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemorySize(), stepFactor.getMemorySize()));
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp((long) (r.getMemorySize() * by + 0.5),
            stepFactor.getMemorySize()));
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (long)(r.getMemorySize() * by),
            stepFactor.getMemorySize()
            )
        );
  }

  @Override
  public boolean fitsIn(Resource cluster,
      Resource smaller, Resource bigger) {
    return smaller.getMemorySize() <= bigger.getMemorySize();
  }
}
