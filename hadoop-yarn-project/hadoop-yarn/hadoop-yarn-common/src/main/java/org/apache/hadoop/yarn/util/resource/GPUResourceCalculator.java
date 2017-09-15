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
public class GPUResourceCalculator extends ResourceCalculator {

  @Override
  public int compare(Resource unused, Resource lhs, Resource rhs) {
    // Only consider GPU
    return lhs.getGPUs() - rhs.getGPUs();
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    // Only consider GPU
    return available.getGPUs() / required.getGPUs();
  }

  @Override
  public float divide(Resource unused,
      Resource numerator, Resource denominator) {
    return ratio(numerator, denominator);
  }

  public boolean isInvalidDivisor(Resource r) {
    if (r.getGPUs() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    return (float)a.getGPUs() / b.getGPUs();
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getGPUs(), denominator));
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    int normalizedGPU = Math.min(
        roundUp(
            Math.max(r.getGPUs(), minimumResource.getGPUs()),
            stepFactor.getGPUs()),
            maximumResource.getGPUs());
    return Resources.createResource(normalizedGPU);
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource) {
    return normalize(r, minimumResource, maximumResource, minimumResource);
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getGPUs(), stepFactor.getGPUs())
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getGPUs(), stepFactor.getGPUs()));
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp((int)(r.getGPUs() * by + 0.5), stepFactor.getGPUs())
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getGPUs() * by),
            stepFactor.getGPUs()
            )
        );
  }

}
