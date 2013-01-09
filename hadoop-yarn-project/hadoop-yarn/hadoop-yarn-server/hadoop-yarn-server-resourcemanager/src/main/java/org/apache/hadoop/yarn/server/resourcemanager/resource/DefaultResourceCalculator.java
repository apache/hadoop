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
package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.yarn.api.records.Resource;

public class DefaultResourceCalculator extends ResourceCalculator {

  Resource clusterResource;
  
  @Override
  public int compare(Resource unused, Resource lhs, Resource rhs) {
    // Only consider memory
    return lhs.getMemory() - rhs.getMemory();
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    // Only consider memory
    return available.getMemory() / required.getMemory();
  }

  @Override
  public float divide(Resource unused, 
      Resource numerator, Resource denominator) {
    return ratio(numerator, denominator);
  }

  @Override
  public float ratio(Resource a, Resource b) {
    return (float)a.getMemory() / b.getMemory();
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator));
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource) {
    return Resources.createResource(
        roundUp(
            Math.max(r.getMemory(), minimumResource.getMemory()),
            minimumResource.getMemory())
        );
  }

  @Override
  public Resource roundUp(Resource r, Resource minimumResource) {
    return Resources.createResource(
        roundUp(r.getMemory(),minimumResource.getMemory())
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()));
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp((int)(r.getMemory() * by + 0.5), stepFactor.getMemory())
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            )
        );
  }

}
