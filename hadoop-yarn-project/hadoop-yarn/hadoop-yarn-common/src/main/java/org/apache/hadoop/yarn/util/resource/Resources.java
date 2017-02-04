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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public class Resources {
  
  // Java doesn't have const :(
  private static final Resource NONE = new Resource() {

    @Override
    public int getMemory() {
      return 0;
    }

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return 0;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGPUs() {
      return 0;
    }

    @Override
    public void setGPUs(int GPUs) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGPULocality() {
      return 0;
    }

    @Override
    public void setGPULocality(int GPULocality) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = 0 - o.getMemory();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGPUs();
        }
      }
      return diff;
    }
    
  };
  
  private static final Resource UNBOUNDED = new Resource() {

    @Override
    public int getMemory() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGPUs() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setGPUs(int GPUs) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGPULocality() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setGPULocality(int GPULocality) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = 0 - o.getMemory();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGPUs();
        }
      }
      return diff;
    }
    
  };

  public static Resource createResource(int memory) {
    return createResource(memory, (memory > 0) ? 1 : 0, (memory > 0) ? 1 : 0);
  }

  public static Resource createResource(int memory, int cores, int GPUs) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setVirtualCores(cores);
    resource.setGPUs(GPUs);
    resource.setGPULocality(0);
    return resource;
  }

  public static Resource createResource(int memory, int cores, int GPUs, int GPULocality) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setVirtualCores(cores);
    resource.setGPUs(GPUs);
    resource.setGPULocality(GPULocality);
    return resource;
  }

  public static Resource none() {
    return NONE;
  }
  
  public static Resource unbounded() {
    return UNBOUNDED;
  }

  public static Resource clone(Resource res) {
    return createResource(res.getMemory(), res.getVirtualCores(), res.getGPUs(), res.getGPULocality());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() + rhs.getGPUs());
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource addToWithLocality(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() + rhs.getGPUs());

    // MJTHIS: FIXME: not clear what to do with recovery
    // Must uncomment it when you are running test cases
    assert (lhs.getGPULocality() & rhs.getGPULocality()) == 0;
    lhs.setGPULocality(lhs.getGPULocality() | rhs.getGPULocality());

    return lhs;
  }

  public static Resource addWithLocality(Resource lhs, Resource rhs) {
    return addToWithLocality(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() - rhs.getGPUs());
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  public static Resource subtractFromWithLocality(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory());
    lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() - rhs.getGPUs());

    // MJTHIS: FIXME: not clear what to do with recovery
    // Must uncomment it when you are running test cases
    assert (lhs.getGPULocality() | rhs.getGPULocality()) == lhs.getGPULocality();
    lhs.setGPULocality(lhs.getGPULocality() & ~rhs.getGPULocality());

    return lhs;
  }

  public static Resource subtractWithLocality(Resource lhs, Resource rhs) {
    return subtractFromWithLocality(clone(lhs), rhs);
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, double by) {
    lhs.setMemory((int)(lhs.getMemory() * by));
    lhs.setVirtualCores((int)(lhs.getVirtualCores() * by));
    lhs.setGPUs((int)(lhs.getGPUs() * by));
    return lhs;
  }

  public static Resource multiply(Resource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }
  
  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }
  
  public static Resource multiplyAndNormalizeDown(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }
  
  public static Resource multiplyAndRoundDown(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemory((int)(lhs.getMemory() * by));
    out.setVirtualCores((int)(lhs.getVirtualCores() * by));
    out.setGPUs((int)(lhs.getGPUs() * by));
    return out;
  }
  
  public static Resource normalize(
      ResourceCalculator calculator, Resource lhs, Resource min,
      Resource max, Resource increment) {
    return calculator.normalize(lhs, min, max, increment);
  }
  
  public static Resource roundUp(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundUp(lhs, factor);
  }
  
  public static Resource roundDown(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundDown(lhs, factor);
  }
  
  public static boolean isInvalidDivisor(
      ResourceCalculator resourceCalculator, Resource divisor) {
    return resourceCalculator.isInvalidDivisor(divisor);
  }

  public static float ratio(
      ResourceCalculator resourceCalculator, Resource lhs, Resource rhs) {
    return resourceCalculator.ratio(lhs, rhs);
  }
  
  public static float divide(
      ResourceCalculator resourceCalculator,
      Resource clusterResource, Resource lhs, Resource rhs) {
    return resourceCalculator.divide(clusterResource, lhs, rhs);
  }
  
  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, int rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }
  
  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceCalculator resourceCalculator,
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0;
  }
  
  public static Resource min(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static Resource max(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
  }
  
  public static boolean fitsIn(Resource smaller, Resource bigger) {
    return smaller.getMemory() <= bigger.getMemory() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores() &&
        smaller.getGPUs() <= bigger.getGPUs();
  }

  public static boolean fitsInWithLocality(Resource smaller, Resource bigger, Resource all) {
    boolean fitsIn = fitsIn(smaller, bigger);
    if (fitsIn == true) {
      if (smaller.getGPULocality() > 0) {
        if(searchGPUs(smaller.getGPUs(), bigger.getGPULocality(), all.getGPULocality(), true, false) != 0) {
          return true;
        }
        else {
          return false;
        }
      }
      else {
        if(searchGPUs(smaller.getGPUs(), bigger.getGPULocality(), all.getGPULocality(), false, false) != 0) {
          return true;
        }
        else {
          return false;
        }
      }
    }
    else {
      return false;
    }
  }
  
  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    return createResource(Math.min(lhs.getMemory(), rhs.getMemory()),
        Math.min(lhs.getVirtualCores(), rhs.getVirtualCores()),
        Math.min(lhs.getGPUs(), rhs.getGPUs()));
  }
  
  public static Resource componentwiseMax(Resource lhs, Resource rhs) {
    return createResource(Math.max(lhs.getMemory(), rhs.getMemory()),
        Math.max(lhs.getVirtualCores(), rhs.getVirtualCores()),
        Math.max(lhs.getGPUs(), rhs.getGPUs()));
  }

  public static int allocateGPUs(Resource smaller, Resource bigger, Resource all) {
    if (smaller.getGPULocality() > 0) {
      return searchGPUs(smaller.getGPUs(), bigger.getGPULocality(), all.getGPULocality(), true, true);
    }
    else {
      return searchGPUs(smaller.getGPUs(), bigger.getGPULocality(), all.getGPULocality(), false, true);
    }
  }

  private static synchronized int searchGPUs(int request, int available, int total, boolean locality, boolean allocate)
  {
    assert request <= 32;

    if (locality == false) {
      if (allocate == false) {
        return request <= Integer.bitCount(available) ? 1 : 0;
      }
      else {
        int result = allocateGPUs(request, available);
        assert Integer.bitCount(result) == request;
        return result;
      }
    }
    else {
      // Now, just assume that each CPU node has four GPUs.
      int numGPUsPerNode = 4;
      int bitmask = 0xF;
      int span = ((request - 1) / 4) + 1;   // Number of CPU nodes to span
      int result = 0;
      int requestPerNode = Math.min(request, numGPUsPerNode);

      for (int i = 0; i < Integer.SIZE / numGPUsPerNode; i++) {
        int in = available & (bitmask << (i * 4));
        int out = allocateGPUs(requestPerNode, in);
        if (Integer.bitCount(out) == requestPerNode) {
          span--;
          result = result | out;
          if (span == 0) {
            break;
          }
        }
      }
      if (allocate == false) {
        return Integer.bitCount(result) == request ? 1 : 0;
      }
      else {
        assert Integer.bitCount(result) == request;
        return result;
      }
    }
  }

  private static int allocateGPUs(int request, int available)
  {
    int result = 0;
    int pos = 1;
    while ((Integer.bitCount(result) < request) && (pos != 0)) {
      if ((pos & available) != 0) {
        result = result | pos;
      }
      pos = pos << 1;
    }
    return result;
  }
}
