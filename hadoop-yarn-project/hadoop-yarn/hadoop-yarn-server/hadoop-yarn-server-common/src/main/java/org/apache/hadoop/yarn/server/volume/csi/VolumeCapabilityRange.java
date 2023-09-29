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
package org.apache.hadoop.yarn.server.volume.csi;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;

/**
 * Volume capability range that specified in a volume resource request,
 * this range defines the desired min/max capacity.
 */
public final class VolumeCapabilityRange {

  private final long minCapacity;
  private final long maxCapacity;
  private final String unit;

  private VolumeCapabilityRange(long minCapacity,
      long maxCapacity, String unit) {
    this.minCapacity = minCapacity;
    this.maxCapacity = maxCapacity;
    this.unit = unit;
  }

  public long getMinCapacity() {
    return minCapacity;
  }

  public long getMaxCapacity() {
    return maxCapacity;
  }

  public String getUnit() {
    return unit;
  }

  @Override
  public String toString() {
    return "MinCapability: " + minCapacity + unit
        + ", MaxCapability: " + maxCapacity + unit;
  }

  public static VolumeCapabilityBuilder newBuilder() {
    return new VolumeCapabilityBuilder();
  }

  /**
   * The builder used to build a VolumeCapabilityRange instance.
   */
  public static class VolumeCapabilityBuilder {
    // An invalid default value implies this value must be set
    private long minCap = -1L;
    private long maxCap = Long.MAX_VALUE;
    private String unit;

    public VolumeCapabilityBuilder minCapacity(long minCapacity) {
      this.minCap = minCapacity;
      return this;
    }

    public VolumeCapabilityBuilder maxCapacity(long maxCapacity) {
      this.maxCap = maxCapacity;
      return this;
    }

    public VolumeCapabilityBuilder unit(String capacityUnit) {
      this.unit = capacityUnit;
      return this;
    }

    public VolumeCapabilityRange build() throws InvalidVolumeException {
      VolumeCapabilityRange
          capability = new VolumeCapabilityRange(minCap, maxCap, unit);
      validateCapability(capability);
      return capability;
    }

    private void validateCapability(VolumeCapabilityRange capability)
        throws InvalidVolumeException {
      if (capability.getMinCapacity() < 0) {
        throw new InvalidVolumeException("Invalid volume capability range,"
            + " minimal capability must not be less than 0. Capability: "
            + capability.toString());
      }
      if (Strings.isNullOrEmpty(capability.getUnit())) {
        throw new InvalidVolumeException("Invalid volume capability range,"
            + " capability unit is missing. Capability: "
            + capability.toString());
      }
    }
  }
}
