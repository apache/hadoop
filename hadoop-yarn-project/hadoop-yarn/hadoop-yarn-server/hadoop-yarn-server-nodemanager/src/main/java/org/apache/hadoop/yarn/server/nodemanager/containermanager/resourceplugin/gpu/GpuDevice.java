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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import java.io.Serializable;

/**
 * This class is used to represent GPU device while allocation.
 */
public class GpuDevice implements Serializable, Comparable {
  private int index;
  private int minorNumber;
  private static final long serialVersionUID = -6812314470754667710L;

  public GpuDevice(int index, int minorNumber) {
    this.index = index;
    this.minorNumber = minorNumber;
  }

  public int getIndex() {
    return index;
  }

  public int getMinorNumber() {
    return minorNumber;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof GpuDevice)) {
      return false;
    }
    GpuDevice other = (GpuDevice) obj;
    return index == other.index && minorNumber == other.minorNumber;
  }

  @Override
  public int compareTo(Object obj) {
    if (obj == null || (!(obj instanceof  GpuDevice))) {
      return -1;
    }

    GpuDevice other = (GpuDevice) obj;

    int result = Integer.compare(index, other.index);
    if (0 != result) {
      return result;
    }
    return Integer.compare(minorNumber, other.minorNumber);
  }

  @Override
  public int hashCode() {
    final int prime = 47;
    return prime * index + minorNumber;
  }

  @Override
  public String toString() {
    return "(index=" + index + ",minor_number=" + minorNumber + ")";
  }
}
