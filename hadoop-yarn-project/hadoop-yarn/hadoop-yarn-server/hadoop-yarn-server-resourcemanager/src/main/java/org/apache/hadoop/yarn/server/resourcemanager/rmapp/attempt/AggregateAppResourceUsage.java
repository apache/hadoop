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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class AggregateAppResourceUsage {
  long memorySeconds;
  long vcoreSeconds;
  long GPUSeconds;
  long GpuBitVecSeconds;

  public AggregateAppResourceUsage(long memorySeconds, long vcoreSeconds, long GPUSeconds, long GpuBitVecSeconds) {
    this.memorySeconds = memorySeconds;
    this.vcoreSeconds = vcoreSeconds;
    this.GPUSeconds = GPUSeconds;
    this.GpuBitVecSeconds = GpuBitVecSeconds;
  }

  /**
   * @return the memorySeconds
   */
  public long getMemorySeconds() {
    return memorySeconds;
  }

  /**
   * @param memorySeconds the memorySeconds to set
   */
  public void setMemorySeconds(long memorySeconds) {
    this.memorySeconds = memorySeconds;
  }

  /**
   * @return the vcoreSeconds
   */
  public long getVcoreSeconds() {
    return vcoreSeconds;
  }

  /**
   * @param vcoreSeconds the vcoreSeconds to set
   */
  public void setVcoreSeconds(long vcoreSeconds) {
    this.vcoreSeconds = vcoreSeconds;
  }

  /**
   * @return the GPUSeconds
   */
  public long getGPUSeconds() {
    return GPUSeconds;
  }

  /**
   * @param GPUSeconds the GPUSeconds to set
   */
  public void setGPUSeconds(long GPUSeconds) {
    this.GPUSeconds = GPUSeconds;
  }

  /**
   * @return the GpuBitVecSeconds
   */
  public long getGpuBitVecSeconds() {
    return GpuBitVecSeconds;
  }

  /**
   * @param GpuBitVecSeconds the GpuBitVecSeconds to set
   */
  public void setGpuBitVecSeconds(long GpuBitVecSeconds) {
    this.GpuBitVecSeconds = GpuBitVecSeconds;
  }
}
