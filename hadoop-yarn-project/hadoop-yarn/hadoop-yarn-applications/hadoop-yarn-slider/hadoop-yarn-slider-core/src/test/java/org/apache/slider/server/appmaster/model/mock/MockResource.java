/*
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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Mock resource.
 */
public class MockResource extends Resource {
  private int memory;
  private int virtualCores;

  public MockResource(int memory, int vcores) {
    this.memory = memory;
    this.virtualCores = vcores;
  }

  @Override
  public int compareTo(Resource other) {
    long diff = this.getMemorySize() - other.getMemorySize();
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
    }
    return diff == 0 ? 0 : (diff > 0 ? 1 : -1);
  }

  @Override
  public long getMemorySize() {
    return memory;
  }

  @Override
  public void setMemorySize(long memorySize) {
    memory = (int) memorySize;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public void setVirtualCores(int vCores) {
    this.virtualCores = vCores;
  }

  @Deprecated
  @Override
  public int getMemory() {
    return memory;
  }

  @Deprecated
  @Override
  public void setMemory(int memory) {
    this.memory = memory;
  }
}
