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
 
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

public class QueueEntitlement {

  private float capacity;
  private float maxCapacity;

  public QueueEntitlement(float capacity, float maxCapacity){
    this.setCapacity(capacity);
    this.maxCapacity = maxCapacity;
   }

  public float getMaxCapacity() {
    return maxCapacity;
  }

  public void setMaxCapacity(float maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  public float getCapacity() {
    return capacity;
  }

  public void setCapacity(float capacity) {
    this.capacity = capacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof QueueEntitlement))
      return false;

    QueueEntitlement that = (QueueEntitlement) o;

    if (Float.compare(that.capacity, capacity) != 0)
      return false;
    return Float.compare(that.maxCapacity, maxCapacity) == 0;
  }

  @Override
  public int hashCode() {
    int result = (capacity != +0.0f ? Float.floatToIntBits(capacity) : 0);
    result = 31 * result + (maxCapacity != +0.0f ? Float.floatToIntBits(
        maxCapacity) : 0);
    return result;
  }
}
