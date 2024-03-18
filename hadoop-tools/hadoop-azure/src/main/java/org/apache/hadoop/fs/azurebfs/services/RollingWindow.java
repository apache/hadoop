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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.LinkedList;

public class RollingWindow {
  private final LinkedList<DataPoint> dataPoints = new LinkedList<>();
  private final long windowSize; // window size in milliseconds

  public RollingWindow(long windowSizeInSeconds) {
    this.windowSize = windowSizeInSeconds * 1000;
  }

  public synchronized void add(long value) {
    dataPoints.add(new DataPoint(System.currentTimeMillis(), value));
    cleanup();
  }

  public synchronized long getSum() {
    cleanup();
    long sum = 0;
    for (DataPoint dataPoint : dataPoints) {
      sum += dataPoint.getValue();
    }
    return sum;
  }

  private void cleanup() {
    long currentTime = System.currentTimeMillis();
    while (!dataPoints.isEmpty() && currentTime - dataPoints.getFirst().getTime() > windowSize) {
      dataPoints.removeFirst();
    }
  }

  private static class DataPoint {
    private final long time;
    private final long value;

    public DataPoint(long time, long value) {
      this.time = time;
      this.value = value;
    }

    public long getTime() {
      return time;
    }

    public long getValue() {
      return value;
    }
  }
}
