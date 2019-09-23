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
package org.apache.hadoop.hdfs.server.federation.metrics;

/**
 * Implementation of the State Store metrics which does not do anything.
 * This is used when the metrics are disabled (e.g., tests).
 */
public class NullStateStoreMetrics extends StateStoreMetrics {
  public void addRead(long latency) {}
  public long getReadOps() {
    return -1;
  }
  public double getReadAvg() {
    return -1;
  }
  public void addWrite(long latency) {}
  public long getWriteOps() {
    return -1;
  }
  public double getWriteAvg() {
    return -1;
  }
  public void addFailure(long latency) {  }
  public long getFailureOps() {
    return -1;
  }
  public double getFailureAvg() {
    return -1;
  }
  public void addRemove(long latency) {}
  public long getRemoveOps() {
    return -1;
  }
  public double getRemoveAvg() {
    return -1;
  }
  public void setCacheSize(String name, int size) {}
  public void reset() {}
  public void shutdown() {}
}
