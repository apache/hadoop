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

package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;

/**
 * No op default RPC scheduler.
 */
public class DefaultRpcScheduler implements RpcScheduler {
  @Override
  public int getPriorityLevel(Schedulable obj) {
    return 0;
  }

  @Override
  public boolean shouldBackOff(Schedulable obj) {
    return false;
  }

  @Override
  public void addResponseTime(String name, int priorityLevel, int queueTime,
      int processingTime) {
  }

  public DefaultRpcScheduler(int priorityLevels, String namespace,
      Configuration conf) {
  }

  @Override
  public void stop() {
  }
}
