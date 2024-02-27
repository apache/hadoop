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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;

/**
 * A pass through fairness policy that implements
 * {@link RouterRpcFairnessPolicyController} and allows any number
 * of handlers to connect to any specific downstream name service.
 */
public class NoRouterRpcFairnessPolicyController implements
    RouterRpcFairnessPolicyController {

  public NoRouterRpcFairnessPolicyController(Configuration conf) {
      // Dummy constructor.
  }

  @Override
  public boolean acquirePermit(String nsId) {
    return true;
  }

  @Override
  public void releasePermit(String nsId) {
    // Dummy, pass through.
  }

  @Override
  public void shutdown() {
    // Nothing for now.
  }

  @Override
  public String getAvailableHandlerOnPerNs(){
    return "N/A";
  }

  @Override
  public int getAvailablePermits(String nsId) {
    return 0;
  }

  @Override
  public boolean contains(String nsId) {
    return true;
  }
}
