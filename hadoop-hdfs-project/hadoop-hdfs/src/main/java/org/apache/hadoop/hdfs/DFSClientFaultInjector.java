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
package org.apache.hadoop.hdfs;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Used for injecting faults in DFSClient and DFSOutputStream tests.
 * Calls into this are a no-op in production code. 
 */
@VisibleForTesting
@InterfaceAudience.Private
public class DFSClientFaultInjector {
  public static DFSClientFaultInjector instance = new DFSClientFaultInjector();
  public static AtomicLong exceptionNum = new AtomicLong(0);

  public static void set(DFSClientFaultInjector dfsClientFaultInjector) {
    instance = dfsClientFaultInjector;
  }

  public static DFSClientFaultInjector get() {
    return instance;
  }

  public boolean corruptPacket() {
    return false;
  }

  public boolean uncorruptPacket() {
    return false;
  }

  public boolean failPacket() {
    return false;
  }

  public void startFetchFromDatanode() {}

  public void fetchFromDatanodeException() {}

  public void readFromDatanodeDelay() {}

  public boolean skipRollingRestartWait() {
    return false;
  }
}
