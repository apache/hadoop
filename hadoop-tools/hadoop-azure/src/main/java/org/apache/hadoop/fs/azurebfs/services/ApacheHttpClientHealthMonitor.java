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


public class ApacheHttpClientHealthMonitor {

  private static final RollingWindow SERVER_CALLS = new RollingWindow(10);

  private static final RollingWindow IO_EXCEPTIONS = new RollingWindow(10);
  private ApacheHttpClientHealthMonitor() {}

  public static boolean usable() {
    final long serverCalls = SERVER_CALLS.getSum();
    if(serverCalls == 0) {
      return true;
    }
    final long exceptions = IO_EXCEPTIONS.getSum();
    final double ratio = ((double)exceptions / serverCalls);
    return ratio < 0.01;

  }

  public static void incrementServerCalls() {
    SERVER_CALLS.add(1);
  }

  public static void incrementUnknownIoExceptions() {
    IO_EXCEPTIONS.add(1);
  }
}
