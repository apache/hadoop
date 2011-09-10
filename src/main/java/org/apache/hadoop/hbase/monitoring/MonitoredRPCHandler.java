/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.monitoring;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * A MonitoredTask implementation optimized for use with RPC Handlers 
 * handling frequent, short duration tasks. String concatenations and object 
 * allocations are avoided in methods that will be hit by every RPC call.
 */
public interface MonitoredRPCHandler extends MonitoredTask {
  public abstract String getRPC();
  public abstract String getRPC(boolean withParams);
  public abstract long getRPCPacketLength();
  public abstract String getClient();
  public abstract long getRPCStartTime();
  public abstract long getRPCQueueTime();
  public abstract boolean isRPCRunning();
  public abstract boolean isOperationRunning();
  
  public abstract void setRPC(String methodName, Object [] params,
      long queueTime);
  public abstract void setRPCPacket(Writable param);
  public abstract void setConnection(String clientAddress, int remotePort);
}
