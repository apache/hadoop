/*
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import com.google.common.base.Function;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 */
public interface RpcServer {

  void setSocketSendBufSize(int size);

  void start();

  void stop();

  void join() throws InterruptedException;

  InetSocketAddress getListenerAddress();

  /** Called for each call.
   * @param param writable parameter
   * @param receiveTime time
   * @return Writable
   * @throws java.io.IOException e
   */
  Writable call(Class<? extends VersionedProtocol> protocol,
      Writable param, long receiveTime)
      throws IOException;

  int getNumOpenConnections();

  int getCallQueueLen();

  void setErrorHandler(HBaseRPCErrorHandler handler);

  void setQosFunction(Function<Writable, Integer> newFunc);

  void openServer();

  void startThreads();

  /**
   * Needed for delayed calls.  We need to be able to store the current call
   * so that we can complete it later.
   * @return Call the server is currently handling.
   */
  Delayable getCurrentCall();

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  HBaseRpcMetrics getRpcMetrics();
}
