/**
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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;

/**
 * The ThreadLocalContext class is designed to capture and transfer the context of a
 * thread-local environment within the Hadoop Distributed File System (HDFS) federation
 * router operations. This is particularly useful for preserving the state across
 * asynchronous operations where the context needs to be maintained consistently.
 *
 * The context includes details such as the current call being processed by the server, the
 * caller's context, and performance monitoring timestamps. By transferring this context,
 * the class ensures that the operations performed on worker threads correctly reflect
 * the state of the original calling thread.
 *
 * Here is a high-level overview of the main components captured by this context:
 * <ul>
 *     <li>{@link Server.Call} - Represents the current server call.</li>
 *     <li>{@link CallerContext} - Stores information about the caller.</li>
 *     <li>startOpTime - Time for an operation to be received in the Router.</li>
 *     <li>proxyOpTime - Time for an operation to be sent to the Namenode.</li>
 * </ul>
 *
 * This class is typically used in scenarios where asynchronous processing is involved, to
 * ensure that the thread executing the asynchronous task has the correct context applied.
 *
 * @see Server
 * @see CallerContext
 * @see FederationRPCPerformanceMonitor
 */
public class ThreadLocalContext {

  /** The current server call being processed. */
  private final Server.Call call;
  /** The caller context containing information about the caller. */
  private final CallerContext context;
  /** Time for an operation to be received in the Router. */
  private final long startOpTime;
  /** Time for an operation to be sent to the Namenode. */
  private final long proxyOpTime;

  /**
   * Constructs a new {@link  ThreadLocalContext} instance, capturing the current
   * thread-local context at the point of creation.
   */
  public ThreadLocalContext() {
    this.call = Server.getCurCall().get();
    this.context = CallerContext.getCurrent();
    this.startOpTime = FederationRPCPerformanceMonitor.getStartOpTime();
    this.proxyOpTime =  FederationRPCPerformanceMonitor.getProxyOpTime();
  }

  /**
   * Transfers the captured context to the current thread. This method is used to apply
   * the context to worker threads that are processing asynchronous tasks, ensuring
   * that the task execution reflects the state of the original calling thread.
   */
  public void transfer() {
    if (call != null) {
      Server.getCurCall().set(call);
    }
    if (context != null) {
      CallerContext.setCurrent(context);
    }
    if (startOpTime != -1L) {
      FederationRPCPerformanceMonitor.setStartOpTime(startOpTime);
    }
    if (proxyOpTime != -1L) {
      FederationRPCPerformanceMonitor.setProxyOpTime(proxyOpTime);
    }
  }

  @Override
  public String toString() {
    return "ThreadLocalContext{" +
        "call=" + call +
        ", context=" + context +
        ", startOpTime=" + startOpTime +
        ", proxyOpTime=" + proxyOpTime +
        '}';
  }
}
