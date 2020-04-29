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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class ExternalCall<T> extends Call {
  private final PrivilegedExceptionAction<T> action;
  private final AtomicBoolean done = new AtomicBoolean();
  private T result;
  private Throwable error;

  public ExternalCall(PrivilegedExceptionAction<T> action) {
    this.action = action;
  }

  @Override
  public String getDetailedMetricsName() {
    return "(external)";
  }

  public abstract UserGroupInformation getRemoteUser();

  public final T get() throws InterruptedException, ExecutionException {
    waitForCompletion();
    if (error != null) {
      throw new ExecutionException(error);
    }
    return result;
  }

  // wait for response to be triggered to support postponed calls
  private void waitForCompletion() throws InterruptedException {
    synchronized(done) {
      while (!done.get()) {
        try {
          done.wait();
        } catch (InterruptedException ie) {
          if (Thread.interrupted()) {
            throw ie;
          }
        }
      }
    }
  }

  boolean isDone() {
    return done.get();
  }

  // invoked by ipc handler
  @Override
  public final Void run() throws IOException {
    try {
      result = action.run();
      sendResponse();
    } catch (Throwable t) {
      abortResponse(t);
    }
    return null;
  }

  @Override
  final void doResponse(Throwable t, RpcStatusProto status) {
    synchronized(done) {
      error = t;
      done.set(true);
      done.notify();
    }
  }
}