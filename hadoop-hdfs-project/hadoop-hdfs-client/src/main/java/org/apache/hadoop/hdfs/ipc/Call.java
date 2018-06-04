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
package org.apache.hadoop.hdfs.ipc;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
class Call {
  private final int id;

  private final String protocolName;

  private final long protocolVersion;

  private final String methodName;

  private final Message param;

  private final Message responseDefaultType;

  private volatile Message response;

  private volatile IOException error;

  private boolean done;

  private final RpcCallback<Call> callback;

  Call(int id, String protocolName, long protocolVersion, String methodName,
      Message param, Message responseDefaultType, RpcCallback<Call> callback) {
    this.id = id;
    this.protocolName = protocolName;
    this.protocolVersion = protocolVersion;
    this.methodName = methodName;
    this.param = param;
    this.responseDefaultType = responseDefaultType;
    this.callback = callback;
  }

  private void callComplete() {
    callback.run(this);
  }

  /**
   * Set the exception when there is an error. Notify the caller the call is
   * done.
   *
   * @param error exception thrown by the call; either local or remote
   */
  void setException(IOException error) {
    synchronized (this) {
      if (done) {
        return;
      }
      this.done = true;
      this.error = error;
    }
    callComplete();
  }

  /**
   * Set the return value when there is no error. Notify the caller the call is
   * done.
   *
   * @param response return value of the call.
   * @param cells Can be null
   */
  void setResponse(Message response) {
    synchronized (this) {
      if (done) {
        return;
      }
      this.done = true;
      this.response = response;
    }
    callComplete();
  }

  int getId() {
    return id;
  }

  String getProtocolName() {
    return protocolName;
  }

  long getProtocolVersion() {
    return protocolVersion;
  }

  String getMethodName() {
    return methodName;
  }

  Message getParam() {
    return param;
  }

  Message getResponseDefaultType() {
    return responseDefaultType;
  }

  Message getResponse() {
    return response;
  }

  IOException getError() {
    return error;
  }

  boolean isDone() {
    return done;
  }
}