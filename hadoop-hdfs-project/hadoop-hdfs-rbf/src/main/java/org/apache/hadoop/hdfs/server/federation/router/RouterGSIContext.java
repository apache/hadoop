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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ClientGSIContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;

/**
 * Global State ID context for the router.
 * <p>
 * This is the router side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RouterGSIContext extends ClientGSIContext {
  private volatile boolean enableObserverRead = false;

  public RouterGSIContext(boolean enableObserverRead) {
    super();
    setEnableObserverRead(enableObserverRead);
  }

  public void setEnableObserverRead(boolean enableObserverRead) {
    this.enableObserverRead = enableObserverRead;
  }


  /**
   * Router side implementation for providing state alignment info in requests.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    if (enableObserverRead) {
      super.updateRequestState(header);
    }
  }
}
