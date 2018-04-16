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

import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.StandbyException;

/**
 * Exception that the Router throws when it is in safe mode. This extends
 * {@link StandbyException} for the client to try another Router when it gets
 * this exception.
 */
public class RouterSafeModeException extends StandbyException {

  private static final long serialVersionUID = 453568188334993493L;

  /** Identifier of the Router that generated this exception. */
  private final String routerId;

  /**
   * Build a new Router safe mode exception.
   * @param router Identifier of the Router.
   * @param op Category of the operation (READ/WRITE).
   */
  public RouterSafeModeException(String router, OperationCategory op) {
    super("Router " + router + " is in safe mode and cannot handle " + op
        + " requests.");
    this.routerId = router;
  }

  /**
   * Get the id of the Router that generated this exception.
   * @return Id of the Router that generated this exception.
   */
  public String getRouterId() {
    return this.routerId;
  }
}
