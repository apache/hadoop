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

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.protobuf.ServiceException;

/**
 * Helper methods for protobuf related RPC implementation
 */
@InterfaceAudience.Private
public class ProtobufHelper {
  private ProtobufHelper() {
    // Hidden constructor for class with only static helper methods
  }

  /**
   * Return the RemoteException wrapped in ServiceException as cause.
   * @param se ServiceException that wraps RemoteException
   * @return RemoteException wrapped in ServiceException or
   *         a new IOException that wraps unexpected ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    return ((e instanceof RemoteException) ? (IOException) e : 
      new IOException(se));
  }
}
