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
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This interface is implemented by the client side translators and can be used
 * to obtain information about underlying protocol e.g. to check if a method is
 * supported on the server side.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface ProtocolMetaInterface {
  
  /**
   * Checks whether the given method name is supported by the server.
   * It is assumed that all method names are unique for a protocol.
   * @param methodName The name of the method
   * @return true if method is supported, otherwise false.
   * @throws IOException
   */
  public boolean isMethodSupported(String methodName) throws IOException;
}
