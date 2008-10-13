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
package org.apache.hadoop.fs.permission;

import java.io.IOException;

@Deprecated
public class AccessControlException extends IOException {
  //Required by {@link java.io.Serializable}.
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor is needed for unwrapping from 
   * {@link org.apache.hadoop.ipc.RemoteException}.
   */
  public AccessControlException() {
    super("Permission denied.");
  }

  /**
   * Constructs an {@link AccessControlException}
   * with the specified detail message.
   * @param s the detail message.
   */
  public AccessControlException(String s) {
    super(s);
  }
}
