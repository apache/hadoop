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
package org.apache.hadoop.security;

import java.io.IOException;

/**
 * An exception indicating access control violations.  
 */
public class AccessControlIOException extends IOException {

  private static final long serialVersionUID = -1874018786480045420L;
  
  /**
   * Default constructor is needed for unwrapping from 
   * {@link org.apache.hadoop.ipc.RemoteException}.
   */
  public AccessControlIOException() {
    super("Permission denied.");
  }

  /**
   * Constructs an {@link AccessControlIOException}
   * with the specified detail message.
   * @param s the detail message.
   */
  public AccessControlIOException(String s) {
    super(s);
  }
}
