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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Thrown when the constraints enoded in a {@link PathHandle} do not hold.
 * For example, if a handle were created with the default
 * {@link Options.HandleOpt#path()} constraints, a call to
 * {@link FileSystem#open(PathHandle)} would succeed if the file were
 * modified, but if a different file was at that location then it would throw
 * this exception.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidPathHandleException extends IOException {
  private static final long serialVersionUID = 0xcd8ac329L;

  public InvalidPathHandleException(String str) {
    super(str);
  }

  public InvalidPathHandleException(String message, Throwable cause) {
    super(message, cause);
  }

}
