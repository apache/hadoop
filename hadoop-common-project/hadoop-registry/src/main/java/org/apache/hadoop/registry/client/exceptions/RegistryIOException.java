/*
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

package org.apache.hadoop.registry.client.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;

/**
 * Base exception for registry operations.
 * <p>
 * These exceptions include the path of the failing operation wherever possible;
 * this can be retrieved via {@link PathIOException#getPath()}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryIOException extends PathIOException {

  /**
   * Build an exception from any other Path IO Exception.
   * This propagates the path of the original exception
   * @param message more specific text
   * @param cause cause
   */
  public RegistryIOException(String message, PathIOException cause) {
    super(cause.getPath() != null ? cause.getPath().toString() : "",
        message,
        cause);
  }

  public RegistryIOException(String path, Throwable cause) {
    super(path, cause);
  }

  public RegistryIOException(String path, String error) {
    super(path, error);
  }

  public RegistryIOException(String path, String error, Throwable cause) {
    super(path, error, cause);
  }
}
