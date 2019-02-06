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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Interface to support authorization in Azure Blob File System.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public interface AbfsAuthorizer {

  /**
   * Initialize authorizer for Azure Blob File System.
   *
   * @throws AbfsAuthorizationException if unable to initialize the authorizer.
   * @throws IOException network problems or similar.
   * @throws IllegalArgumentException if the required parameters are not provided.
   */
  void init() throws AbfsAuthorizationException, IOException;

  /**
   * Checks if the provided {@link FsAction} is allowed on the provided {@link Path}s.
   *
   * @param action the {@link FsAction} being requested on the provided {@link Path}s.
   * @param absolutePaths The absolute paths of the storage being accessed.
   * @return true if authorized, otherwise false.
   * @throws AbfsAuthorizationException on authorization failure.
   * @throws IOException network problems or similar.
   * @throws IllegalArgumentException if the required parameters are not provided.
   */
  boolean isAuthorized(FsAction action, Path... absolutePaths)
      throws AbfsAuthorizationException, IOException;

}
