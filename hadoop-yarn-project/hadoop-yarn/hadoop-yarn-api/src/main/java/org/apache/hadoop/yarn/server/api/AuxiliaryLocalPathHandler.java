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
package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** An Interface that can retrieve local directories to read from or write to.
 *  Components can implement this interface to link it to
 *  their own Directory Handler Service
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AuxiliaryLocalPathHandler {
  /**
   * Get a path from the local FS for reading for a given Auxiliary Service.
   * @param path the requested path
   * @return the complete path to the file on a local disk
   * @throws IOException if the file read encounters a problem
   */
  Path getLocalPathForRead(String path) throws IOException;

  /**
   * Get a path from the local FS for writing for a given Auxiliary Service.
   * @param path the requested path
   * @return the complete path to the file on a local disk
   * @throws IOException if the path creations fails
   */
  Path getLocalPathForWrite(String path) throws IOException;

  /**
   * Get a path from the local FS for writing a file of an estimated size
   * for a given Auxiliary Service.
   * @param path the requested path
   * @param size the size of the file that is going to be written
   * @return the complete path to the file on a local disk
   * @throws IOException if the path creations fails
   */
  Path getLocalPathForWrite(String path, long size) throws IOException;
}
