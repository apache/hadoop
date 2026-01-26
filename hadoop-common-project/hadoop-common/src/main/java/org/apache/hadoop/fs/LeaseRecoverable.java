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

package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * Whether the given Path of the FileSystem has the capability to perform lease recovery.
 */
public interface LeaseRecoverable {

  /**
   * Start the lease recovery of a file.
   *
   * @param file path to a file.
   * @return true if the file is already closed, and it does not require lease recovery.
   * @throws IOException if an error occurs during lease recovery.
   * @throws UnsupportedOperationException if lease recovery is not supported by this filesystem.
   */
  boolean recoverLease(Path file) throws IOException;

  /**
   * Get the close status of a file.
   * @param file The string representation of the path to the file
   * @return return true if file is closed
   * @throws IOException If an I/O error occurred
   * @throws UnsupportedOperationException if isFileClosed is not supported by this filesystem.
   */
  boolean isFileClosed(Path file) throws IOException;
}
