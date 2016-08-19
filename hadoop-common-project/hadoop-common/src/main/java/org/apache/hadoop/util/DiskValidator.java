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
package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import java.io.File;

/**
 * A interface for disk validators.
 *
 * The {@link #checkStatus(File)} operation checks status of a file/dir.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface DiskValidator {
  /**
   * Check the status of a file/dir.
   * @param dir a file/dir
   * @throws DiskErrorException if any disk error
   */
  void checkStatus(File dir) throws DiskErrorException;
}
