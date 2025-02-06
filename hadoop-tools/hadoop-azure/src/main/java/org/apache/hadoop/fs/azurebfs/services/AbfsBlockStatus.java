/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

/**
 * Enum representing the status of an ABFS block.
 *
 * <p>This enum is used to indicate the current status of a block in the Azure Blob File System (ABFS).
 * The possible statuses are:</p>
 * <ul>
 *   <li>NEW - The block is newly created and has not been processed yet.</li>
 *   <li>SUCCESS - The block has been successfully processed.</li>
 *   <li>FAILED - The block processing has failed.</li>
 * </ul>
 */
public enum AbfsBlockStatus {
  /**
   * The block is newly created and has not been processed yet.
   */
  NEW,

  /**
   * The block has been successfully processed.
   */
  SUCCESS,

  /**
   * The block processing has failed.
   */
  FAILED
}
