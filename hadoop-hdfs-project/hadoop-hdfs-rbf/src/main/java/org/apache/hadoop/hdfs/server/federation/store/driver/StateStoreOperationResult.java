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

package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * State store operation result with list of failed records.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class StateStoreOperationResult {

  private final List<String> failedRecordsKeys;
  private final boolean isOperationSuccessful;

  private static final StateStoreOperationResult DEFAULT_OPERATION_SUCCESS_RESULT =
      new StateStoreOperationResult(Collections.emptyList(), true);

  /**
   * State store operation result constructor with list of failed records keys and boolean
   * to inform whether the overall operation is successful.
   *
   * @param failedRecordsKeys The list of failed records keys.
   * @param isOperationSuccessful True if the operation was successful, False otherwise.
   */
  public StateStoreOperationResult(List<String> failedRecordsKeys,
      boolean isOperationSuccessful) {
    this.failedRecordsKeys = failedRecordsKeys;
    this.isOperationSuccessful = isOperationSuccessful;
  }

  /**
   * State store operation result constructor with a single failed record key.
   *
   * @param failedRecordKey The failed record key.
   */
  public StateStoreOperationResult(String failedRecordKey) {
    if (failedRecordKey != null && failedRecordKey.length() > 0) {
      this.isOperationSuccessful = false;
      this.failedRecordsKeys = Collections.singletonList(failedRecordKey);
    } else {
      this.isOperationSuccessful = true;
      this.failedRecordsKeys = Collections.emptyList();
    }
  }

  public List<String> getFailedRecordsKeys() {
    return failedRecordsKeys;
  }

  public boolean isOperationSuccessful() {
    return isOperationSuccessful;
  }

  public static StateStoreOperationResult getDefaultSuccessResult() {
    return DEFAULT_OPERATION_SUCCESS_RESULT;
  }
}
