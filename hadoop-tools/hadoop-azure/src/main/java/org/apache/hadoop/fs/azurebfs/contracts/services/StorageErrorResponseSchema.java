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

package org.apache.hadoop.fs.azurebfs.contracts.services;

/**
 * Response Schema for Storage Error Parsing.
 * Common schema for both endpoints.
 */
public class StorageErrorResponseSchema {

  public StorageErrorResponseSchema(final String storageErrorCode,
      final String storageErrorMessage,
      final String expectedAppendPos) {
    this.storageErrorCode = storageErrorCode;
    this.storageErrorMessage = storageErrorMessage;
    this.expectedAppendPos = expectedAppendPos;
  }

  private String storageErrorCode;
  private String storageErrorMessage;
  private String expectedAppendPos;

  public String getStorageErrorCode() {
    return storageErrorCode;
  }

  public void setStorageErrorCode(final String storageErrorCode) {
    this.storageErrorCode = storageErrorCode;
  }

  public String getStorageErrorMessage() {
    return storageErrorMessage;
  }

  public void setStorageErrorMessage(final String storageErrorMessage) {
    this.storageErrorMessage = storageErrorMessage;
  }

  public String getExpectedAppendPos() {
    return expectedAppendPos;
  }

  public void setExpectedAppendPos(final String expectedAppendPos) {
    this.expectedAppendPos = expectedAppendPos;
  }
}
