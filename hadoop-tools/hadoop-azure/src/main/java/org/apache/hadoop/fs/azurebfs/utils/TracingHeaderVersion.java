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

package org.apache.hadoop.fs.azurebfs.utils;

/**
 * Enum representing the version of the tracing header used in Azure Blob File System (ABFS).
 * It defines two versions: V0 and V1, with their respective field counts.
 * Any changes to the tracing header should introduce a new version so that every
 * version has a fixed predefined schema of fields.
 */
public enum TracingHeaderVersion {

  /**
   * Version 0 of the tracing header, which has no version prefix and contains 8 permanent and a few optional fields.
   * This is the initial version of the tracing header.
   */
  V0("", 8),
  /**
   * Version 1 of the tracing header, which includes a version prefix and has 13 permanent fields.
   * This version is used for the current tracing header schema.
   * Schema: version:clientCorrelationId:clientRequestId:fileSystemId
   *         :primaryRequestId:streamId:opType:retryHeader:ingressHandler
   *         :position:operatedBlobCount:operationSpecificHeader:httpOperationHeader
   */
  V1("v1", 13);

  private final String versionString;
  private final int fieldCount;

  TracingHeaderVersion(String versionString, int fieldCount) {
    this.versionString = versionString;
    this.fieldCount = fieldCount;
  }

  @Override
  public String toString() {
    return versionString;
  }

  /**
   * Returns the latest version of the tracing header. Any changes done to the
   * schema of tracing context header should be accompanied by a version bump.
   * @return the latest version of the tracing header.
   */
  public static TracingHeaderVersion getCurrentVersion() {
    return V1;
  }

  public int getFieldCount() {
    return fieldCount;
  }

  public String getVersionString() {
    return versionString;
  }
}
