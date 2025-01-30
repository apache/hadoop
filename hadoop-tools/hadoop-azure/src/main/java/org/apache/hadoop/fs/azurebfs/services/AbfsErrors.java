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
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;

/**
 * ABFS error constants.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AbfsErrors {
  public static final String ERR_WRITE_WITHOUT_LEASE = "Attempted to write to file without lease";
  public static final String ERR_LEASE_EXPIRED = "A lease ID was specified, but the lease for the resource has expired.";
  public static final String ERR_LEASE_EXPIRED_BLOB = "A lease ID was specified, but the lease for the blob has expired.";
  public static final String ERR_NO_LEASE_ID_SPECIFIED = "There is currently a lease on the "
      + "resource and no lease ID was specified in the request";
  public static final String ERR_NO_LEASE_ID_SPECIFIED_BLOB = "There is currently a lease on the "
      + "blob and no lease ID was specified in the request";
  public static final String ERR_PARALLEL_ACCESS_DETECTED = "Parallel access to the create path "
      + "detected. Failing request to honor single writer semantics";
  public static final String ERR_ACQUIRING_LEASE = "Unable to acquire lease";
  public static final String ERR_LEASE_ALREADY_PRESENT = "There is already a lease present";
  public static final String ERR_LEASE_NOT_PRESENT = "There is currently no lease on the resource";
  public static final String ERR_LEASE_ID_NOT_PRESENT = "The lease ID is not present with the "
      + "specified lease operation";
  public static final String ERR_LEASE_DID_NOT_MATCH = "The lease ID specified did not match the "
      + "lease ID for the resource with the specified lease operation";
  public static final String ERR_LEASE_BROKEN = "The lease ID matched, but the lease has been "
      + "broken explicitly and cannot be renewed";
  public static final String ERR_LEASE_FUTURE_EXISTS = "There is already an existing lease "
      + "operation";
  public static final String ERR_NO_LEASE_THREADS = "Lease desired but no lease threads "
      + "configured, set " + FS_AZURE_LEASE_THREADS;
  public static final String ERR_CREATE_ON_ROOT = "Cannot create file over root path";
  public static final String PATH_EXISTS = "The specified path, or an element of the path, "
      + "exists and its resource type is invalid for this operation.";
  public static final String BLOB_OPERATION_NOT_SUPPORTED = "Blob operation is not supported.";
  public static final String INVALID_APPEND_OPERATION = "The resource was created or modified by the Azure Blob Service API "
      + "and cannot be appended to by the Azure Data Lake Storage Service API";
  public static final String CONDITION_NOT_MET = "The condition specified using "
      + "HTTP conditional header(s) is not met.";
  /**
   * Exception message on filesystem init if token-provider-auth-type configs are provided
   */
  public static final String UNAUTHORIZED_SAS = "Incorrect SAS token provider configured for non-hierarchical namespace account.";
  public static final String ERR_RENAME_BLOB =
      "FNS-Blob rename was not successful for source and destination path: ";
  public static final String ERR_DELETE_BLOB =
      "FNS-Blob delete was not successful for path: ";
  public static final String ATOMIC_DIR_RENAME_RECOVERY_ON_GET_PATH_EXCEPTION =
      "Path had to be recovered from atomic rename operation.";
  private AbfsErrors() {}
}
