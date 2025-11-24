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

package org.apache.hadoop.fs.azurebfs.constants;

/**
 * Azure Storage Offers two sets of Rest APIs for interacting with the storage account.
 * <ol>
 *   <li>Blob Rest API: <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api></a></li>
 *   <li>Data Lake Rest API: <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/operation-groups></a></li>
 * </ol>
 */
public enum AbfsServiceType {
  /**
   * Service type to set operative endpoint as Data Lake Rest API.
   */
  DFS,
  /**
   * Service type to set operative endpoint as Blob Rest API.
   */
  BLOB
}
