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
 * The ListResultEntrySchema model.
 */
public interface ListResultEntrySchema {

  /**
   * Get the name value.
   * @return the name value
   */
  String name();

  /**
   * Set the name value.
   * @param name the name value to set
   * @return the ListResultEntrySchema object itself.
   */
  ListResultEntrySchema withName(String name);

  /**
   * Get the isDirectory value.
   * @return the isDirectory value
   */
  Boolean isDirectory();

  /**
   * Get the lastModified value.
   * @return the lastModified value
   */
  String lastModified();

  /**
   * Get the eTag value.
   * @return the eTag value
   */
  String eTag();

  /**
   * Get the contentLength value.
   * @return the contentLength value
   */
  Long contentLength();

  /**
   * Get the owner value.
   * @return the owner value
   */
  String owner();

  /**
   * Get the group value.
   * @return the group value
   */
  String group();

  /**
   * Get the permissions value.
   * @return the permissions value
   */
  String permissions();

  /**
   * Get the encryption context value.
   * @return the encryption context value
   */
  String getXMsEncryptionContext();

  /**
   * Get the customer-provided encryption-256 value.
   * @return the customer-provided encryption-256 value
   */
  String getCustomerProvidedKeySha256();
}
