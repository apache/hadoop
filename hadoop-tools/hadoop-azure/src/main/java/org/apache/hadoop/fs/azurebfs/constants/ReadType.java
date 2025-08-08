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
 * Enumeration for different types of read operations triggered by AbfsInputStream.
 */
public enum ReadType {
  /**
   * Synchronous read from the storage service. No optimization is being applied.
   */
  DIRECT_READ("DR"),
  /**
   * Synchronous read from the storage service where optimization were considered but found disabled.
   */
  NORMAL_READ("NR"),
  /**
   * Asynchronous read from the storage service for filling up cache.
   */
  PREFETCH_READ("PR"),
  /**
   * Synchronous read from the storage service when nothing was found in cache.
   */
  MISSEDCACHE_READ("MR"),
  /**
   * Synchronous read from the storage service for reading the footer of a file.
   * Only triggered when footer read optimization kicks in.
   */
  FOOTER_READ("FR"),
  /**
   * Synchronous read from the storage service for reading a small file fully.
   * Only triggered when small file read optimization kicks in.
   */
  SMALLFILE_READ("SR"),
  /**
   * None of the above read types were applicable.
   */
  UNKNOWN_READ("UR");

  private final String readType;

  ReadType(String readType) {
    this.readType = readType;
  }

  /**
   * Get the read type as a string.
   *
   * @return the read type string
   */
  @Override
  public String toString() {
    return readType;
  }
}
