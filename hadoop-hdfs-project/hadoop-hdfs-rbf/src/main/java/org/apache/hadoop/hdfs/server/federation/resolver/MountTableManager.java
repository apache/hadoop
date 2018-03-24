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
package org.apache.hadoop.hdfs.server.federation.resolver;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;

/**
 * Manage a mount table.
 */
public interface MountTableManager {

  /**
   * Add an entry to the mount table.
   *
   * @param request Fully populated request object.
   * @return True if the mount table entry was successfully committed to the
   *         data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException;

  /**
   * Updates an existing entry in the mount table.
   *
   * @param request Fully populated request object.
   * @return True if the mount table entry was successfully committed to the
   *         data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException;

  /**
   * Remove an entry from the mount table.
   *
   * @param request Fully populated request object.
   * @return True the mount table entry was removed from the data store.
   * @throws IOException Throws exception if the data store is not initialized.
   */
  RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException;

  /**
   * List all mount table entries present at or below the path. Fetches from the
   * state store.
   *
   * @param request Fully populated request object.
   *
   * @return List of all mount table entries under the path. Zero-length list if
   *         none are found.
   * @throws IOException Throws exception if the data store cannot be queried.
   */
  GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException;
}