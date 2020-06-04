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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface filesystems MAY implement to offer a batched list.
 * If implemented, filesystems SHOULD declare
 * {@link CommonPathCapabilities#FS_EXPERIMENTAL_BATCH_LISTING} to be a supported
 * path capability.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BatchListingOperations {

  /**
   * Batched listing API that returns {@link PartialListing}s for the
   * passed Paths.
   *
   * @param paths List of paths to list.
   * @return RemoteIterator that returns corresponding PartialListings.
   * @throws IOException failure
   */
  RemoteIterator<PartialListing<FileStatus>> batchedListStatusIterator(
      List<Path> paths) throws IOException;

  /**
   * Batched listing API that returns {@link PartialListing}s for the passed
   * Paths. The PartialListing will contain {@link LocatedFileStatus} entries
   * with locations.
   *
   * @param paths List of paths to list.
   * @return RemoteIterator that returns corresponding PartialListings.
   * @throws IOException failure
   */
  RemoteIterator<PartialListing<LocatedFileStatus>>
      batchedListLocatedStatusIterator(
          List<Path> paths) throws IOException;

}
