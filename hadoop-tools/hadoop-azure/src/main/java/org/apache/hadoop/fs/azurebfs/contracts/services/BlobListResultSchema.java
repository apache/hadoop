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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.ArrayList;
import java.util.List;

/**
 * The ListResultSchema model for Blob Endpoint Listing.
 */
public class BlobListResultSchema implements ListResultSchema {

  // List of paths returned by Blob Endpoint Listing.
  private List<BlobListResultEntrySchema> paths;

  // Continuation token for the next page of results.
  private String nextMarker;

  public BlobListResultSchema() {
    this.paths = new ArrayList<>();
    nextMarker = null;
  }

  /**
   * Return the list of paths returned by Blob Endpoint Listing.
   * @return the paths value
   */
  @Override
  public List<BlobListResultEntrySchema> paths() {
    return paths;
  }

  /**
   * Set the paths value to list of paths returned by Blob Endpoint Listing.
   * @param paths the paths value to set
   * @return the ListSchema object itself.
   */
  @Override
  public ListResultSchema withPaths(final List<? extends ListResultEntrySchema> paths) {
    this.paths = (List<BlobListResultEntrySchema>) paths;
    return this;
  }

  public void addBlobListEntry(final BlobListResultEntrySchema blobListEntry) {
    this.paths.add(blobListEntry);
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }
}
