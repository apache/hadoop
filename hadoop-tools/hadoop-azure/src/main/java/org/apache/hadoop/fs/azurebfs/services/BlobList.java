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

import java.util.ArrayList;
import java.util.List;

/**
 *  Contains result of GetBlobList API in processable state.
 */
public class BlobList {

  /**
   * List of {@link BlobProperty} returned by server
   */
  private final List<BlobProperty> blobProperties = new ArrayList<>();

  /**
   * Since there could be many blobs which can be returned by server on the
   * GetBlobList API on a path and server wants to return only limited number of
   * blob-information in one go. The expectation from the server is to use a token
   * called as <code>nextMarker</code> and call the GetBlobList API again for the
   * same path.
   */
  private String nextMarker;

  void addBlobProperty(final BlobProperty blobProperty) {
    blobProperties.add(blobProperty);
  }

  void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }

  public List<BlobProperty> getBlobPropertyList() {
    return blobProperties;
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public BlobList() {

  }
}
