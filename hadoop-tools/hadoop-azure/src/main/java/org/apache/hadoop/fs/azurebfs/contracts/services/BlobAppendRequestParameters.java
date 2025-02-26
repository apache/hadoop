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

/**
 * Following parameters are used by AbfsBlobClient only.
 * Blob Endpoint Append API requires blockId and eTag to be passed in the request.
 */
public class BlobAppendRequestParameters {
  private String blockId;
  private String eTag;

  /**
   * Constructor to be used for interacting with AbfsBlobClient.
   * @param blockId blockId of the block to be appended
   * @param eTag eTag of the blob being appended
   */
  public BlobAppendRequestParameters(String blockId, String eTag) {
    this.blockId = blockId;
    this.eTag = eTag;
  }

  public String getBlockId() {
    return blockId;
  }

  public String getETag() {
    return eTag;
  }

  public void setBlockId(final String blockId) {
    this.blockId = blockId;
  }

  public void setETag(final String eTag) {
    this.eTag = eTag;
  }
}
