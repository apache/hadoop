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

package org.apache.hadoop.ozone.client;

import java.util.List;

/**
 * A class that encapsulates OzoneKeyLocation.
 */
public class OzoneKeyDetails extends OzoneKey {

  /**
   * A list of block location information to specify replica locations.
   */
  private List<OzoneKeyLocation> ozoneKeyLocations;

  /**
   * Constructs OzoneKeyDetails from OmKeyInfo.
   */
  public OzoneKeyDetails(String volumeName, String bucketName, String keyName,
                  long size, long creationTime, long modificationTime,
                         List<OzoneKeyLocation> ozoneKeyLocations) {
    super(volumeName, bucketName, keyName, size, creationTime,
        modificationTime);
    this.ozoneKeyLocations = ozoneKeyLocations;
  }

  /**
   * Returns the location detail information of the specific Key.
   */
  public List<OzoneKeyLocation> getOzoneKeyLocations() {
    return ozoneKeyLocations;
  }

  /**
   * Set details of key location.
   * @param ozoneKeyLocations - details of key location
   */
  public void setOzoneKeyLocations(List<OzoneKeyLocation> ozoneKeyLocations) {
    this.ozoneKeyLocations = ozoneKeyLocations;
  }
}
