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
package org.apache.hadoop.ozone.s3;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;

import com.google.common.annotations.VisibleForTesting;

/**
 * Basic helpers for all the REST endpoints.
 */
public class EndpointBase {

  @Inject
  private OzoneClient client;

  protected OzoneBucket getBucket(String volumeName, String bucketName)
      throws IOException {
    return getVolume(volumeName).getBucket(bucketName);
  }

  protected OzoneBucket getBucket(OzoneVolume volume, String bucketName)
      throws IOException {
    OzoneBucket bucket = null;
    try {
      bucket = volume.getBucket(bucketName);
    } catch (Exception ex) {
      if (ex.getMessage().contains("NOT_FOUND")) {
        throw new NotFoundException("Bucket" + bucketName + " is not found");
      } else {
        throw ex;
      }
    }
    return bucket;
  }

  protected OzoneVolume getVolume(String volumeName) throws IOException {
    OzoneVolume volume = null;
    try {
      volume = client.getObjectStore().getVolume(volumeName);
    } catch (Exception ex) {
      if (ex.getMessage().contains("NOT_FOUND")) {
        throw new NotFoundException("Volume " + volumeName + " is not found");
      } else {
        throw ex;
      }
    }
    return volume;
  }

  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }
}
