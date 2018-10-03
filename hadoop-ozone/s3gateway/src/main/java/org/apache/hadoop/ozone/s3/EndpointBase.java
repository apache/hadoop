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
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable.Resource;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic helpers for all the REST endpoints.
 */
public class EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(EndpointBase.class);
  @Inject
  private OzoneClient client;
  private String requestId;

  protected OzoneBucket getBucket(String volumeName, String bucketName)
      throws IOException {
    return getVolume(volumeName).getBucket(bucketName);
  }

  protected OzoneBucket getBucket(OzoneVolume volume, String bucketName)
      throws OS3Exception, IOException {
    OzoneBucket bucket;
    try {
      bucket = volume.getBucket(bucketName);
    } catch (IOException ex) {
      LOG.error("Error occurred is {}", ex);
      if (ex.getMessage().contains("NOT_FOUND")) {
        OS3Exception oex = S3ErrorTable.newError(S3ErrorTable.NO_SUCH_BUCKET,
            OzoneUtils.getRequestID(), Resource.BUCKET);
        throw oex;
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

  /**
   * Set the requestId.
   * @param id
   */
  protected void setRequestId(String id) {
    this.requestId = id;
  }

  @VisibleForTesting
  public String getRequestId() {
    return requestId;
  }

}
