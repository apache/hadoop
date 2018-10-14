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
package org.apache.hadoop.ozone.s3.bucket;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.commontypes.BucketMetadata;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

/**
 * List Object Rest endpoint.
 */
@Path("/{volume}")
public class ListBucket extends EndpointBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ListBucket.class);

  @GET
  @Produces(MediaType.APPLICATION_XML)
  public ListBucketResponse get(@PathParam("volume") String volumeName)
      throws OS3Exception, IOException {
    OzoneVolume volume;
    try {
      volume = getVolume(volumeName);
    } catch (NotFoundException ex) {
      LOG.error("Exception occurred in ListBucket: volume {} not found.",
          volumeName, ex);
      OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
          .NO_SUCH_VOLUME, S3ErrorTable.Resource.VOLUME);
      throw os3Exception;
    } catch (IOException e) {
      throw e;
    }

    Iterator<? extends OzoneBucket> volABucketIter = volume.listBuckets(null);
    ListBucketResponse response = new ListBucketResponse();

    while(volABucketIter.hasNext()) {
      OzoneBucket next = volABucketIter.next();
      BucketMetadata bucketMetadata = new BucketMetadata();
      bucketMetadata.setName(next.getName());
      bucketMetadata.setCreationDate(
          Instant.ofEpochMilli(next.getCreationTime()));
      response.addBucket(bucketMetadata);
    }

    return response;
  }
}
