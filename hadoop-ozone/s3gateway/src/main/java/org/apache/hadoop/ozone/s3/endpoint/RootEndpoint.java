/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.commontypes.BucketMetadata;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.header.AuthenticationHeaderParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.s3.util.OzoneS3Util.getVolumeName;

/**
 * Top level rest endpoint.
 */
@Path("/")
public class RootEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootEndpoint.class);

  /**
   * Rest endpoint to list all the buckets of the current user.
   *
   * See https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
   * for more details.
   */
  @GET
  public Response get()
      throws OS3Exception, IOException {
    OzoneVolume volume;
    ListBucketResponse response = new ListBucketResponse();

    AuthenticationHeaderParser authenticationHeaderParser =
        getAuthenticationHeaderParser();

    if (!authenticationHeaderParser.doesAuthenticationInfoExists()) {
      return Response.status(Status.TEMPORARY_REDIRECT)
          .header("Location", "/static/")
          .build();
    }
    String volumeName = getVolumeName(authenticationHeaderParser.
        getAccessKeyID());
    Iterator<? extends OzoneBucket> bucketIterator = listS3Buckets(volumeName,
        null);

    while (bucketIterator.hasNext()) {
      OzoneBucket next = bucketIterator.next();
      BucketMetadata bucketMetadata = new BucketMetadata();
      bucketMetadata.setName(next.getName());
      bucketMetadata.setCreationDate(Instant.ofEpochMilli(next
          .getCreationTime()));
      response.addBucket(bucketMetadata);
    }

    return Response.ok(response).build();
  }
}
