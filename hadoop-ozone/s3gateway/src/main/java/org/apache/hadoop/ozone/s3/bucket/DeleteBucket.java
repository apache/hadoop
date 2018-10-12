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

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import org.apache.http.HttpStatus;

/**
 * Delete a bucket.
 */
@Path("/{bucket}")
public class DeleteBucket extends EndpointBase {

  @DELETE
  @Produces(MediaType.APPLICATION_XML)
  public Response delete(@PathParam("bucket") String bucketName)
      throws IOException, OS3Exception {

    try {
      deleteS3Bucket(bucketName);
    } catch (IOException ex) {
      if (ex.getMessage().contains("BUCKET_NOT_EMPTY")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .BUCKET_NOT_EMPTY, S3ErrorTable.Resource.BUCKET);
        throw os3Exception;
      } else if (ex.getMessage().contains("BUCKET_NOT_FOUND")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .NO_SUCH_BUCKET, S3ErrorTable.Resource.BUCKET);
        throw os3Exception;
      } else {
        throw ex;
      }
    }

    return Response
        .status(HttpStatus.SC_NO_CONTENT)
        .build();

  }
}
