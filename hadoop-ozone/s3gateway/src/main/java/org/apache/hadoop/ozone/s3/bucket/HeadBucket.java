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

import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the bucket exists or not.
 */
@Path("/{bucket}")
public class HeadBucket extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(HeadBucket.class);

  @HEAD
  public Response head(@PathParam("bucket") String bucketName)
      throws Exception {
    try {
      getVolume(getOzoneVolumeName(bucketName)).getBucket(bucketName);
    } catch (IOException ex) {
      LOG.error("Exception occurred in headBucket", ex);
      if (ex.getMessage().contains("NOT_FOUND")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
                .NO_SUCH_BUCKET, Resource.BUCKET);
        throw os3Exception;
      } else {
        throw ex;
      }
    }
    return Response.ok().build();
  }
}
