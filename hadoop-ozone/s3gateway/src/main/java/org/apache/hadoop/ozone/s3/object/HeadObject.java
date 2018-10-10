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
package org.apache.hadoop.ozone.s3.object;

import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get object info rest endpoint.
 */
@Path("/{volume}/{bucket}/{path:.+}")
public class HeadObject extends EndpointBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(HeadObject.class);

  @HEAD
  @Produces(MediaType.APPLICATION_XML)
  public Response head(
      @PathParam("volume") String volumeName,
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath) throws Exception {
    OzoneKeyDetails key;

    try {
      key = getVolume(volumeName).getBucket(bucketName).getKey(keyPath);
      // TODO: return the specified range bytes of this object.
    } catch (IOException ex) {
      LOG.error("Exception occurred in HeadObject", ex);
      if (ex.getMessage().contains("KEY_NOT_FOUND")) {
        OS3Exception os3Exception = S3ErrorTable.newError(S3ErrorTable
            .NO_SUCH_OBJECT, S3ErrorTable.Resource.OBJECT);
        throw os3Exception;
      } else {
        throw ex;
      }
    }

    return Response.ok().status(HttpStatus.SC_OK)
        .header("Last-Modified", key.getModificationTime())
        .header("ETag", "" + key.getModificationTime())
        .header("Content-Length", key.getDataSize())
        .header("Content-Type", "binary/octet-stream")
        .build();
  }
}
