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
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.s3.EndpointBase;

/**
 * Get object info rest endpoint.
 */
@Path("/{volume}/{bucket}/{path:.+}")
public class HeadObject extends EndpointBase {

  @HEAD
  @Produces(MediaType.APPLICATION_XML)
  public Response head(
      @PathParam("volume") String volumeName,
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @HeaderParam("Content-Length") long length,
      InputStream body) throws IOException {

    OzoneBucket bucket = getBucket(volumeName, bucketName);
    OzoneKeyDetails key = bucket.getKey(keyPath);

    return Response.
        ok()
        .header("Content-Length", key.getDataSize())
        .build();

  }
}
