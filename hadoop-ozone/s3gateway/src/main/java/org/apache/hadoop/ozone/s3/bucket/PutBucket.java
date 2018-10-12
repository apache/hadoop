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

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;

import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV2;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Create new bucket.
 */
@Path("/{bucket}")
public class PutBucket extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PutBucket.class);

  @PUT
  public Response put(@PathParam("bucket") String bucketName, @Context
                  HttpHeaders httpHeaders) throws IOException, OS3Exception {

    String auth = httpHeaders.getHeaderString("Authorization");
    LOG.info("Auth header string {}", auth);

    if (auth == null) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, S3ErrorTable
          .Resource.HEADER);
    }

    String userName;
    if (auth.startsWith("AWS4")) {
      LOG.info("V4 Header {}", auth);
      AuthorizationHeaderV4 authorizationHeader = new AuthorizationHeaderV4(
          auth);
      userName = authorizationHeader.getAccessKeyID().toLowerCase();
    } else {
      LOG.info("V2 Header {}", auth);
      AuthorizationHeaderV2 authorizationHeader = new AuthorizationHeaderV2(
          auth);
      userName = authorizationHeader.getAccessKeyID().toLowerCase();
    }

    String location = createS3Bucket(userName, bucketName);

    LOG.info("Location is {}", location);
    return Response.status(HttpStatus.SC_OK).header("Location", location)
       .build();

  }
}
