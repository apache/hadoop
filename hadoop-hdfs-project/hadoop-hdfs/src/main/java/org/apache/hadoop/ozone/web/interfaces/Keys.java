/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.interfaces;

import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;

/**
 * This interface defines operations permitted on a key.
 */

@Path("/{volume}/{bucket}/{keys:.*}")
public interface Keys {

  /**
   * Adds a key to an existing bucket. If the object already exists
   * this call will overwrite or add with new version number if the bucket
   * versioning is turned on.
   *
   * @param volume Storage Volume Name
   * @param bucket Name of the bucket
   * @param keys Name of the Object
   * @param is InputStream or File Data
   * @param req Request
   * @param headers http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @PUT
  @Consumes(MediaType.WILDCARD)
  Response putKey(@PathParam("volume") String volume,
      @PathParam("bucket") String bucket, @PathParam("keys") String keys,
      InputStream is, @Context Request req, @Context UriInfo info,
      @Context HttpHeaders headers) throws OzoneException;

  /**
   * Gets the Key if it exists.
   *
   * @param volume Storage Volume
   * @param bucket Name of the bucket
   * @param keys Object Name
   * @param info Tag info
   * @param req Request
   * @param uriInfo Uri info
   * @param headers Http Header
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @GET
  Response getKey(@PathParam("volume") String volume,
      @PathParam("bucket") String bucket, @PathParam("keys") String keys,
      @QueryParam(Header.OZONE_LIST_QUERY_TAG) String info,
      @Context Request req, @Context UriInfo uriInfo,
      @Context HttpHeaders headers) throws OzoneException;

  /**
   * Deletes an existing key.
   *
   * @param volume Storage Volume Name
   * @param bucket Name of the bucket
   * @param keys Name of the Object
   * @param req http Request
   * @param headers HttpHeaders
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @DELETE
  Response deleteKey(@PathParam("volume") String volume,
      @PathParam("bucket") String bucket, @PathParam("keys") String keys,
      @Context Request req, @Context UriInfo info, @Context HttpHeaders headers)
      throws OzoneException;
}

