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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Bucket Interface acts as the HTTP entry point for
 * bucket related functionality.
 */
@Path("/{volume}/{bucket}")
@Api(tags = "bucket")
public interface Bucket {
  /**
   * createBucket call handles the POST request for Creating a Bucket.
   *
   * @param volume - Volume name
   * @param bucket - Bucket Name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @POST
  @ApiOperation("Create new bucket to a volume")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "x-ozone-version", example = "v1", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "x-ozone-user", example = "user", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "Date", example = "Date: Mon, 26 Jun 2017 "
          + "04:23:30 GMT", required = true, paramType = "header"),
      @ApiImplicitParam(name = "Authorization", example = "OZONE", required =
          true, paramType = "header")})
  Response createBucket(@PathParam("volume") String volume,
                        @PathParam("bucket") String bucket,
                        @Context Request req, @Context UriInfo info,
                        @Context HttpHeaders headers) throws OzoneException;

  /**
   * updateBucket call handles the PUT request for updating a Bucket.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @PUT
  @ApiOperation("Update bucket")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "x-ozone-version", example = "v1", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "x-ozone-user", example = "user", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "Date", example = "Date: Mon, 26 Jun 2017 "
          + "04:23:30 GMT", required = true, paramType = "header"),
      @ApiImplicitParam(name = "Authorization", example = "OZONE", required =
          true, paramType = "header")})
  Response updateBucket(@PathParam("volume") String volume,
                        @PathParam("bucket") String bucket,
                        @Context Request req, @Context UriInfo info,
                        @Context HttpHeaders headers) throws OzoneException;

  /**
   * Deletes an empty bucket.
   *
   * @param volume Volume name
   * @param bucket Bucket Name
   * @param req - Http request
   * @param info - Uri Info
   * @param headers - Http headers
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @DELETE
  @ApiOperation("Deletes an empty bucket.")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "x-ozone-version", example = "v1", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "x-ozone-user", example = "user", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "Date", example = "Date: Mon, 26 Jun 2017 "
          + "04:23:30 GMT", required = true, paramType = "header"),
      @ApiImplicitParam(name = "Authorization", example = "OZONE", required =
          true, paramType = "header")})
  Response deleteBucket(@PathParam("volume") String volume,
                        @PathParam("bucket") String bucket,
                        @Context Request req, @Context UriInfo info,
                        @Context HttpHeaders headers) throws OzoneException;

  /**
   * List Buckets lists the contents of a bucket.
   *
   * @param volume - Storage Volume Name
   * @param bucket - Bucket Name
   * @param info - Information type needed
   * @param prefix - Prefix for the keys to be fetched
   * @param maxKeys - MaxNumber of Keys to Return
   * @param prevKey - Continuation Token
   * @param req - Http request
   * @param headers - Http headers
   *
   * @return - Json Body
   *
   * @throws OzoneException
   */

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("List contents of a bucket")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "x-ozone-version", example = "v1", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "x-ozone-user", example = "user", required =
          true, paramType = "header"),
      @ApiImplicitParam(name = "Date", example = "Date: Mon, 26 Jun 2017 "
          + "04:23:30 GMT", required = true, paramType = "header"),
      @ApiImplicitParam(name = "Authorization", example = "OZONE", required =
          true, paramType = "header")})
  @SuppressWarnings("parameternumber")
  Response listBucket(@PathParam("volume") String volume,
                      @PathParam("bucket") String bucket,
                      @DefaultValue(Header.OZONE_INFO_QUERY_KEY)
                      @QueryParam(Header.OZONE_INFO_QUERY_TAG)
                      String info,
                      @QueryParam(Header.OZONE_LIST_QUERY_PREFIX)
                      String prefix,
                      @DefaultValue(Header.OZONE_DEFAULT_LIST_SIZE)
                      @QueryParam(Header.OZONE_LIST_QUERY_MAXKEYS)
                      int maxKeys,
                      @QueryParam(Header.OZONE_LIST_QUERY_PREVKEY)
                      String prevKey,
                      @Context Request req, @Context UriInfo uriInfo,
                      @Context HttpHeaders headers) throws OzoneException;


}
