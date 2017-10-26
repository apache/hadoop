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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.client.rest.headers.Header;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Volume Interface acts as the HTTP entry point for
 * volume related functionality.
 */
@InterfaceAudience.Private
@Path("/{volume}")
public interface Volume {

  /**
   * Creates a Volume owned by the user.
   *
   * Params :
   * Quota - Specifies the Maximum usable size by the user
   * the valid parameters for quota are <int>(<BYTES| MB|GB|TB>) | remove.
   * For example 10GB or "remove".
   *
   * @param volume Volume Name, this has to be unique at Ozone Level
   * @param quota Quota for this Storage Volume - <int>(<MB|GB|TB>) | remove
   * @param req - Request Object - Request Object
   * @param uriInfo - Http UriInfo
   * @param headers Http Headers HttpHeaders
   *
   * @return Response
   *
   * @throws OzoneException
   */

  @POST
  Response createVolume(@PathParam("volume") String volume,
      @DefaultValue(Header.OZONE_QUOTA_UNDEFINED)
      @QueryParam(Header.OZONE_QUOTA_QUERY_TAG) String quota,
      @Context Request req, @Context UriInfo uriInfo,
      @Context HttpHeaders headers) throws OzoneException;

  /**
   * Updates a Volume owned by the user.
   *
   * Params :
   * Owner - Specifies the name of the owner
   * Quota - Specifies the Maximum usable size by the user
   * the valid parameters for quota are <int>(<MB|GB|TB>) | remove.
   * For example 10GB or "remove".
   *
   * @param volume Volume Name, this has to be unique at Ozone Level
   * @param quota Quota for this Storage Volume - <int>(<MB|GB|TB>) | remove
   * @param req - Request Object - Request Object
   * @param headers Http Headers HttpHeaders
   *
   * @return Response
   *
   * @throws OzoneException
   */
  @PUT
  Response updateVolume(@PathParam("volume") String volume,
      @DefaultValue(Header.OZONE_QUOTA_UNDEFINED)
      @QueryParam(Header.OZONE_QUOTA_QUERY_TAG) String quota,
      @Context Request req, @Context UriInfo uriInfo,
      @Context HttpHeaders headers) throws OzoneException;

  /**
   * Deletes a Volume if it is empty.
   *
   * @param volume Storage Volume Name
   *
   * @return Response Response
   *
   * @throws OzoneException
   */
  @DELETE
  Response deleteVolume(@PathParam("volume") String volume,
      @Context Request req, @Context UriInfo uriInfo,
      @Context HttpHeaders headers) throws OzoneException;

  /**
   * Returns Volume info. This API can be invoked either
   * by admin or the owner
   *
   * @param volume - Storage Volume Name
   * @param req - Http Req
   * @param headers - Http headers
   *
   * @return - Response
   *
   * @throws OzoneException
   */
  @GET
  Response getVolumeInfo(@PathParam("volume") String volume,
      @DefaultValue(Header.OZONE_LIST_QUERY_BUCKET)
      @QueryParam(Header.OZONE_LIST_QUERY_TAG) String info,
      @QueryParam(Header.OZONE_LIST_QUERY_PREFIX) String prefix,
      @DefaultValue(Header.OZONE_DEFAULT_LIST_SIZE)
      @QueryParam(Header.OZONE_LIST_QUERY_MAXKEYS) int keys,
      @QueryParam(Header.OZONE_LIST_QUERY_PREVKEY) String prevKey,
      @QueryParam(Header.OZONE_LIST_QUERY_ROOTSCAN) boolean rootScan,
      @Context Request req, @Context UriInfo uriInfo,
      @Context HttpHeaders headers) throws OzoneException;

}
