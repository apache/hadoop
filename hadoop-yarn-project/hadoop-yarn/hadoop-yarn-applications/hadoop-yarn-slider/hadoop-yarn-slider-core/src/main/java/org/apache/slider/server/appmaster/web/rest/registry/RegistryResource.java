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

package org.apache.slider.server.appmaster.web.rest.registry;

import com.google.inject.Singleton;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * This is the read-only view of the YARN registry.
 * 
 * Model:
 * <ol>
 *   <li>a tree of nodes</li>
 *   <li>Default view is of children + record</li>
 * </ol>
 * 
 */
@Singleton
public class RegistryResource extends AbstractSliderResource {
  protected static final Logger log =
      LoggerFactory.getLogger(RegistryResource.class);
  public static final String SERVICE_PATH =
      "/{path:.*}";

  private final RegistryOperations registry;

  /**
   * Construct an instance bonded to a registry
   * @param slider slider API
   */
  public RegistryResource(WebAppApi slider) {
    super(slider);
    this.registry = slider.getRegistryOperations();
  }

  
  /**
   * Internal init code, per request
   * @param request incoming request 
   * @param uriInfo URI details
   */
  private void init(HttpServletRequest request, UriInfo uriInfo) {
    log.debug(uriInfo.getRequestUri().toString());
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public PathEntryResource getRoot(@Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
    return lookup("/", request, uriInfo);
  }

//   {path:.*}

  @Path(SERVICE_PATH)
  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public PathEntryResource lookup(
      @PathParam("path") String path,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
      init(request, uriInfo);
      return resolvePath(path);
  }

  /**
   * Do the actual processing of requests to responses; can be directly
   * invoked for testing.
   * @param path path to query
   * @return the entry
   * @throws WebApplicationException on any failure.
   */
  public PathEntryResource resolvePath(String path) throws
      WebApplicationException {
    try {
      PathEntryResource pathEntry =
          fromRegistry(path);
      if (log.isDebugEnabled()) {
        log.debug("Resolved:\n{}", pathEntry);
      }
      return pathEntry;
   
    } catch (Exception e) {
      throw buildException(path, e);
    }
  }


  /**
   * Build from the registry, filling up the children and service records.
   * If there is no service record at the end of the path, that entry is 
   * null
   * @param path path to query
   * @return the built up record
   * @throws IOException problems
   *
   */
  private PathEntryResource fromRegistry(String path) throws IOException {
    PathEntryResource entry = new PathEntryResource();
    try {
      entry.service = registry.resolve(path);
    } catch (NoRecordException e) {
      // ignoring
      log.debug("No record at {}", path);
    } catch (InvalidRecordException e) {
      // swallowing this exception, the sign of "no entry present"
      // "nothing parseable"
        log.warn("Failed to resolve {}: {}", path, e, e);
    }
    entry.nodes = registry.list(path);
    return entry;
  }
}
