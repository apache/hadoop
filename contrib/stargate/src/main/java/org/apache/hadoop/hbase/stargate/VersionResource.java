/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.stargate.model.VersionModel;

/**
 * Implements Stargate software version reporting via
 * <p>
 * <tt>/version/stargate</tt>
 * <p>
 * <tt>/version</tt> (alias for <tt>/version/stargate</tt>)
 */
public class VersionResource implements Constants {
  private static final Log LOG = LogFactory.getLog(VersionResource.class);

  private CacheControl cacheControl;
  private RESTServlet servlet;

  public VersionResource() throws IOException {
    servlet = RESTServlet.getInstance();
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  /**
   * Build a response for a version request.
   * @param context servlet context
   * @param uriInfo (JAX-RS context variable) request URL
   * @return a response for a version request 
   */
  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response get(@Context ServletContext context, 
      @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    ResponseBuilder response = Response.ok(new VersionModel(context));
    response.cacheControl(cacheControl);
    return response.build();
  }

  /**
   * Dispatch to StorageClusterVersionResource
   */
  @Path("cluster")
  public StorageClusterVersionResource getClusterVersionResource() 
      throws IOException {
    return new StorageClusterVersionResource();
  }

  /**
   * Dispatch <tt>/version/stargate</tt> to self.
   */
  @Path("stargate")
  public VersionResource getVersionResource() {
    return this;
  }
}
