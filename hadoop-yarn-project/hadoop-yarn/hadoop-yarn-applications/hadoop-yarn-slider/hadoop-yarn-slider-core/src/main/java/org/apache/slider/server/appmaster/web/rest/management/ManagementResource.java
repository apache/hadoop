/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.management;

import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.appmaster.web.rest.management.resources.AggregateConfResource;
import org.apache.slider.server.appmaster.web.rest.management.resources.ConfTreeResource;
import org.apache.slider.server.appmaster.web.rest.management.resources.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URL;

/**
 *
 */
public class ManagementResource extends AbstractSliderResource {
  protected static final Logger log =
      LoggerFactory.getLogger(ManagementResource.class);
  public static final String CONFIG = "config";
  public static final String APP_UNDER_MANAGEMENT = "/app";

  public ManagementResource(WebAppApi slider) {
    super(slider);
  }

  private void init(HttpServletResponse res) {
    res.setContentType(null);
  }

  @GET
  public Response getWadl (@Context HttpServletRequest request) {
    return redirectToAppWadl(request);
  }

  @GET
  @Path("/app")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public AggregateConfResource getAggregateConfiguration(@Context UriInfo uriInfo,
                                                         @Context HttpServletResponse res) {
    init(res);
    return ResourceFactory.createAggregateConfResource(getAggregateConf(),
                                                       uriInfo.getAbsolutePathBuilder());
  }

  @GET
  @Path(APP_UNDER_MANAGEMENT+"/configurations/{config}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public ConfTreeResource getConfTreeResource(@PathParam(CONFIG) String config,
                                              @Context UriInfo uriInfo,
                                              @Context HttpServletResponse res) {
    init(res);
    AggregateConfResource aggregateConf =
        ResourceFactory.createAggregateConfResource(getAggregateConf(),
      uriInfo.getBaseUriBuilder()
      .path(RestPaths.SLIDER_CONTEXT_ROOT)
      .path(RestPaths.MANAGEMENT + APP_UNDER_MANAGEMENT));
    return aggregateConf.getConfTree(config);
  }

  protected AggregateConf getAggregateConf() {
    return slider.getAppState().getInstanceDefinitionSnapshot();
  }
}
