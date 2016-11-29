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
package org.apache.slider.server.appmaster.web.rest;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource;
import org.apache.slider.server.appmaster.web.rest.management.ManagementResource;
import org.apache.slider.server.appmaster.web.rest.publisher.PublisherResource;
import org.apache.slider.server.appmaster.web.rest.registry.RegistryResource;

import javax.ws.rs.Path;

/**
 *  The available REST services exposed by a slider AM. 
 */
@Singleton
@Path(RestPaths.SLIDER_CONTEXT_ROOT)
public class AMWebServices {
  
  /** AM/WebApp info object */
  private WebAppApi slider;
  private final ManagementResource managementResource;
  private final PublisherResource publisherResource;
  private final RegistryResource registryResource;
  private final ApplicationResource applicationResource;

  @Inject
  public AMWebServices(WebAppApi slider) {
    this.slider = slider;
    managementResource = new ManagementResource(slider);
    publisherResource = new PublisherResource(slider);
    registryResource = new RegistryResource(slider);
    applicationResource = new ApplicationResource(slider);
  }

  @Path(RestPaths.SLIDER_SUBPATH_MANAGEMENT)
  public ManagementResource getManagementResource() {
    return managementResource;
  }

  @Path(RestPaths.SLIDER_SUBPATH_PUBLISHER)
  public PublisherResource getPublisherResource() {
    return publisherResource;
  }
 
  @Path(RestPaths.SLIDER_SUBPATH_REGISTRY)
  public RegistryResource getRegistryResource() {
    return registryResource;
  }
  
  @Path(RestPaths.SLIDER_SUBPATH_APPLICATION)
  public ApplicationResource getApplicationResource() {
    return applicationResource;
  }
}
