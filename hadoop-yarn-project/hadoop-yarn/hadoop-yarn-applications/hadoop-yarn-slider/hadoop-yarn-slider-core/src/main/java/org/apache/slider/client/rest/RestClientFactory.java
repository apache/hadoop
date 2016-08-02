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

package org.apache.slider.client.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.slider.client.ClientRegistryBinder;
import org.apache.slider.api.SliderApplicationApi;
import org.apache.slider.core.registry.info.CustomRegistryConstants;

import java.io.IOException;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_APPLICATION;

/**
 * Factory for the Rest client; hides the lookup and instantiation.
 * <p>
 * 
 */
public class RestClientFactory {

  private final ClientRegistryBinder binder;
  private final Client jerseyClient;
  private final String user, serviceclass, instance;

  public RestClientFactory(RegistryOperations operations,
      Client jerseyClient,
      String user,
      String serviceclass,
      String instance) {
    this.jerseyClient = jerseyClient;
    this.user = user;
    this.serviceclass = serviceclass;
    this.instance = instance;
    binder = new ClientRegistryBinder(operations);
  }

  /**
   * Locate the AM
   * @return a resource to the AM
   * @throws IOException any failure to resolve to the AM
   */
  private WebResource locateAppmaster() throws IOException {
    String restAPI = binder.lookupExternalRestAPI(user, serviceclass, instance,
        CustomRegistryConstants.AM_REST_BASE);
    return jerseyClient.resource(restAPI);
  }

  /**
   * Locate the slider AM then instantiate a client instance against
   * its Application API.
   * @return the instance
   * @throws IOException on any failure
   */
  public SliderApplicationApi createSliderAppApiClient() throws IOException {
    WebResource appmaster = locateAppmaster();
    return createSliderAppApiClient(appmaster);
  }

   /**
   * Create a Slider application API client instance against
   * its Application API.
   * @param appmaster The AM to work against.
   * @return the instance
   * @throws IOException on any failure
   */
  public SliderApplicationApi createSliderAppApiClient(WebResource appmaster) {
    WebResource appResource = appmaster.path(SLIDER_PATH_APPLICATION);
    return new SliderApplicationApiRestClient(jerseyClient, appResource);
  }

}
