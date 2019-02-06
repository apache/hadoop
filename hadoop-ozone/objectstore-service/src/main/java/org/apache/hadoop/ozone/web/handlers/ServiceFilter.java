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

package org.apache.hadoop.ozone.web.handlers;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import org.apache.hadoop.ozone.client.rest.headers.Header;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;

/**
 * This class is used to intercept root URL requests and route it to
 * Volume List functionality.
 */
@Provider
public class ServiceFilter implements ContainerRequestFilter {
  /**
   * Filter the request.
   * <p>
   * An implementation may modify the state of the request or
   * create a new instance.
   *
   * @param request the request.
   *
   * @return the request.
   */
  @Override
  public ContainerRequest filter(ContainerRequest request) {
    if (request.getRequestUri().getPath().length() > 1) {
      return request;
    }

    // Just re-route it to volume handler with some hypothetical volume name.
    // volume name is ignored.

    request.setUris(request.getBaseUri(),
        UriBuilder.fromUri(request.getRequestUri())
        .path("/service")
        .queryParam("info", Header.OZONE_LIST_QUERY_SERVICE)
        .build());

    return request;
  }
}
