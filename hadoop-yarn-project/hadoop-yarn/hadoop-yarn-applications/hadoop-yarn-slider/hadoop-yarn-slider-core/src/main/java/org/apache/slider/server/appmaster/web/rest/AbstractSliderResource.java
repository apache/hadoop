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

package org.apache.slider.server.appmaster.web.rest;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.registry.client.exceptions.NoPathPermissionsException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URL;

/**
 * Abstract resource base class for REST resources
 * that use the slider WebAppApi
 */
public abstract class AbstractSliderResource {
  private static final Logger log =
      LoggerFactory.getLogger(AbstractSliderResource.class);
  protected final WebAppApi slider;
  protected final MetricsAndMonitoring metricsAndMonitoring;

  protected AbstractSliderResource(WebAppApi slider) {
    this.slider = slider;
    metricsAndMonitoring = slider.getMetricsAndMonitoring();
  }

  /**
   * Generate a redirect to the WASL
   * @param request to base the URL on
   * @return a 302 response
   */
  protected Response redirectToAppWadl(HttpServletRequest request) {
    try {
      URI location = new URL(request.getScheme(),
          request.getServerName(),
          request.getServerPort(),
          RestPaths.APPLICATION_WADL).toURI();
      return Response.temporaryRedirect(location).build();
    } catch (Exception e) {
      log.error("Error during redirect to WADL", e);
      throw new WebApplicationException(Response.serverError().build());
    }
  }

  /**
   * Convert any exception caught into a web application
   * exception for rethrowing
   * @param path path of request
   * @param ex exception
   * @return an exception to throw
   */
  public WebApplicationException buildException(String path,
      Exception ex) {
    try {
      throw ex;
    } catch (WebApplicationException e) {
      // rethrow direct
      throw e;
    } catch (FileNotFoundException e) {
      return new NotFoundException("Not found: " + path);
    } catch (PathNotFoundException e) {
      return new NotFoundException("Not found: " + path);
    } catch (AuthenticationFailedException e) {
      return new ForbiddenException(path);
    } catch (NoPathPermissionsException e) {
      return new ForbiddenException(path);
    } catch (Exception e) {
      log.error("Error during generation of response: {}", e, e);
      return new WebApplicationException(e);
    }
  }

  /**
   * Mark an GET operation on a path
   * @param verb HTTP Verb
   * @param path path relative to slider API
   */
  protected void mark(String verb, String path) {
    metricsAndMonitoring.markMeterAndCounter(verb + "-" + path);
  }

  /**
   * Mark an GET operation on a path
   * @param verb HTTP Verb
   * @param path path relative to slider API
   */
  protected void mark(String verb, String path, String subpath) {
    metricsAndMonitoring.markMeterAndCounter(verb + "-" + path + subpath);
  }

  /**
   * Mark a GET operation on a path
   * @param path path relative to slider API
   */
  protected void markGet(String path) {
    mark("GET", path);
  }

  /**
   * Mark a GET operation on a path
   * @param path path relative to slider API
   */
  protected void markGet(String path, String subpath) {
    mark("GET", path, subpath);
  }

  /**
   * Mark a GET operation on a path
   * @param path path relative to slider API
   */
  protected void markPost(String path, String subpath) {
    mark("POST", path, subpath);
  }

  /**
   * Mark a GET operation on a path
   * @param path path relative to slider API
   */
  protected void markPut(String path, String subpath) {
    mark("PUT", path, subpath);
  }

  /**
   * Mark a GET operation on a path
   * @param path path relative to slider API
   */
  protected void markDelete(String path, String subpath) {
    mark("DELETE", path, subpath);
  }
  
}
