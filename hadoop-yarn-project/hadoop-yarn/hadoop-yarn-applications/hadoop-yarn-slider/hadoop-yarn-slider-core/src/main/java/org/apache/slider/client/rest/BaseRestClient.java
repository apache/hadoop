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

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.apache.slider.core.restclient.HttpVerb;
import org.apache.slider.core.restclient.UgiJerseyBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;


/**
 * This is a base class for Jersey REST clients in Slider.
 * It supports the execution of operations —with
 * exceptions uprated to IOExceptions when needed.
 * <p>
 * Subclasses can use these operations to provide an API-like view
 * of the REST model
 */
public class BaseRestClient  {
  private static final Logger log =
      LoggerFactory.getLogger(BaseRestClient.class);
  private final Client client;

  public BaseRestClient(
      Client client) {
    Preconditions.checkNotNull(client, "null jersey client");
    this.client = client;
  }

  /**
   * Get the jersey client
   * @return jersey client
   */
  public Client getClient() {
    return client;
  }

  /**
   * Execute the operation. Failures are raised as IOException subclasses
   * @param method method to execute
   * @param resource resource to work against
   * @param c class to build
   * @param <T> type expected
   * @return an instance of the type T
   * @throws IOException on any failure
   */
  public <T> T exec(HttpVerb method, WebResource resource, Class<T> c)
      throws IOException {
    try {
      Preconditions.checkArgument(c != null);
      log.debug("{}} {}", method, resource.getURI());
      return resource.accept(MediaType.APPLICATION_JSON_TYPE)
              .method(method.getVerb(), c);
    } catch (ClientHandlerException ex) {
      throw ExceptionConverter.convertJerseyException(method.getVerb(),
          resource.getURI().toString(),
          ex);
    } catch (UniformInterfaceException ex) {
      throw UgiJerseyBinding.uprateFaults(method,
          resource.getURI().toString(),
          ex);
    }
  }

  /**
   * Execute the operation. Failures are raised as IOException subclasses
   * @param method method to execute
   * @param resource resource to work against
   * @param t type to work with
   * @param <T> type expected
   * @return an instance of the type T
   * @throws IOException on any failure
   */
  public <T> T exec(HttpVerb method, WebResource resource, GenericType<T> t)
      throws IOException {
    try {
      Preconditions.checkArgument(t != null);
      log.debug("{}} {}", method, resource.getURI());
      resource.accept(MediaType.APPLICATION_JSON_TYPE);
      return resource.method(method.getVerb(), t);
    } catch (ClientHandlerException ex) {
      throw ExceptionConverter.convertJerseyException(method.getVerb(),
          resource.getURI().toString(),
          ex);
    } catch (UniformInterfaceException ex) {
      throw UgiJerseyBinding.uprateFaults(method, resource.getURI().toString(),
          ex);
    }
  }


  /**
   * Execute the  GET operation. Failures are raised as IOException subclasses
   * @param resource resource to work against
   * @param c class to build
   * @param <T> type expected
   * @return an instance of the type T
   * @throws IOException on any failure
   */
  public <T> T get(WebResource resource, Class<T> c) throws IOException {
    return exec(HttpVerb.GET, resource, c);
  }

  /**
   * Create a Web resource from the client.
   *
   * @param u the URI of the resource.
   * @return the Web resource.
   */
  public WebResource resource(URI u) {
    return client.resource(u);
  }

  /**
   * Create a Web resource from the client.
   *
   * @param u the URI of the resource.
   * @return the Web resource.
   */

  public WebResource resource(String url) {
    return client.resource(url);
  }

}
