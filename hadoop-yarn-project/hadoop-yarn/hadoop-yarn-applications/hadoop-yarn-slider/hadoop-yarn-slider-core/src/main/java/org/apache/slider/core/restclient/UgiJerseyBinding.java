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

package org.apache.slider.core.restclient;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Class to bond to a Jersey client, for UGI integration and SPNEGO.
 * <p>
 *   Usage: create an instance, then when creating a Jersey <code>Client</code>
 *   pass in to the constructor the handler provided by {@link #getHandler()}
 *
 * see <a href="https://jersey.java.net/apidocs/1.17/jersey/com/sun/jersey/client/urlconnection/HttpURLConnectionFactory.html">Jersey docs</a>
 */
public class UgiJerseyBinding implements
    HttpURLConnectionFactory {
  private static final Logger log =
      LoggerFactory.getLogger(UgiJerseyBinding.class);

  private final UrlConnectionOperations operations;
  private final URLConnectionClientHandler handler;

  /**
   * Construct an instance
   * @param operations operations instance
   */
  @SuppressWarnings("ThisEscapedInObjectConstruction")
  public UgiJerseyBinding(UrlConnectionOperations operations) {
    Preconditions.checkArgument(operations != null, "Null operations");
    this.operations = operations;
    handler = new URLConnectionClientHandler(this);
  }

  /**
   * Create an instance off the configuration. The SPNEGO policy
   * is derived from the current UGI settings.
   * @param conf config
   */
  public UgiJerseyBinding(Configuration conf) {
    this(new UrlConnectionOperations(conf));
  }

  /**
   * Get a URL connection. 
   * @param url URL to connect to
   * @return the connection
   * @throws IOException any problem. {@link AuthenticationException} 
   * errors are wrapped
   */
  @Override
  public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
    try {
      // open a connection handling status codes and so redirections
      // but as it opens a connection, it's less useful than you think.

      return operations.openConnection(url);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
  }

  public UrlConnectionOperations getOperations() {
    return operations;
  }

  public URLConnectionClientHandler getHandler() {
    return handler;
  }
  
  /**
   * Get the SPNEGO flag (as found in the operations instance
   * @return the spnego policy
   */
  public boolean isUseSpnego() {
    return operations.isUseSpnego();
  }


  /**
   * Uprate error codes 400 and up into faults; 
   * <p>
   * see {@link ExceptionConverter#convertJerseyException(String, String, UniformInterfaceException)}
   */
  public static IOException uprateFaults(HttpVerb verb, String url,
      UniformInterfaceException ex)
      throws IOException {
    return ExceptionConverter.convertJerseyException(verb.getVerb(),
        url, ex);
  }

  /**
   * Create the standard Jersey client Config
   * @return the recommended Jersey Client config
   */
  public ClientConfig createJerseyClientConfig() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    return clientConfig;
  }

  /**
   * Create a jersey client bonded to this handler, using the
   * supplied client config
   * @param clientConfig client configuratin
   * @return a new client instance to use
   */
  public Client createJerseyClient(ClientConfig clientConfig) {
    return new Client(getHandler(), clientConfig);
  }

  /**
   * Create a jersey client bonded to this handler, using the
   * client config created with {@link #createJerseyClientConfig()}
   * @return a new client instance to use
   */
  public Client createJerseyClient() {
    return createJerseyClient(createJerseyClientConfig());
  }

}


