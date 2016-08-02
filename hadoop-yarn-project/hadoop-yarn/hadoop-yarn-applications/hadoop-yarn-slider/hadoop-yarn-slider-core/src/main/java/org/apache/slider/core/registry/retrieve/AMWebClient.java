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

package org.apache.slider.core.registry.retrieve;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.slider.client.rest.BaseRestClient;
import org.apache.slider.core.restclient.HttpVerb;
import org.apache.slider.core.restclient.UgiJerseyBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

/**
 * Class to retrieve artifacts from the AM's web site. This sets up
 * the redirection and security logic properly
 */
public class AMWebClient {


  private final BaseRestClient restClient;
  private static final Logger
      log = LoggerFactory.getLogger(AMWebClient.class);


  public AMWebClient(Configuration conf) {
    UgiJerseyBinding binding = new UgiJerseyBinding(conf);

    restClient = new BaseRestClient(binding.createJerseyClient());

  }


  private static URLConnectionClientHandler getUrlConnectionClientHandler() {
    return new URLConnectionClientHandler(new HttpURLConnectionFactory() {
      @Override
      public HttpURLConnection getHttpURLConnection(URL url)
          throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (connection.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP) {
          // is a redirect - are we changing schemes?
          String redirectLocation = connection.getHeaderField(HttpHeaders.LOCATION);
          String originalScheme = url.getProtocol();
          String redirectScheme = URI.create(redirectLocation).getScheme();
          if (!originalScheme.equals(redirectScheme)) {
            // need to fake it out by doing redirect ourselves
            log.info("Protocol change during redirect. Redirecting {} to URL {}",
                     url, redirectLocation);
            URL redirectURL = new URL(redirectLocation);
            connection = (HttpURLConnection) redirectURL.openConnection();
          }
        }
        if (connection instanceof HttpsURLConnection) {
          log.debug("Attempting to configure HTTPS connection using client "
                    + "configuration");
          final SSLFactory factory;
          final SSLSocketFactory sf;
          final HostnameVerifier hv;

          try {
            HttpsURLConnection c = (HttpsURLConnection) connection;
            factory = new SSLFactory(SSLFactory.Mode.CLIENT, new Configuration());
            factory.init();
            sf = factory.createSSLSocketFactory();
            hv = factory.getHostnameVerifier();
            c.setSSLSocketFactory(sf);
            c.setHostnameVerifier(hv);
          } catch (Exception e) {
            log.info("Unable to configure HTTPS connection from "
                     + "configuration.  Using JDK properties.");
          }

        }
        return connection;
      }
    });
  }

  public WebResource resource(String url) {
    return restClient.resource(url);
  }

  public BaseRestClient getRestClient() {
    return restClient;
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
  public <T> T exec(HttpVerb method, WebResource resource, Class<T> c) throws IOException {
    return restClient.exec(method, resource, c);
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
    return restClient.exec(method, resource, t);
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
    return restClient.get(resource, c);
  }
}
