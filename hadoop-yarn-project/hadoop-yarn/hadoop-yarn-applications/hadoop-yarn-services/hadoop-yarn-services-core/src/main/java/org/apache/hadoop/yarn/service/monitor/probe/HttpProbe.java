/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.monitor.probe;

import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * A probe that checks whether a successful HTTP response code can be obtained
 * from a container. A well-formed URL must be provided. The URL is intended
 * to contain a token ${THIS_HOST} that will be replaced by the IP of the
 * container. This probe also performs the checks of the {@link DefaultProbe}.
 * Additional configurable properties include:
 *
 *   url - required URL for HTTP connection, e.g. http://${THIS_HOST}:8080
 *   timeout - connection timeout (default 1000)
 *   min.success - minimum response code considered successful (default 200)
 *   max.success - maximum response code considered successful (default 299)
 *
 */
public class HttpProbe extends DefaultProbe {
  protected static final Logger log = LoggerFactory.getLogger(HttpProbe.class);

  private static final String HOST_TOKEN = "${THIS_HOST}";

  private final String urlString;
  private final int timeout;
  private final int min, max;


  public HttpProbe(String url, int timeout, int min, int max,
      Map<String, String> props) {
    super("Http probe of " + url + " [" + min + "-" + max + "]", props);
    this.urlString = url;
    this.timeout = timeout;
    this.min = min;
    this.max = max;
  }

  public static HttpProbe create(Map<String, String> props)
      throws IOException {
    String urlString = getProperty(props, WEB_PROBE_URL, null);
    new URL(urlString);
    int timeout = getPropertyInt(props, WEB_PROBE_CONNECT_TIMEOUT,
        WEB_PROBE_CONNECT_TIMEOUT_DEFAULT);
    int minSuccess = getPropertyInt(props, WEB_PROBE_MIN_SUCCESS,
        WEB_PROBE_MIN_SUCCESS_DEFAULT);
    int maxSuccess = getPropertyInt(props, WEB_PROBE_MAX_SUCCESS,
        WEB_PROBE_MAX_SUCCESS_DEFAULT);
    return new HttpProbe(urlString, timeout, minSuccess, maxSuccess, props);
  }


  private static HttpURLConnection getConnection(URL url, int timeout) throws
      IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setInstanceFollowRedirects(true);
    connection.setConnectTimeout(timeout);
    return connection;
  }

  @Override
  public ProbeStatus ping(ComponentInstance instance) {
    ProbeStatus status = super.ping(instance);
    if (!status.isSuccess()) {
      return status;
    }
    String ip = instance.getContainerStatus().getIPs().get(0);
    HttpURLConnection connection = null;
    try {
      URL url = new URL(urlString.replace(HOST_TOKEN, ip));
      connection = getConnection(url, this.timeout);
      int rc = connection.getResponseCode();
      if (rc < min || rc > max) {
        String error = "Probe " + url + " error code: " + rc;
        log.info(error);
        status.fail(this,
            new IOException(error));
      } else {
        status.succeed(this);
      }
    } catch (Throwable e) {
      String error = "Probe " + urlString + " failed for IP " + ip + ": " + e;
      log.info(error, e);
      status.fail(this,
          new IOException(error, e));
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
    return status;
  }
}
