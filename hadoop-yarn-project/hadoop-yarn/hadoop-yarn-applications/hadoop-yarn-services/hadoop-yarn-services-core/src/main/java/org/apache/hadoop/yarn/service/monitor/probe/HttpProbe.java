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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpProbe extends Probe {
  protected static final Logger log = LoggerFactory.getLogger(HttpProbe.class);

  private static final String HOST_TOKEN = "${THIS_HOST}";

  private final String urlString;
  private final int timeout;
  private final int min, max;


  public HttpProbe(String url, int timeout, int min, int max, Configuration
      conf) {
    super("Http probe of " + url + " [" + min + "-" + max + "]", conf);
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
    return new HttpProbe(urlString, timeout, minSuccess, maxSuccess, null);
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
    ProbeStatus status = new ProbeStatus();
    ContainerStatus containerStatus = instance.getContainerStatus();
    if (containerStatus == null || ServiceUtils.isEmpty(containerStatus.getIPs())
        || StringUtils.isEmpty(containerStatus.getHost())) {
      status.fail(this, new IOException("IP is not available yet"));
      return status;
    }

    String ip = containerStatus.getIPs().get(0);
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
