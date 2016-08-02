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

package org.apache.slider.server.servicemonitor;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpProbe extends Probe {
  protected static final Logger log = LoggerFactory.getLogger(HttpProbe.class);

  private final URL url;
  private final int timeout;
  private final int min, max;


  public HttpProbe(URL url, int timeout, int min, int max, Configuration conf) throws IOException {
    super("Http probe of " + url + " [" + min + "-" + max + "]", conf);
    this.url = url;
    this.timeout = timeout;
    this.min = min;
    this.max = max;
  }

  public static HttpURLConnection getConnection(URL url, int timeout) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setInstanceFollowRedirects(true);
    connection.setConnectTimeout(timeout);
    return connection;
  }
  
  @Override
  public ProbeStatus ping(boolean livePing) {
    ProbeStatus status = new ProbeStatus();
    HttpURLConnection connection = null;
    try {
      if (log.isDebugEnabled()) {
        // LOG.debug("Fetching " + url + " with timeout " + timeout);
      }
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
    } catch (IOException e) {
      String error = "Probe " + url + " failed: " + e;
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
