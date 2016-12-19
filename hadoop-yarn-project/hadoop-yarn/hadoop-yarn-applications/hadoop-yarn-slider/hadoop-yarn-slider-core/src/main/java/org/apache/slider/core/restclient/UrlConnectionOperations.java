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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Operations on the JDK UrlConnection class.
 *
 */
public class UrlConnectionOperations extends Configured  {
  private static final Logger log =
      LoggerFactory.getLogger(UrlConnectionOperations.class);

  private SliderURLConnectionFactory connectionFactory;

  private boolean useSpnego = false;

  /**
   * Create an instance off the configuration. The SPNEGO policy
   * is derived from the current UGI settings.
   * @param conf config
   */
  public UrlConnectionOperations(Configuration conf) {
    super(conf);
    connectionFactory = SliderURLConnectionFactory.newInstance(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      log.debug("SPNEGO is enabled");
      setUseSpnego(true);
    }
  }


  public boolean isUseSpnego() {
    return useSpnego;
  }

  public void setUseSpnego(boolean useSpnego) {
    this.useSpnego = useSpnego;
  }

  /**
   * Opens a url with cache disabled, redirect handled in 
   * (JDK) implementation.
   *
   * @param url to open
   * @return URLConnection
   * @throws IOException
   * @throws AuthenticationException authentication failure
   */
  public HttpURLConnection openConnection(URL url) throws
      IOException,
      AuthenticationException {
    Preconditions.checkArgument(url.getPort() != 0, "no port");
    return (HttpURLConnection) connectionFactory.openConnection(url, useSpnego);
  }
}
