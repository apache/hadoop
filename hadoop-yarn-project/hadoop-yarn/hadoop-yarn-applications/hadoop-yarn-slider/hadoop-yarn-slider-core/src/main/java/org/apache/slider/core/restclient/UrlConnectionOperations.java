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

  public HttpOperationResponse execGet(URL url) throws
      IOException,
      AuthenticationException {
    return execHttpOperation(HttpVerb.GET, url, null, "");
  }

  public HttpOperationResponse execHttpOperation(HttpVerb verb,
      URL url,
      byte[] payload,
      String contentType)
      throws IOException, AuthenticationException {
    HttpURLConnection conn = null;
    HttpOperationResponse outcome = new HttpOperationResponse();
    int resultCode;
    byte[] body = null;
    log.debug("{} {} spnego={}", verb, url, useSpnego);

    boolean doOutput = verb.hasUploadBody();
    if (doOutput) {
      Preconditions.checkArgument(payload !=null,
          "Null payload on a verb which expects one");
    }
    try {
      conn = openConnection(url);
      conn.setRequestMethod(verb.getVerb());
      conn.setDoOutput(doOutput);
      if (doOutput) {
        conn.setRequestProperty("Content-Type", contentType);
      }

      // now do the connection
      conn.connect();
      
      if (doOutput) {
        OutputStream output = conn.getOutputStream();
        IOUtils.write(payload, output);
        output.close();
      }
      
      resultCode = conn.getResponseCode();
      outcome.lastModified = conn.getLastModified();
      outcome.contentType = conn.getContentType();
      outcome.headers = conn.getHeaderFields();
      InputStream stream = conn.getErrorStream();
      if (stream == null) {
        stream = conn.getInputStream();
      }
      if (stream != null) {
        // read into a buffer.
        body = IOUtils.toByteArray(stream);
      } else {
        // no body: 
        log.debug("No body in response");

      }
    } catch (SSLException e) {
      throw e;
    } catch (IOException e) {
      throw NetUtils.wrapException(url.toString(),
          url.getPort(), "localhost", 0, e);

    } catch (AuthenticationException e) {
      throw new AuthenticationException("From " + url + ": " + e, e);

    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    uprateFaults(HttpVerb.GET, url.toString(), resultCode, "", body);
    outcome.responseCode = resultCode;
    outcome.data = body;
    return outcome;
  }

  /**
   * Uprate error codes 400 and up into faults; 
   * 404 is converted to a {@link NotFoundException},
   * 401 to {@link ForbiddenException}
   *
   * @param verb HTTP Verb used
   * @param url URL as string
   * @param resultCode response from the request
   * @param bodyAsString
   *@param body optional body of the request  @throws IOException if the result was considered a failure
   */
  public static void uprateFaults(HttpVerb verb, String url,
      int resultCode, String bodyAsString, byte[] body)
      throws IOException {

    if (resultCode < 400) {
      //success
      return;
    }
    String msg = verb.toString() +" "+ url;
    if (resultCode == 404) {
      throw new NotFoundException(msg);
    }
    if (resultCode == 401) {
      throw new ForbiddenException(msg);
    }
    // all other error codes
    
    // get a string respnse
    if (bodyAsString == null) {
      if (body != null && body.length > 0) {
        bodyAsString = new String(body);
      } else {
        bodyAsString = "";
      }
    }
    String message =  msg +
                     " failed with exit code " + resultCode
                     + ", body length " + bodyAsString.length()
                     + ":\n" + bodyAsString;
    log.error(message);
    throw new IOException(message);
  }

}
