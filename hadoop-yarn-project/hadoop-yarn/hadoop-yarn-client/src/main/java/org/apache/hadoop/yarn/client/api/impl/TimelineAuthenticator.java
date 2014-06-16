/**
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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDelegationTokenResponse;
import org.apache.hadoop.yarn.security.client.TimelineAuthenticationConsts;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenOperation;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;

/**
 * A <code>KerberosAuthenticator</code> subclass that fallback to
 * {@link TimelineAuthenticationConsts}.
 */
@Private
@Unstable
public class TimelineAuthenticator extends KerberosAuthenticator {

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
  }

  /**
   * Returns the fallback authenticator if the server does not use Kerberos
   * SPNEGO HTTP authentication.
   * 
   * @return a {@link TimelineAuthenticationConsts} instance.
   */
  @Override
  protected Authenticator getFallBackAuthenticator() {
    return new TimelineAuthenticator();
  }

  public static void injectDelegationToken(Map<String, String> params,
      Token<?> dtToken)
      throws IOException {
    if (dtToken != null) {
      params.put(TimelineAuthenticationConsts.DELEGATION_PARAM,
          dtToken.encodeToUrlString());
    }
  }

  @Private
  @VisibleForTesting
  boolean hasDelegationToken(URL url) {
    if (url.getQuery() == null) {
      return false;
    } else {
      return url.getQuery().contains(
          TimelineAuthenticationConsts.DELEGATION_PARAM + "=");
    }
  }

  @Override
  public void authenticate(URL url, AuthenticatedURL.Token token)
      throws IOException, AuthenticationException {
    if (!hasDelegationToken(url)) {
      super.authenticate(url, token);
    }
  }

  public static Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      URL url, AuthenticatedURL.Token token, String renewer) throws IOException {
    TimelineDelegationTokenOperation op =
        TimelineDelegationTokenOperation.GETDELEGATIONTOKEN;
    Map<String, String> params = new HashMap<String, String>();
    params.put(TimelineAuthenticationConsts.OP_PARAM, op.toString());
    params.put(TimelineAuthenticationConsts.RENEWER_PARAM, renewer);
    url = appendParams(url, params);
    AuthenticatedURL aUrl =
        new AuthenticatedURL(new TimelineAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(op.getHttpMethod());
      TimelineDelegationTokenResponse dtRes = validateAndParseResponse(conn);
      if (!dtRes.getType().equals(
          TimelineAuthenticationConsts.DELEGATION_TOKEN_URL)) {
        throw new IOException("The response content is not expected: "
            + dtRes.getContent());
      }
      String tokenStr = dtRes.getContent().toString();
      Token<TimelineDelegationTokenIdentifier> dToken =
          new Token<TimelineDelegationTokenIdentifier>();
      dToken.decodeFromUrlString(tokenStr);
      return dToken;
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

  public static long renewDelegationToken(URL url,
      AuthenticatedURL.Token token,
      Token<TimelineDelegationTokenIdentifier> dToken) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(TimelineAuthenticationConsts.OP_PARAM,
        TimelineDelegationTokenOperation.RENEWDELEGATIONTOKEN.toString());
    params.put(TimelineAuthenticationConsts.TOKEN_PARAM,
        dToken.encodeToUrlString());
    url = appendParams(url, params);
    AuthenticatedURL aUrl =
        new AuthenticatedURL(new TimelineAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(
          TimelineDelegationTokenOperation.RENEWDELEGATIONTOKEN.getHttpMethod());
      TimelineDelegationTokenResponse dtRes = validateAndParseResponse(conn);
      if (!dtRes.getType().equals(
          TimelineAuthenticationConsts.DELEGATION_TOKEN_EXPIRATION_TIME)) {
        throw new IOException("The response content is not expected: "
            + dtRes.getContent());
      }
      return Long.valueOf(dtRes.getContent().toString());
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

  public static void cancelDelegationToken(URL url,
      AuthenticatedURL.Token token,
      Token<TimelineDelegationTokenIdentifier> dToken) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(TimelineAuthenticationConsts.OP_PARAM,
        TimelineDelegationTokenOperation.CANCELDELEGATIONTOKEN.toString());
    params.put(TimelineAuthenticationConsts.TOKEN_PARAM,
        dToken.encodeToUrlString());
    url = appendParams(url, params);
    AuthenticatedURL aUrl =
        new AuthenticatedURL(new TimelineAuthenticator());
    try {
      HttpURLConnection conn = aUrl.openConnection(url, token);
      conn.setRequestMethod(TimelineDelegationTokenOperation.CANCELDELEGATIONTOKEN
          .getHttpMethod());
      validateAndParseResponse(conn);
    } catch (AuthenticationException ex) {
      throw new IOException(ex.toString(), ex);
    }
  }

  /**
   * Convenience method that appends parameters an HTTP <code>URL</code>.
   * 
   * @param url
   *          the url.
   * @param params
   *          the query string parameters.
   * 
   * @return a <code>URL</code>
   * 
   * @throws IOException
   *           thrown if an IO error occurs.
   */
  public static URL appendParams(URL url, Map<String, String> params)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(url);
    String separator = url.toString().contains("?") ? "&" : "?";
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(separator).append(entry.getKey()).append("=").
          append(URLEncoder.encode(entry.getValue(), "UTF8"));
      separator = "&";
    }
    return new URL(sb.toString());
  }

  /**
   * Validates the response of an <code>HttpURLConnection</code>. If the current
   * status code is not 200, it will throw an exception with a detail message
   * using Server side error messages if available. Otherwise,
   * {@link TimelineDelegationTokenResponse} will be parsed and returned.
   * 
   * @param conn
   *          the <code>HttpURLConnection</code>.
   * @return
   * @throws IOException
   *           thrown if the current status code is not 200 or the JSON response
   *           cannot be parsed correctly
   */
  private static TimelineDelegationTokenResponse validateAndParseResponse(
      HttpURLConnection conn) throws IOException {
    int status = conn.getResponseCode();
    JsonNode json = mapper.readTree(conn.getInputStream());
    if (status == HttpURLConnection.HTTP_OK) {
      return mapper.readValue(json, TimelineDelegationTokenResponse.class);
    } else {
      // If the status code is not 200, some thing wrong should happen at the
      // server side, the JSON content is going to contain exception details.
      // We can use the JSON content to reconstruct the exception object.
      try {
        String message =
            json.get(TimelineAuthenticationConsts.ERROR_MESSAGE_JSON)
                .getTextValue();
        String exception =
            json.get(TimelineAuthenticationConsts.ERROR_EXCEPTION_JSON)
                .getTextValue();
        String className =
            json.get(TimelineAuthenticationConsts.ERROR_CLASSNAME_JSON)
                .getTextValue();

        try {
          ClassLoader cl = TimelineAuthenticator.class.getClassLoader();
          Class<?> klass = cl.loadClass(className);
          Constructor<?> constr = klass.getConstructor(String.class);
          throw (IOException) constr.newInstance(message);
        } catch (IOException ex) {
          throw ex;
        } catch (Exception ex) {
          throw new IOException(MessageFormat.format("{0} - {1}", exception,
              message));
        }
      } catch (IOException ex) {
        if (ex.getCause() instanceof IOException) {
          throw (IOException) ex.getCause();
        }
        throw new IOException(
            MessageFormat.format("HTTP status [{0}], {1}",
                status, conn.getResponseMessage()));
      }
    }
  }

}
