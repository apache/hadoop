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

package org.apache.hadoop.yarn.service.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource.Builder;

/**
 * Http connection utilities.
 *
 */
public class HttpUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(HttpUtil.class);
  private static final Base64 BASE_64_CODEC = new Base64(0);

  protected HttpUtil() {
    // prevents calls from subclass
    throw new UnsupportedOperationException();
  }

  /**
   * Generate SPNEGO challenge request token.
   *
   * @param server - hostname to contact
   * @throws IOException
   * @throws InterruptedException
   */
  public static String generateToken(String server) throws
      IOException, InterruptedException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    LOG.debug("The user credential is {}", currentUser);
    String challenge = currentUser
        .doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            try {
              GSSManager manager = GSSManager.getInstance();
              // GSS name for server
              GSSName serverName = manager.createName("HTTP@" + server,
                  GSSName.NT_HOSTBASED_SERVICE);
              // Create a GSSContext for authentication with the service.
              // We're passing client credentials as null since we want them to
              // be read from the Subject.
              // We're passing Oid as null to use the default.
              GSSContext gssContext = manager.createContext(
                  serverName.canonicalize(null), null, null,
                  GSSContext.DEFAULT_LIFETIME);
              gssContext.requestMutualAuth(true);
              gssContext.requestCredDeleg(true);
              // Establish context
              byte[] inToken = new byte[0];
              byte[] outToken = gssContext.initSecContext(inToken, 0,
                  inToken.length);
              gssContext.dispose();
              // Base64 encoded and stringified token for server
              LOG.debug("Got valid challenge for host {}", serverName);
              return new String(BASE_64_CODEC.encode(outToken),
                  StandardCharsets.US_ASCII);
            } catch (GSSException e) {
              LOG.error("Error: ", e);
              throw new AuthenticationException(e);
            }
          }
        });
    return challenge;
  }

  public static Builder connect(String url) throws URISyntaxException,
      IOException, InterruptedException {
    boolean useKerberos = UserGroupInformation.isSecurityEnabled();
    URI resource = new URI(url);
    Client client = Client.create();
    Builder builder = client
        .resource(url).type(MediaType.APPLICATION_JSON);
    if (useKerberos) {
      String challenge = generateToken(resource.getHost());
      builder.header(HttpHeaders.AUTHORIZATION, "Negotiate " +
          challenge);
      LOG.debug("Authorization: Negotiate {}", challenge);
    }
    return builder;
  }
}
