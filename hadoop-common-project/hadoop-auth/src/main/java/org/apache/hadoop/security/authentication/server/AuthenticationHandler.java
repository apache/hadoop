/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.server;

import org.apache.hadoop.security.authentication.client.AuthenticationException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Properties;

/**
 * Interface for server authentication mechanisms.
 * The {@link AuthenticationFilter} manages the lifecycle of the authentication handler.
 * Implementations must be thread-safe as one instance is initialized and used for all requests.
 */
public interface AuthenticationHandler {

  String WWW_AUTHENTICATE = HttpConstants.WWW_AUTHENTICATE_HEADER;

  /**
   * Returns the authentication type of the authentication handler.
   * This should be a name that uniquely identifies the authentication type.
   * For example 'simple' or 'kerberos'.
   *
   * @return the authentication type of the authentication handler.
   */
  public String getType();

  /**
   * Initializes the authentication handler instance.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter#init} method.
   *
   * @param config configuration properties to initialize the handler.
   *
   * @throws ServletException thrown if the handler could not be initialized.
   */
  public void init(Properties config) throws ServletException;

  /**
   * Destroys the authentication handler instance.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter#destroy} method.
   */
  public void destroy();

  /**
   * Performs an authentication management operation.
   * <p>
   * This is useful for handling operations like get/renew/cancel
   * delegation tokens which are being handled as operations of the
   * service end-point.
   * <p>
   * If the method returns <code>TRUE</code> the request will continue normal
   * processing, this means the method has not produced any HTTP response.
   * <p>
   * If the method returns <code>FALSE</code> the request will end, this means 
   * the method has produced the corresponding HTTP response.
   *
   * @param token the authentication token if any, otherwise <code>NULL</code>.
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   * @return <code>TRUE</code> if the request should be processed as a regular
   * request,
   * <code>FALSE</code> otherwise.
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if an Authentication error occurred.
   */
  public boolean managementOperation(AuthenticationToken token,
                                     HttpServletRequest request,
                                     HttpServletResponse response)
    throws IOException, AuthenticationException;

  /**
   * Performs an authentication step for the given HTTP client request.
   * <p>
   * This method is invoked by the {@link AuthenticationFilter} only if the HTTP client request is
   * not yet authenticated.
   * <p>
   * Depending upon the authentication mechanism being implemented, a particular HTTP client may
   * end up making a sequence of invocations before authentication is successfully established (this is
   * the case of Kerberos SPNEGO).
   * <p>
   * This method must return an {@link AuthenticationToken} only if the the HTTP client request has
   * been successfully and fully authenticated.
   * <p>
   * If the HTTP client request has not been completely authenticated, this method must take over
   * the corresponding HTTP response and it must return <code>null</code>.
   *
   * @param request the HTTP client request.
   * @param response the HTTP client response.
   *
   * @return an {@link AuthenticationToken} if the HTTP client request has been authenticated,
   *         <code>null</code> otherwise (in this case it must take care of the response).
   *
   * @throws IOException thrown if an IO error occurred.
   * @throws AuthenticationException thrown if an Authentication error occurred.
   */
  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
    throws IOException, AuthenticationException;

}
