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

/**
 * This class defines constants used for HTTP protocol entities (such as
 * headers, methods and their values).
 */
public final class HttpConstants {

  /**
   * This class defines the HTTP protocol constants. Hence it is not intended
   * to be instantiated.
   */
  private HttpConstants() {
  }

  /**
   * HTTP header used by the server endpoint during an authentication sequence.
   */
  public static final String WWW_AUTHENTICATE_HEADER = "WWW-Authenticate";

  /**
   * HTTP header used by the client endpoint during an authentication sequence.
   */
  public static final String AUTHORIZATION_HEADER = "Authorization";

  /**
   * HTTP header prefix used by the SPNEGO client/server endpoints during an
   * authentication sequence.
   */
  public static final String NEGOTIATE = "Negotiate";

  /**
   * HTTP header prefix used during the Basic authentication sequence.
   */
  public static final String BASIC = "Basic";

  /**
   * HTTP header prefix used during the Basic authentication sequence.
   */
  public static final String DIGEST = "Digest";

}
