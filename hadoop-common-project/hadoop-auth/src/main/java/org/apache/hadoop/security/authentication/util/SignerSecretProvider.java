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
package org.apache.hadoop.security.authentication.util;

import java.util.Properties;
import javax.servlet.ServletContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The SignerSecretProvider is an abstract way to provide a secret to be used
 * by the Signer so that we can have different implementations that potentially
 * do more complicated things in the backend.
 * See the RolloverSignerSecretProvider class for an implementation that
 * supports rolling over the secret at a regular interval.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class SignerSecretProvider {

  /**
   * Initialize the SignerSecretProvider
   * @param config configuration properties
   * @param servletContext servlet context
   * @param tokenValidity The amount of time a token is valid for
   * @throws Exception thrown if an error occurred
   */
  public abstract void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception;
  /**
   * Will be called on shutdown; subclasses should perform any cleanup here.
   */
  public void destroy() {}

  /**
   * Returns the current secret to be used by the Signer for signing new
   * cookies.  This should never return null.
   * <p>
   * Callers should be careful not to modify the returned value.
   * @return the current secret
   */
  public abstract byte[] getCurrentSecret();

  /**
   * Returns all secrets that a cookie could have been signed with and are still
   * valid; this should include the secret returned by getCurrentSecret().
   * <p>
   * Callers should be careful not to modify the returned value.
   * @return the secrets
   */
  public abstract byte[][] getAllSecrets();
}
