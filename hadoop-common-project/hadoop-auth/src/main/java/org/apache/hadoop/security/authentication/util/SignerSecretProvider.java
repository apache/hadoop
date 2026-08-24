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
 * <p>
 * Implementations should override
 * {@link #initialize(Properties, SecretProviderContext, long)}, which names no
 * servlet type. The older {@link #init(Properties, ServletContext, long)} is
 * still the entry point callers use and is still honoured when an
 * implementation overrides it, so providers written against it keep working
 * unchanged; it is deprecated and will be removed with the move to the jakarta
 * servlet namespace.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class SignerSecretProvider {

  /**
   * Initialize the SignerSecretProvider.
   * <p>
   * The default implementation adapts the ServletContext to a
   * {@link SecretProviderContext} and calls
   * {@link #initialize(Properties, SecretProviderContext, long)}, so a provider
   * that overrides only that method is initialized correctly through this
   * entry point. A provider that overrides this method instead is called
   * directly, as before.
   *
   * @param config configuration properties
   * @param servletContext servlet context
   * @param tokenValidity The amount of time a token is valid for
   * @throws Exception thrown if an error occurred
   * @deprecated override
   *             {@link #initialize(Properties, SecretProviderContext, long)},
   *             which does not name a servlet type and so is unaffected by
   *             which servlet namespace the container provides.
   */
  @Deprecated
  public void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception {
    initialize(config, ServletSecretProviderContext.of(servletContext),
        tokenValidity);
  }

  /**
   * Initialize the SignerSecretProvider against an attribute store.
   * <p>
   * The default implementation throws, because a provider has to implement one
   * of the two initialization methods. It is never reached by a provider that
   * overrides the deprecated
   * {@link #init(Properties, ServletContext, long)}, since callers go through
   * that method and its override does not delegate here.
   *
   * @param config configuration properties
   * @param context the attribute store to initialize against
   * @param tokenValidity The amount of time a token is valid for
   * @throws Exception thrown if an error occurred
   */
  public void initialize(Properties config, SecretProviderContext context,
          long tokenValidity) throws Exception {
    throw new UnsupportedOperationException(getClass().getName()
        + " implements neither initialize(Properties, SecretProviderContext,"
        + " long) nor the deprecated init(Properties, ServletContext, long)");
  }

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
