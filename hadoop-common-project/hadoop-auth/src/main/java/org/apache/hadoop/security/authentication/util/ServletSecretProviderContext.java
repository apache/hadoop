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

import java.util.Objects;
import javax.servlet.ServletContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Presents a ServletContext to a {@link SignerSecretProvider} as a
 * {@link SecretProviderContext}.
 * <p>
 * Reads and writes go straight through to the ServletContext, so an object a
 * provider shares this way - the CuratorFramework client of
 * {@link ZKSignerSecretProvider} - is still a ServletContext attribute under
 * the same name, and is still found there by everything that looks for it,
 * including DelegationTokenAuthenticationFilter in hadoop-common.
 * <p>
 * This class is the one place in the provider hierarchy that names a servlet
 * type, and it goes away with the
 * {@link SignerSecretProvider#init(java.util.Properties, ServletContext, long)}
 * it exists to support.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
final class ServletSecretProviderContext implements SecretProviderContext {

  private final ServletContext servletContext;

  private ServletSecretProviderContext(ServletContext servletContext) {
    this.servletContext = servletContext;
  }

  /**
   * Returns a context backed by the given ServletContext.
   * <p>
   * The ServletContext may be null - most callers of init pass null - and a
   * provider that never touches the store is unaffected. One that does fails
   * on first access, as it did when it was handed the null ServletContext
   * directly.
   *
   * @param servletContext the servlet context, or null
   * @return an attribute store, never null
   */
  static SecretProviderContext of(ServletContext servletContext) {
    return new ServletSecretProviderContext(servletContext);
  }

  @Override
  public Object getAttribute(String name) {
    return servletContext(name).getAttribute(name);
  }

  @Override
  public void setAttribute(String name, Object value) {
    servletContext(name).setAttribute(name, value);
  }

  private ServletContext servletContext(String name) {
    return Objects.requireNonNull(servletContext,
        () -> "SignerSecretProvider was initialized without a ServletContext,"
            + " so there is nowhere to share the attribute " + name);
  }
}
