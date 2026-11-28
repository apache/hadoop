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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The attribute store a {@link SignerSecretProvider} is initialized against.
 * <p>
 * A SignerSecretProvider that has to share an object with the rest of the
 * application it is embedded in - as {@link ZKSignerSecretProvider} shares its
 * CuratorFramework client - reads and writes that object here.  When the
 * provider is running inside a servlet container the attributes are those of
 * the ServletContext, so the sharing is unchanged; see
 * {@link org.apache.hadoop.security.authentication.server.AuthenticationFilter}.
 * <p>
 * This interface exists so that a SignerSecretProvider need not name a servlet
 * type.  Implementations of it that are backed by a ServletContext do, but the
 * providers themselves stay independent of the servlet API and so of which
 * servlet namespace - javax or jakarta - the container provides.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public interface SecretProviderContext {

  /**
   * Returns the attribute stored under the given name, or null if there is
   * none.
   * @param name the attribute name
   * @return the attribute value, or null
   */
  Object getAttribute(String name);

  /**
   * Stores an attribute under the given name, replacing any previous value.
   * @param name the attribute name
   * @param value the attribute value
   */
  void setAttribute(String name, Object value);
}
