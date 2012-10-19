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
package org.apache.hadoop.security.authentication.client;


import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Interface to configure  {@link HttpURLConnection} created by
 * {@link AuthenticatedURL} instances.
 */
public interface ConnectionConfigurator {

  /**
   * Configures the given {@link HttpURLConnection} instance.
   *
   * @param conn the {@link HttpURLConnection} instance to configure.
   * @return the configured {@link HttpURLConnection} instance.
   * 
   * @throws IOException if an IO error occurred.
   */
  public HttpURLConnection configure(HttpURLConnection conn) throws IOException;

}
