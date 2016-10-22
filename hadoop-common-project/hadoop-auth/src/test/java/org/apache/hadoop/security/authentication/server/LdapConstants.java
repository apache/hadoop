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
 * This class defines the constants used by the LDAP integration tests.
 */
public final class LdapConstants {

  /**
   * This class defines constants to be used for LDAP integration testing.
   * Hence this class is not expected to be instantiated.
   */
  private LdapConstants() {
  }

  public static final String LDAP_BASE_DN = "dc=example,dc=com";
  public static final String LDAP_SERVER_ADDR = "localhost";

}
