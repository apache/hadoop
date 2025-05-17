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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class TestAuthenticationToken {

  @Test
  public void testAnonymous() {
    assertNotNull(AuthenticationToken.ANONYMOUS);
    assertEquals(null, AuthenticationToken.ANONYMOUS.getUserName());
    assertEquals(null, AuthenticationToken.ANONYMOUS.getName());
    assertEquals(null, AuthenticationToken.ANONYMOUS.getType());
    assertEquals(-1, AuthenticationToken.ANONYMOUS.getExpires());
    assertFalse(AuthenticationToken.ANONYMOUS.isExpired());
  }
}
