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
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.Assert;
import org.junit.Test;

public class TestStringSignerSecretProvider {

  @Test
  public void testGetSecrets() throws Exception {
    String secretStr = "secret";
    StringSignerSecretProvider secretProvider
            = new StringSignerSecretProvider();
    Properties secretProviderProps = new Properties();
    secretProviderProps.setProperty(
            AuthenticationFilter.SIGNATURE_SECRET, "secret");
    secretProvider.init(secretProviderProps, null, -1);
    byte[] secretBytes = secretStr.getBytes();
    Assert.assertArrayEquals(secretBytes, secretProvider.getCurrentSecret());
    byte[][] allSecrets = secretProvider.getAllSecrets();
    Assert.assertEquals(1, allSecrets.length);
    Assert.assertArrayEquals(secretBytes, allSecrets[0]);
  }
}
