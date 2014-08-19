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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A SignerSecretProvider that simply creates a secret based on a given String.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class StringSignerSecretProvider extends SignerSecretProvider {

  private byte[] secret;
  private byte[][] secrets;

  public StringSignerSecretProvider(String secretStr) {
    secret = secretStr.getBytes();
    secrets = new byte[][]{secret};
  }

  @Override
  public void init(Properties config, long tokenValidity) throws Exception {
    // do nothing
  }

  @Override
  public byte[] getCurrentSecret() {
    return secret;
  }

  @Override
  public byte[][] getAllSecrets() {
    return secrets;
  }
}
