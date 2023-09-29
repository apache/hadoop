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
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.ServletContext;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * A SignerSecretProvider that simply loads a secret from a specified file.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FileSignerSecretProvider extends SignerSecretProvider {

  private byte[] secret;
  private byte[][] secrets;

  public FileSignerSecretProvider() {}

  @Override
  public void init(Properties config, ServletContext servletContext,
                   long tokenValidity) throws Exception {

    String signatureSecretFile = config.getProperty(
        AuthenticationFilter.SIGNATURE_SECRET_FILE, null);

    if (signatureSecretFile != null) {
      try (Reader reader = new InputStreamReader(Files.newInputStream(
              Paths.get(signatureSecretFile)), StandardCharsets.UTF_8)) {
        StringBuilder sb = new StringBuilder();
        int c = reader.read();
        while (c > -1) {
          sb.append((char) c);
          c = reader.read();
        }

        secret = sb.toString().getBytes(StandardCharsets.UTF_8);
        if (secret.length == 0) {
          throw new RuntimeException("No secret in signature secret file: "
             + signatureSecretFile);
        }
      } catch (IOException ex) {
        throw new RuntimeException("Could not read signature secret file: " +
            signatureSecretFile);
      }
    }

    secrets = new byte[][]{secret};
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
