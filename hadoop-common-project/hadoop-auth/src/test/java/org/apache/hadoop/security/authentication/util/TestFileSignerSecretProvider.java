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

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Properties;

public class TestFileSignerSecretProvider {

  @Test
  public void testGetSecrets() throws Exception {
    File testDir = new File(System.getProperty("test.build.data",
        "target/test-dir"));
    testDir.mkdirs();
    String secretValue = "hadoop";
    File secretFile = new File(testDir, "http-secret.txt");
    Writer writer = new FileWriter(secretFile);
    writer.write(secretValue);
    writer.close();

    FileSignerSecretProvider secretProvider
            = new FileSignerSecretProvider();
    Properties secretProviderProps = new Properties();
    secretProviderProps.setProperty(
            AuthenticationFilter.SIGNATURE_SECRET_FILE,
        secretFile.getAbsolutePath());
    secretProvider.init(secretProviderProps, null, -1);
    Assert.assertArrayEquals(secretValue.getBytes(),
        secretProvider.getCurrentSecret());
    byte[][] allSecrets = secretProvider.getAllSecrets();
    Assert.assertEquals(1, allSecrets.length);
    Assert.assertArrayEquals(secretValue.getBytes(), allSecrets[0]);
  }
}
