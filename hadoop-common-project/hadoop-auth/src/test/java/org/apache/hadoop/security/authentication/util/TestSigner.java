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
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.junit.Assert;
import org.junit.Test;

public class TestSigner {

  @Test
  public void testNullAndEmptyString() throws Exception {
    Signer signer = new Signer(createStringSignerSecretProvider());
    try {
      signer.sign(null);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      signer.sign("");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
  }

  @Test
  public void testSignature() throws Exception {
    Signer signer = new Signer(createStringSignerSecretProvider());
    String s1 = signer.sign("ok");
    String s2 = signer.sign("ok");
    String s3 = signer.sign("wrong");
    Assert.assertEquals(s1, s2);
    Assert.assertNotEquals(s1, s3);
  }

  @Test
  public void testVerify() throws Exception {
    Signer signer = new Signer(createStringSignerSecretProvider());
    String t = "test";
    String s = signer.sign(t);
    String e = signer.verifyAndExtract(s);
    Assert.assertEquals(t, e);
  }

  @Test
  public void testInvalidSignedText() throws Exception {
    Signer signer = new Signer(createStringSignerSecretProvider());
    try {
      signer.verifyAndExtract("test");
      Assert.fail();
    } catch (SignerException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
  }

  @Test
  public void testTampering() throws Exception {
    Signer signer = new Signer(createStringSignerSecretProvider());
    String t = "test";
    String s = signer.sign(t);
    s += "x";
    try {
      signer.verifyAndExtract(s);
      Assert.fail();
    } catch (SignerException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
  }

  private StringSignerSecretProvider createStringSignerSecretProvider() throws Exception {
      StringSignerSecretProvider secretProvider = new StringSignerSecretProvider();
      Properties secretProviderProps = new Properties();
      secretProviderProps.setProperty(AuthenticationFilter.SIGNATURE_SECRET, "secret");
      secretProvider.init(secretProviderProps, null, -1);
      return secretProvider;
  }

  @Test
  public void testMultipleSecrets() throws Exception {
    TestSignerSecretProvider secretProvider = new TestSignerSecretProvider();
    Signer signer = new Signer(secretProvider);
    secretProvider.setCurrentSecret("secretB");
    String t1 = "test";
    String s1 = signer.sign(t1);
    String e1 = signer.verifyAndExtract(s1);
    Assert.assertEquals(t1, e1);
    secretProvider.setPreviousSecret("secretA");
    String t2 = "test";
    String s2 = signer.sign(t2);
    String e2 = signer.verifyAndExtract(s2);
    Assert.assertEquals(t2, e2);
    Assert.assertEquals(s1, s2); //check is using current secret for signing
    secretProvider.setCurrentSecret("secretC");
    secretProvider.setPreviousSecret("secretB");
    String t3 = "test";
    String s3 = signer.sign(t3);
    String e3 = signer.verifyAndExtract(s3);
    Assert.assertEquals(t3, e3);
    Assert.assertNotEquals(s1, s3); //check not using current secret for signing
    String e1b = signer.verifyAndExtract(s1);
    Assert.assertEquals(t1, e1b); // previous secret still valid
    secretProvider.setCurrentSecret("secretD");
    secretProvider.setPreviousSecret("secretC");
    try {
      signer.verifyAndExtract(s1);  // previous secret no longer valid
      Assert.fail();
    } catch (SignerException ex) {
      // Expected
    }
  }

  class TestSignerSecretProvider extends SignerSecretProvider {

    private byte[] currentSecret;
    private byte[] previousSecret;

    @Override
    public void init(Properties config, ServletContext servletContext,
            long tokenValidity) {
    }

    @Override
    public byte[] getCurrentSecret() {
      return currentSecret;
    }

    @Override
    public byte[][] getAllSecrets() {
      return new byte[][]{currentSecret, previousSecret};
    }

    public void setCurrentSecret(String secretStr) {
      currentSecret = secretStr.getBytes();
    }

    public void setPreviousSecret(String previousSecretStr) {
      previousSecret = previousSecretStr.getBytes();
    }
  }
}
