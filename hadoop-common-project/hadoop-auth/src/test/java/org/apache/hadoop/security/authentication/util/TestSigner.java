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

import org.junit.Assert;
import org.junit.Test;

public class TestSigner {

  @Test
  public void testNoSecret() throws Exception {
    try {
      new Signer(null);
      Assert.fail();
    }
    catch (IllegalArgumentException ex) {
    }
  }

  @Test
  public void testNullAndEmptyString() throws Exception {
    Signer signer = new Signer("secret".getBytes());
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
    Signer signer = new Signer("secret".getBytes());
    String s1 = signer.sign("ok");
    String s2 = signer.sign("ok");
    String s3 = signer.sign("wrong");
    Assert.assertEquals(s1, s2);
    Assert.assertNotSame(s1, s3);
  }

  @Test
  public void testVerify() throws Exception {
    Signer signer = new Signer("secret".getBytes());
    String t = "test";
    String s = signer.sign(t);
    String e = signer.verifyAndExtract(s);
    Assert.assertEquals(t, e);
  }

  @Test
  public void testInvalidSignedText() throws Exception {
    Signer signer = new Signer("secret".getBytes());
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
    Signer signer = new Signer("secret".getBytes());
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
}
