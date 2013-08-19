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

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Assert;
import org.junit.Test;

public class TestAuthenticationToken {

  @Test
  public void testAnonymous() {
    Assert.assertNotNull(AuthenticationToken.ANONYMOUS);
    Assert.assertEquals(null, AuthenticationToken.ANONYMOUS.getUserName());
    Assert.assertEquals(null, AuthenticationToken.ANONYMOUS.getName());
    Assert.assertEquals(null, AuthenticationToken.ANONYMOUS.getType());
    Assert.assertEquals(-1, AuthenticationToken.ANONYMOUS.getExpires());
    Assert.assertFalse(AuthenticationToken.ANONYMOUS.isExpired());
  }

  @Test
  public void testConstructor() throws Exception {
    try {
      new AuthenticationToken(null, "p", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthenticationToken("", "p", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthenticationToken("u", null, "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthenticationToken("u", "", "t");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthenticationToken("u", "p", null);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    try {
      new AuthenticationToken("u", "p", "");
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected
    } catch (Throwable ex) {
      Assert.fail();
    }
    new AuthenticationToken("u", "p", "t");
  }

  @Test
  public void testGetters() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthenticationToken token = new AuthenticationToken("u", "p", "t");
    token.setExpires(expires);
    Assert.assertEquals("u", token.getUserName());
    Assert.assertEquals("p", token.getName());
    Assert.assertEquals("t", token.getType());
    Assert.assertEquals(expires, token.getExpires());
    Assert.assertFalse(token.isExpired());
    Thread.sleep(51);
    Assert.assertTrue(token.isExpired());
  }

  @Test
  public void testToStringAndParse() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthenticationToken token = new AuthenticationToken("u", "p", "t");
    token.setExpires(expires);
    String str = token.toString();
    token = AuthenticationToken.parse(str);
    Assert.assertEquals("p", token.getName());
    Assert.assertEquals("t", token.getType());
    Assert.assertEquals(expires, token.getExpires());
    Assert.assertFalse(token.isExpired());
    Thread.sleep(51);
    Assert.assertTrue(token.isExpired());
  }

  @Test
  public void testParseInvalid() throws Exception {
    long expires = System.currentTimeMillis() + 50;
    AuthenticationToken token = new AuthenticationToken("u", "p", "t");
    token.setExpires(expires);
    String str = token.toString();
    str = str.substring(0, str.indexOf("e="));
    try {
      AuthenticationToken.parse(str);
      Assert.fail();
    } catch (AuthenticationException ex) {
      // Expected
    } catch (Exception ex) {
      Assert.fail();
    }
  }
}
